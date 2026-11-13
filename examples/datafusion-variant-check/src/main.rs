//! Reads otlp2parquet output written with `[parquet.variant]` enabled and queries
//! the VARIANT columns through DataFusion + `datafusion-variant`.
//!
//! Usage: datafusion-variant-check <parquet-dir> <variant-column> <key> [sql]
//! e.g.   datafusion-variant-check ./data/traces span_attributes http.method
//!        datafusion-variant-check s3://otlp/variant/traces span_attributes http.route
//!
//! `<parquet-dir>` is a local path or an `s3://bucket/prefix` URL. For S3 (e.g. MinIO)
//! set AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION.
//!
//! The data is exposed as the view `t`. With a trailing `sql` argument the tool
//! just runs that query and prints the result.

use anyhow::{bail, Context, Result};
use datafusion::prelude::*;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::sync::Arc;
use url::Url;

async fn count(ctx: &SessionContext, sql: &str) -> Result<i64> {
    let batches = ctx
        .sql(sql)
        .await
        .with_context(|| format!("planning: {sql}"))?
        .collect()
        .await
        .with_context(|| format!("executing: {sql}"))?;
    let col = batches
        .first()
        .context("no result batch")?
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .context("expected an Int64 count")?
        .value(0);
    Ok(col)
}

/// One-line summary of which keys a VARIANT column shreds, e.g.
/// `shredded: http.route (Utf8), http.response.status_code (Int64)`.
fn describe_shredding(ty: &datafusion::arrow::datatypes::DataType) -> String {
    use datafusion::arrow::datatypes::DataType;
    let DataType::Struct(fields) = ty else {
        return format!("not a VARIANT struct ({ty})");
    };
    let Some(DataType::Struct(keys)) = fields
        .iter()
        .find(|f| f.name() == "typed_value")
        .map(|f| f.data_type())
    else {
        return "VARIANT, not shredded".to_string();
    };
    let parts: Vec<String> = keys
        .iter()
        .map(|k| match k.data_type() {
            DataType::Struct(inner) => match inner.iter().find(|f| f.name() == "typed_value") {
                Some(t) => format!("{} ({})", k.name(), t.data_type()),
                None => k.name().to_string(),
            },
            other => format!("{} ({other})", k.name()),
        })
        .collect();
    format!("VARIANT, shredded: {}", parts.join(", "))
}

fn collect_parquet(path: &std::path::Path, out: &mut Vec<String>) -> Result<()> {
    if path.is_file() {
        out.push(path.to_string_lossy().into_owned());
        return Ok(());
    }
    for entry in std::fs::read_dir(path).with_context(|| format!("listing {}", path.display()))? {
        let p = entry?.path();
        if p.is_dir() {
            collect_parquet(&p, out)?;
        } else if p.extension().is_some_and(|e| e == "parquet") {
            out.push(p.to_string_lossy().into_owned());
        }
    }
    Ok(())
}

/// Find the `.parquet` files under `dir`: either `s3://bucket/prefix` (any S3-compatible
/// store such as MinIO; endpoint and credentials come from the AWS_* environment
/// variables) or a local path.
async fn find_files(ctx: &SessionContext, dir: &str) -> Result<Vec<String>> {
    if let Some(rest) = dir.strip_prefix("s3://") {
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            // The demo MinIO endpoint is plain http. (Path-style addressing is the default.)
            .with_allow_http(true)
            .build()
            .context("building the S3 client (check AWS_ENDPOINT_URL, AWS_ACCESS_KEY_ID, ...)")?;
        let store: Arc<dyn ObjectStore> = Arc::new(store);
        let base = format!("s3://{bucket}");
        ctx.register_object_store(&Url::parse(&base)?, store.clone());
        return list_objects(store.as_ref(), &base, prefix).await;
    }
    let mut files = Vec::new();
    collect_parquet(std::path::Path::new(dir), &mut files)?;
    files.sort();
    Ok(files)
}

/// List `.parquet` objects under `prefix`, returned as full `base/key` URLs.
async fn list_objects(store: &dyn ObjectStore, base: &str, prefix: &str) -> Result<Vec<String>> {
    let prefix = ObjectPath::from(prefix);
    let mut listing = store.list(Some(&prefix));
    let mut files = Vec::new();
    while let Some(meta) = listing.next().await {
        let meta = meta.with_context(|| format!("listing {base}/{prefix}"))?;
        if meta.location.as_ref().ends_with(".parquet") {
            files.push(format!("{base}/{}", meta.location));
        }
    }
    files.sort();
    Ok(files)
}

/// otlp2parquet output is nested Hive-style (signal/service/year=/month=/day=/hour=/file),
/// and DataFusion's directory listing finds no files at that depth. So create one external
/// table per file (the documented, Variant-aware reader, which understands shredded
/// layouts) and union them into the view `t`.
async fn create_view(ctx: &SessionContext, files: &[String]) -> Result<()> {
    for (i, file) in files.iter().enumerate() {
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE t_{i} STORED AS PARQUET LOCATION '{file}'"
        ))
        .await
        .with_context(|| format!("creating table over {file}"))?
        .collect()
        .await?;
    }
    let union = (0..files.len())
        .map(|i| format!("SELECT * FROM t_{i}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    ctx.sql(&format!("CREATE VIEW t AS {union}"))
        .await
        .context("creating union view")?
        .collect()
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (Some(dir), Some(column), Some(key)) = (args.next(), args.next(), args.next()) else {
        bail!("usage: datafusion-variant-check <parquet-dir> <variant-column> <key> [sql]");
    };
    let custom_sql = args.next();

    let mut ctx = SessionContext::new();
    datafusion_variant::register_all(&mut ctx)?;

    let files = find_files(&ctx, &dir).await?;
    if files.is_empty() {
        bail!("no .parquet files under {dir}");
    }
    println!("== {} parquet file(s) under {dir}", files.len());
    create_view(&ctx, &files).await?;

    if let Some(sql) = custom_sql {
        ctx.sql(&sql)
            .await
            .with_context(|| format!("planning: {sql}"))?
            .show()
            .await?;
        return Ok(());
    }

    let df = ctx.sql("SELECT * FROM t LIMIT 1").await?;
    let field = df.schema().inner().field_with_name(&column)?.clone();
    println!("== {column}: {}", describe_shredding(field.data_type()));

    let total = count(&ctx, "SELECT count(*) FROM t").await?;
    println!("== rows: {total}");

    // Bracket notation keeps dotted OTel keys (http.request.method) as a single key.
    let path = format!("$['{key}']").replace('\'', "''");
    let with_key = count(
        &ctx,
        &format!(
            "SELECT count(*) FROM t WHERE variant_get({column}, '{path}', 'VARCHAR') IS NOT NULL"
        ),
    )
    .await?;
    println!("== rows where variant_get({column}, \"{path}\", 'VARCHAR') is not null: {with_key}");
    if with_key == 0 {
        bail!("expected at least one row with key '{key}' in {column}");
    }
    // Rebuilding the whole object. datafusion-variant cannot yet do this for *shredded*
    // object columns (it asks for `unshred_variant`), so skip it quietly in that case.
    match ctx
        .sql(&format!("SELECT to_json({column}) AS json FROM t LIMIT 3"))
        .await?
        .show()
        .await
    {
        Ok(()) => {}
        Err(_) => println!(
            "== to_json({column}) skipped: this datafusion-variant version cannot rebuild a whole \
             shredded object; read keys with variant_get instead"
        ),
    }
    println!("OK");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;

    /// Runs the listing and table-building code that the `s3://` path uses, against a
    /// local object store registered under a made-up URL. Only the S3 client itself and
    /// the network are not covered. Needs VARIANT_CHECK_TEST_DIR=<dir holding otlp2parquet
    /// output, with a `traces/` folder>; it does nothing when that is unset.
    #[tokio::test]
    async fn lists_and_queries_through_an_object_store() -> Result<()> {
        let Ok(dir) = std::env::var("VARIANT_CHECK_TEST_DIR") else {
            return Ok(());
        };
        let mut ctx = SessionContext::new();
        datafusion_variant::register_all(&mut ctx)?;
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&dir)?);
        ctx.register_object_store(&Url::parse("testfs://bucket")?, store.clone());

        let files = list_objects(store.as_ref(), "testfs://bucket", "traces").await?;
        assert!(!files.is_empty(), "no parquet files listed under traces/");
        assert!(files
            .iter()
            .all(|f| f.starts_with("testfs://bucket/traces/")));
        create_view(&ctx, &files).await?;

        let rows = count(&ctx, "SELECT count(*) FROM t").await?;
        let routed = count(
            &ctx,
            "SELECT count(*) FROM t WHERE variant_get(span_attributes, '$[''http.route'']', 'VARCHAR') IS NOT NULL",
        )
        .await?;
        println!(
            "listed {} files, {rows} rows, {routed} with http.route",
            files.len()
        );
        assert!(rows > 0 && rows == routed);
        Ok(())
    }
}
