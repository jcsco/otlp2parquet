//! Reads otlp2parquet output written with `[parquet.variant]` enabled and queries
//! the VARIANT columns through DataFusion + `datafusion-variant`.
//!
//! Usage: datafusion-variant-check <parquet-dir> <variant-column> <key> [sql]
//! e.g.   datafusion-variant-check ./data/traces span_attributes http.method
//!
//! The data is exposed as the view `t`. With a trailing `sql` argument the tool
//! just runs that query and prints the result.

use anyhow::{bail, Context, Result};
use datafusion::prelude::*;

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

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let (Some(dir), Some(column), Some(key)) = (args.next(), args.next(), args.next()) else {
        bail!("usage: datafusion-variant-check <parquet-dir> <variant-column> <key> [sql]");
    };
    let custom_sql = args.next();

    // otlp2parquet output is nested Hive-style (signal/service/year=/month=/day=/hour=/file),
    // and DataFusion's directory listing finds no files at that depth. So create one
    // external table per file (the documented, Variant-aware reader, which understands
    // shredded layouts) and union them into the view `t`.
    let mut files = Vec::new();
    collect_parquet(std::path::Path::new(&dir), &mut files)?;
    if files.is_empty() {
        bail!("no .parquet files under {dir}");
    }
    files.sort();
    println!("== {} parquet file(s) under {dir}", files.len());

    let mut ctx = SessionContext::new();
    datafusion_variant::register_all(&mut ctx)?;
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

    if let Some(sql) = custom_sql {
        ctx.sql(&sql)
            .await
            .with_context(|| format!("planning: {sql}"))?
            .show()
            .await?;
        return Ok(());
    }

    println!("== schema of {column}");
    let df = ctx.sql("SELECT * FROM t LIMIT 1").await?;
    let field = df.schema().inner().field_with_name(&column)?.clone();
    println!("{field:#?}");

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
    // object columns (it asks for `unshred_variant`), so report instead of failing.
    println!("== to_json({column}) (first 3)");
    match ctx
        .sql(&format!("SELECT to_json({column}) AS json FROM t LIMIT 3"))
        .await?
        .show()
        .await
    {
        Ok(()) => {}
        Err(e) => println!(
            "NOTE: to_json({column}) failed (known datafusion-variant limitation for shredded objects): {e}"
        ),
    }
    println!("OK");
    Ok(())
}
