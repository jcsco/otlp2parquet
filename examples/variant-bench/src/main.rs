//! Benchmarks the same trace queries over three layouts of otlp2parquet output:
//!   plain     attributes as JSON strings (queried with datafusion-functions-json)
//!   variant   attributes as VARIANT, nothing shredded
//!   shredded  attributes as VARIANT, hot keys shredded into typed columns
//!
//! Usage: variant-bench compact <src-dir> <dst-dir>      (merge small files into one)
//!        variant-bench <plain-dir> <variant-dir> <shredded-dir> [runs]
//!
//! Each dir holds the `traces/` output of one otlp2parquet run (see scripts/run.sh).
//! Every query is run once to warm the OS page cache, then `runs` timed times (default 5).
//! Results of the three layouts are compared and must be identical.

use anyhow::{bail, Context, Result};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::prelude::*;
use std::path::Path;
use std::time::Instant;

#[derive(Clone, Copy, PartialEq)]
enum Layout {
    Plain,
    Variant,
    Shredded,
}

impl Layout {
    fn name(self) -> &'static str {
        match self {
            Layout::Plain => "plain",
            Layout::Variant => "variant",
            Layout::Shredded => "shredded",
        }
    }

    /// SQL expression reading `key` from `col` as text.
    fn text(self, col: &str, key: &str) -> String {
        match self {
            Layout::Plain => format!("json_get_str({col}, '{key}')"),
            _ => format!("variant_get({col}, '$[''{key}'']', 'VARCHAR')"),
        }
    }

    /// SQL expression reading `key` from `col` as a 64-bit integer.
    fn int(self, col: &str, key: &str) -> String {
        match self {
            Layout::Plain => format!("json_get_int({col}, '{key}')"),
            _ => format!("variant_get({col}, '$[''{key}'']', 'BIGINT')"),
        }
    }
}

struct Query {
    name: &'static str,
    /// What the query exercises.
    about: &'static str,
    sql: fn(Layout) -> String,
}

fn queries() -> Vec<Query> {
    const A: &str = "span_attributes";
    vec![
        Query {
            name: "q1 filter shredded keys",
            about: "5xx on one route",
            sql: |l| {
                format!(
                    "SELECT count(*) FROM t WHERE {} = '/api/orders/{{id}}' AND {} >= 500",
                    l.text(A, "http.route"),
                    l.int(A, "http.response.status_code")
                )
            },
        },
        Query {
            name: "q2 group by shredded key",
            about: "requests per route and method",
            sql: |l| {
                format!(
                    "SELECT {r} AS route, {m} AS method, count(*) AS n FROM t \
                     GROUP BY 1, 2 ORDER BY 3 DESC, 1, 2",
                    r = l.text(A, "http.route"),
                    m = l.text(A, "http.request.method")
                )
            },
        },
        Query {
            name: "q3 error rate by route",
            about: "group by + filtered count",
            sql: |l| {
                format!(
                    "SELECT {r} AS route, count(*) AS spans, \
                     count(*) FILTER (WHERE {s} >= 500) AS errors FROM t \
                     GROUP BY 1 ORDER BY errors DESC, 1",
                    r = l.text(A, "http.route"),
                    s = l.int(A, "http.response.status_code")
                )
            },
        },
        Query {
            name: "q4 filter UNshredded key",
            about: "one tenant (not shredded)",
            sql: |l| {
                format!(
                    "SELECT count(*) FROM t WHERE {} = 'tenant-007'",
                    l.text(A, "app.tenant")
                )
            },
        },
        Query {
            name: "q5 group by UNshredded key",
            about: "requests per region",
            sql: |l| {
                format!(
                    "SELECT {} AS region, count(*) AS n FROM t GROUP BY 1 ORDER BY 2 DESC, 1",
                    l.text(A, "app.region")
                )
            },
        },
        Query {
            name: "q6 no attribute access",
            about: "control: plain columns only",
            sql: |_| {
                "SELECT service_name, count(*) AS n, avg(duration_time_unix_nano) FROM t \
                 GROUP BY 1 ORDER BY 1"
                    .to_string()
            },
        },
    ]
}

fn collect_parquet(path: &Path, out: &mut Vec<String>) -> Result<()> {
    for entry in std::fs::read_dir(path).with_context(|| format!("listing {}", path.display()))? {
        let p = entry?.path();
        if p.is_dir() {
            collect_parquet(&p, out)?;
        } else if p.extension().is_some_and(|e| e == "parquet") {
            out.push(std::fs::canonicalize(&p)?.to_string_lossy().into_owned());
        }
    }
    Ok(())
}

/// Registers the files in `dir` as the view `name`. otlp2parquet output is nested Hive-style
/// and DataFusion's directory listing finds nothing at that depth, so there is one external
/// table per file, unioned.
async fn register(ctx: &SessionContext, name: &str, dir: &str) -> Result<(usize, u64)> {
    let mut files = Vec::new();
    collect_parquet(Path::new(dir), &mut files)?;
    files.sort();
    if files.is_empty() {
        bail!("no .parquet files under {dir}");
    }
    let mut bytes = 0;
    for (i, f) in files.iter().enumerate() {
        bytes += std::fs::metadata(f)?.len();
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE {name}_{i} STORED AS PARQUET LOCATION '{f}'"
        ))
        .await?
        .collect()
        .await?;
    }
    let union = (0..files.len())
        .map(|i| format!("SELECT * FROM {name}_{i}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    ctx.sql(&format!("CREATE VIEW {name} AS {union}"))
        .await?
        .collect()
        .await?;
    Ok((files.len(), bytes))
}

/// Runs `sql` against `t`, returning the rendered result and the timings in ms.
async fn time(ctx: &SessionContext, sql: &str, runs: usize) -> Result<(String, Vec<f64>)> {
    let run = || async {
        let batches = ctx
            .sql(sql)
            .await
            .with_context(|| format!("planning: {sql}"))?
            .collect()
            .await
            .with_context(|| format!("executing: {sql}"))?;
        Ok::<_, anyhow::Error>(batches)
    };
    let warm = run().await?;
    let rendered = pretty_format_batches(&warm)?.to_string();
    let mut times = Vec::with_capacity(runs);
    for _ in 0..runs {
        let t0 = Instant::now();
        run().await?;
        times.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    times.sort_by(|a, b| a.total_cmp(b));
    Ok((rendered, times))
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Merges every `.parquet` file under `src` into one file at `dst`.
///
/// otlp2parquet writes one small file per request and service, which would make the
/// benchmark measure file-open overhead. The same merge (zstd, 128k-row row groups) is
/// applied to all three layouts, so only the column encoding differs between them. The
/// arrow `parquet` crate round-trips the VARIANT annotation and the shredded layout.
fn compact(src: &str, dst: &str) -> Result<()> {
    use datafusion::arrow::compute::concat_batches;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let mut files = Vec::new();
    collect_parquet(Path::new(src), &mut files)?;
    files.sort();
    if files.is_empty() {
        bail!("no .parquet files under {src}");
    }
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_max_row_group_row_count(Some(131_072))
        .build();
    let mut writer: Option<ArrowWriter<std::fs::File>> = None;
    let mut pending = Vec::new();
    let mut pending_rows = 0;
    for f in &files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(f)?)?
            .with_batch_size(8192)
            .build()?;
        for batch in reader {
            let batch = batch?;
            pending_rows += batch.num_rows();
            pending.push(batch);
            if pending_rows >= 131_072 {
                let merged = concat_batches(&pending[0].schema(), &pending)?;
                if writer.is_none() {
                    std::fs::create_dir_all(dst)?;
                    let out = std::fs::File::create(Path::new(dst).join("compacted.parquet"))?;
                    writer = Some(ArrowWriter::try_new(
                        out,
                        merged.schema(),
                        Some(props.clone()),
                    )?);
                }
                writer.as_mut().context("writer")?.write(&merged)?;
                pending.clear();
                pending_rows = 0;
            }
        }
    }
    if !pending.is_empty() {
        let merged = concat_batches(&pending[0].schema(), &pending)?;
        if writer.is_none() {
            std::fs::create_dir_all(dst)?;
            let out = std::fs::File::create(Path::new(dst).join("compacted.parquet"))?;
            writer = Some(ArrowWriter::try_new(out, merged.schema(), Some(props))?);
        }
        writer.as_mut().context("writer")?.write(&merged)?;
    }
    writer.context("nothing written")?.close()?;
    println!("compacted {} files from {src} into {dst}", files.len());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("compact") {
        let (Some(src), Some(dst)) = (args.get(1), args.get(2)) else {
            bail!("usage: variant-bench compact <src-dir> <dst-dir>");
        };
        return compact(src, dst);
    }
    if args.len() < 3 {
        bail!("usage: variant-bench <plain-dir> <variant-dir> <shredded-dir> [runs]");
    }
    let runs: usize = args.get(3).map(|r| r.parse()).transpose()?.unwrap_or(5);
    let layouts = [
        (Layout::Plain, args[0].as_str()),
        (Layout::Variant, args[1].as_str()),
        (Layout::Shredded, args[2].as_str()),
    ];

    // One context per layout so each query sees exactly one table named `t`.
    let mut ctxs = Vec::new();
    println!("cores: {}", std::thread::available_parallelism()?.get());
    println!(
        "{:<10} {:>6} {:>10} {:>12}",
        "layout", "files", "rows", "size (MiB)"
    );
    for (layout, dir) in layouts {
        let mut ctx = SessionContext::new();
        datafusion_variant::register_all(&mut ctx)?;
        datafusion_functions_json::register_all(&mut ctx)?;
        let (files, bytes) = register(&ctx, "t", dir).await?;
        let rows = ctx
            .sql("SELECT count(*) FROM t")
            .await?
            .collect()
            .await?
            .first()
            .context("no count")?
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .context("count type")?
            .value(0);
        println!(
            "{:<10} {:>6} {:>10} {:>12.1}",
            layout.name(),
            files,
            rows,
            mib(bytes)
        );
        ctxs.push((layout, ctx));
    }

    println!("\nmedian ms over {runs} runs (min in parentheses); speedup is plain / shredded");
    println!(
        "{:<28} {:>16} {:>16} {:>16} {:>8}  {}",
        "query", "plain", "variant", "shredded", "speedup", "results"
    );
    let mut failed = false;
    for q in queries() {
        let mut cells = Vec::new();
        let mut rendered: Vec<String> = Vec::new();
        let mut medians = Vec::new();
        for (layout, ctx) in &ctxs {
            let (text, times) = time(ctx, &(q.sql)(*layout), runs).await?;
            let median = times[times.len() / 2];
            cells.push(format!("{:>8.1} ({:>5.1})", median, times[0]));
            medians.push(median);
            rendered.push(text);
        }
        let same = rendered.windows(2).all(|w| w[0] == w[1]);
        failed |= !same;
        println!(
            "{:<28} {:>16} {:>16} {:>16} {:>7.1}x  {}",
            q.name,
            cells[0],
            cells[1],
            cells[2],
            medians[0] / medians[2],
            if same { "identical" } else { "MISMATCH" }
        );
        println!("{:<28}   {}", "", q.about);
        if !same {
            for ((layout, _), text) in ctxs.iter().zip(&rendered) {
                eprintln!("--- {} / {}\n{text}", q.name, layout.name());
            }
        }
    }
    if failed {
        bail!("the layouts returned different results");
    }
    Ok(())
}
