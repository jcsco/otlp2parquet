# Variant Shredding Examples

[Parquet VARIANT output](reference.md#parquet-variant) stores attribute maps as one flexible column. *Shredding* additionally copies chosen keys into typed Parquet columns. Queries that filter or group on those keys read the typed column directly instead of decoding the whole attribute map, and keys you did not shred stay readable as before.

This page shows what to shred and how to query it. All examples need a build with the `variant` feature (`make build-cli-variant`). Queries use DataFusion with [`datafusion-variant`](querying.md#apache-datafusion-variant-columns); the config is the same for any engine that reads shredded VARIANT.

## Choosing keys

Shred keys that you:

- **filter on** (`WHERE http.route = '/checkout'`),
- **group or join on** (`GROUP BY service.namespace`), or
- **read on nearly every query**.

Leave everything else alone. A handful of keys per column (roughly 3 to 6) is a good start. Shredded keys cost an extra typed column each, and a key you never query adds nothing.

Pick the type the key really has: `utf8` for names and routes, `int64` for counts and status codes, `bool`, `float64`. If a particular row holds a different type (say `"n/a"` where an integer is expected), that value is not dropped or coerced; it is kept in the column's residual value and is still returned by queries.

## 1. HTTP server traces

Slice request spans by method, route and status without decoding every span's attributes.

```toml
[parquet.variant]
enabled = true

[parquet.variant.shred.traces]
span_attributes = { "http.request.method" = "utf8", "http.route" = "utf8", "http.response.status_code" = "int64" }
```

```sql
-- Error rate by route
SELECT variant_get(span_attributes, '$[''http.route'']', 'VARCHAR')                 AS route,
       count(*)                                                                     AS spans,
       count(*) FILTER (WHERE variant_get(span_attributes,
              '$[''http.response.status_code'']', 'BIGINT') >= 500)                 AS errors
FROM traces
GROUP BY 1
ORDER BY errors DESC;
```

Bracket notation (`$['http.route']`) keeps a dotted OTel key as one key. Shredding config uses the same rule: `"http.route"` is one key, not a nested `http` then `route`.

## 2. Resource attributes: who ran this?

`resource_attributes` is the same shape on every signal, so shredding the few deployment keys you slice by pays off everywhere. The setting is per signal, so repeat it:

```toml
[parquet.variant.shred.traces]
resource_attributes = { "service.namespace" = "utf8", "deployment.environment" = "utf8" }

[parquet.variant.shred.logs]
resource_attributes = { "service.namespace" = "utf8", "deployment.environment" = "utf8" }

[parquet.variant.shred.metrics]
resource_attributes = { "service.namespace" = "utf8", "deployment.environment" = "utf8" }
```

```sql
-- Production spans per service
SELECT service_name, count(*) AS spans
FROM traces
WHERE variant_get(resource_attributes, '$[''deployment.environment'']', 'VARCHAR') = 'production'
GROUP BY 1;
```

Keys you did not shred (for example `host.name`) still work in the same column with the same query shape. You only gain speed on the shredded ones.

## 3. Metrics by label, with a per-type override

The shared `shred.metrics` table applies to gauge, sum, histogram and exponential histogram. A per-type table replaces the whole shared table for that type, including other columns such as `resource_attributes`, so repeat any keys you still want shredded. Here histograms use route and status, while gauges use host and CPU:

```toml
[parquet.variant.shred.metrics]
metric_attributes = { "http.route" = "utf8", "http.status_code" = "int64" }

[parquet.variant.shred.metrics.gauge]
metric_attributes = { "host" = "utf8", "cpu" = "int64" }
```

```sql
-- histogram table: data points by route and status
SELECT variant_get(metric_attributes, '$[''http.route'']',       'VARCHAR') AS route,
       variant_get(metric_attributes, '$[''http.status_code'']', 'BIGINT')  AS status,
       count(*) AS points
FROM histogram
GROUP BY 1, 2;

-- gauge table: average usage per CPU
SELECT variant_get(metric_attributes, '$[''host'']', 'VARCHAR') AS host,
       variant_get(metric_attributes, '$[''cpu'']',  'BIGINT')  AS cpu,
       avg(double_value) AS avg_usage
FROM gauge
WHERE name = 'cpu.usage'
GROUP BY 1, 2;
```

## 4. Logs: structured bodies

Make the body VARIANT too (it is not in the default column list). JSON-object bodies keep their structure and can be queried by key; plain-text bodies become string variants.

```toml
[parquet.variant]
enabled = true
columns = ["resource_attributes", "log_attributes", "body"]

[parquet.variant.shred.logs]
resource_attributes = { "service.namespace" = "utf8" }
log_attributes      = { "http.request.method" = "utf8" }
```

`body`, `events_json`, `links_json` and `exemplars_json` can be VARIANT but cannot be shredded.

## 5. Configure from the environment

Containers often set config through variables. The shred table is passed as one JSON document with the same shape as the TOML:

```bash
export OTLP2PARQUET_PARQUET_VARIANT_ENABLED=true
export OTLP2PARQUET_PARQUET_VARIANT_COLUMNS="resource_attributes,span_attributes"
export OTLP2PARQUET_PARQUET_VARIANT_SHRED='{
  "traces": {
    "span_attributes": { "http.route": "utf8", "http.response.status_code": "int64" }
  }
}'
```

## Try it with Docker Compose

`docker-compose.yml` has a `variant` profile that runs the whole loop locally for **traces, logs and metrics**: an otlp2parquet built with the `variant` feature and shredding configured for each signal, a small sender that posts sample payloads every 5 seconds, and a DataFusion query tool. The server writes to the compose file's **MinIO** (S3) under the `variant/` prefix, so VARIANT files stay separate from the plain-Parquet output in the same bucket (`s3://otlp/variant/...`), and the query tool reads them back from MinIO over S3. It uses its own port (4319). Start it by naming the services, as below: starting the server also starts MinIO and creates the bucket, whereas `docker compose --profile variant up` would also start the default otlp2parquet and telemetrygen containers.

What is shredded:

| Signal | Column | Shredded keys |
|---|---|---|
| traces | `span_attributes` | `http.request.method`, `http.route`, `http.response.status_code` (int64) |
| logs | `log_attributes` | `http.route`, `http.response.status_code` (int64) |
| metrics (sum) | `metric_attributes` | `http.route`, `http.response.status_code` (int64) |
| metrics (gauge) | `metric_attributes` | `host`, `cpu` (int64) |
| metrics (histogram) | `metric_attributes` | `http.route`, `http.request.method` |
| all | `resource_attributes` | `service.namespace`, `deployment.environment` |

Log `body` is also VARIANT, and some sample records have structured bodies you can query by key. The sample payloads also carry unshredded keys (`app.latency_ms`, `user.tier`) so you can compare shredded and unshredded keys in the same column.

```bash
docker compose up -d --build otlp2parquet-variant variant-load
# wait ~10 seconds for files to be written, then run the default query
# (error rate by route, from traces):
docker compose run --rm datafusion
```

The tool takes `<dir> <variant-column> <key> [sql]` and exposes the files in `<dir>` as the table `t`. `<dir>` is a local path or an `s3://` location. Here: `s3://otlp/variant/traces`, `s3://otlp/variant/logs` and `s3://otlp/variant/metrics/{gauge,sum,histogram}`. With only the first three arguments it runs a short check instead, printing which keys the column shreds and how many rows have the key:

```bash
docker compose run --rm datafusion s3://otlp/variant/traces span_attributes http.route
# == span_attributes: VARIANT, shredded: http.request.method (Utf8), http.response.status_code (Int64), http.route (Utf8)
# == rows: 56
```

```bash
# Error rate by route, from logs
docker compose run --rm datafusion s3://otlp/variant/logs log_attributes http.route \
  "SELECT variant_get(log_attributes, '\$[''http.route'']', 'VARCHAR') AS route,
          count(*) AS logs,
          count(*) FILTER (WHERE variant_get(log_attributes,
                '\$[''http.response.status_code'']', 'BIGINT') >= 500) AS errors
   FROM t GROUP BY 1 ORDER BY errors DESC"

# Structured log bodies, queried by key
docker compose run --rm datafusion s3://otlp/variant/logs body event \
  "SELECT severity_text, variant_get(body, '\$[''event'']', 'VARCHAR') AS event
   FROM t WHERE variant_get(body, '\$[''event'']', 'VARCHAR') IS NOT NULL"

# Request counts by route and status, from the sum metric
docker compose run --rm datafusion s3://otlp/variant/metrics/sum metric_attributes http.route \
  "SELECT variant_get(metric_attributes, '\$[''http.route'']', 'VARCHAR') AS route,
          variant_get(metric_attributes, '\$[''http.response.status_code'']', 'BIGINT') AS status,
          sum(int_value) AS requests
   FROM t GROUP BY 1, 2 ORDER BY 3 DESC"
```

The sample payloads are in [`examples/variant-demo/`](https://github.com/smithclay/otlp2parquet/tree/main/examples/variant-demo) (`traces.json`, `logs.json`, `metrics.json`).

A per-type metrics table (`gauge`, `histogram`, …) **replaces** the shared `metrics` table for that type, including its `resource_attributes` entry. The compose file repeats the resource keys in the gauge and histogram tables for that reason.

## Check what was shredded

Print the typed columns a file contains, using pyarrow:

```python
import pyarrow.parquet as pq

t = pq.read_table("data/traces/my-service/year=2025/month=10/day=17/hour=21/....parquet")
shredded = t.column("span_attributes").combine_chunks().field("typed_value")
print({f.name: str(shredded.field(f.name).field("typed_value").type) for f in shredded.type})
# {'http.route': 'string', 'http.response.status_code': 'int64'}
```

Or run the bundled check against a directory of output: `make check-datafusion DIR=./data/traces COLUMN=span_attributes KEY=http.route`. The tool also reads `s3://bucket/prefix` locations, using the `AWS_ENDPOINT_URL`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_REGION` environment variables, which suits any S3-compatible store.

## Things to keep in mind

- **Changing the shred config changes the layout of new files only.** Older files keep their old layout. Query engines that reconcile different layouts (see the notes in [Query Data](querying.md#apache-datafusion-variant-columns)) handle this; others may need you to query old and new ranges separately.
- **Shredding is not a performance guarantee.** It lets engines read typed columns and use Parquet statistics on them. These examples were checked for correctness, not benchmarked, so measure on your own data and queries.
- **Engine support differs.** Check your engine reads shredded VARIANT, and which functions it supports on them, before relying on it.
