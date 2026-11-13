//! Parquet VARIANT output (cargo feature `variant`).
//!
//! The ingestion pipeline keeps semi-structured OTLP columns as JSON strings so
//! batching stays cheap. Just before a batch is encoded to Parquet, the columns
//! selected in `[parquet.variant]` are converted to the Parquet `VARIANT` logical
//! type, and configured attribute keys are shredded into typed columns.

use crate::config::{
    ColumnShred, ShredType, VariantConfig, LOGS_VARIANT_COLUMNS, METRICS_VARIANT_COLUMNS,
    METRIC_KINDS, TRACES_VARIANT_COLUMNS,
};
use crate::SignalType;
use arrow::array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, FieldRef, Schema};
use arrow::error::ArrowError;
use once_cell::sync::OnceCell;
use parquet_variant::{VariantPath, VariantPathElement};
use parquet_variant_compute::{
    json_to_variant, shred_variant, ShreddedSchemaBuilder, VariantArray,
};
use serde::de::IgnoredAny;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

static PLAN: OnceCell<VariantPlan> = OnceCell::new();

/// What to do with one column.
#[derive(Debug)]
struct ColumnPlan {
    name: String,
    /// Log body: plain strings are common, so non-JSON text becomes a string variant.
    is_body: bool,
    /// Shredding schema for the variant, if any keys are configured.
    shred: Option<DataType>,
}

/// Resolved per-signal conversion plan.
#[derive(Debug, Default)]
pub struct VariantPlan {
    logs: Vec<ColumnPlan>,
    traces: Vec<ColumnPlan>,
    metrics: Vec<(&'static str, Vec<ColumnPlan>)>,
}

/// Build the global plan from config. A no-op when variant output is disabled.
pub fn initialize(config: &VariantConfig) -> Result<(), ArrowError> {
    if !config.enabled {
        return Ok(());
    }
    let plan = VariantPlan::from_config(config)?;
    let _ = PLAN.set(plan);
    Ok(())
}

/// Convert the configured columns of `batch`. Returns `None` when variant output
/// is disabled or no configured column applies to this batch.
pub fn apply(
    batch: &RecordBatch,
    signal: SignalType,
    metric_type: Option<&str>,
) -> Result<Option<RecordBatch>, ArrowError> {
    match PLAN.get() {
        Some(plan) => plan.apply(batch, signal, metric_type),
        None => Ok(None),
    }
}

impl VariantPlan {
    pub fn from_config(config: &VariantConfig) -> Result<Self, ArrowError> {
        let build =
            |allowed: &[&str], shred: &ColumnShred| -> Result<Vec<ColumnPlan>, ArrowError> {
                config
                    .columns
                    .iter()
                    .filter(|c| allowed.contains(&c.as_str()))
                    .map(|name| {
                        Ok(ColumnPlan {
                            name: name.clone(),
                            is_body: name == "body",
                            shred: shred.get(name).map(shred_schema).transpose()?,
                        })
                    })
                    .collect()
            };

        let mut metrics = Vec::new();
        for kind in METRIC_KINDS {
            let table = config.shred.metrics.for_kind(kind);
            metrics.push((*kind, build(METRICS_VARIANT_COLUMNS, table)?));
        }
        Ok(Self {
            logs: build(LOGS_VARIANT_COLUMNS, &config.shred.logs)?,
            traces: build(TRACES_VARIANT_COLUMNS, &config.shred.traces)?,
            metrics,
        })
    }

    fn columns(&self, signal: SignalType, metric_type: Option<&str>) -> &[ColumnPlan] {
        match signal {
            SignalType::Logs => &self.logs,
            SignalType::Traces => &self.traces,
            SignalType::Metrics => metric_type
                .and_then(|m| self.metrics.iter().find(|(k, _)| *k == m))
                .map(|(_, cols)| cols.as_slice())
                .unwrap_or(&[]),
        }
    }

    pub fn apply(
        &self,
        batch: &RecordBatch,
        signal: SignalType,
        metric_type: Option<&str>,
    ) -> Result<Option<RecordBatch>, ArrowError> {
        let schema = batch.schema();
        let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        let mut changed = false;

        for plan in self.columns(signal, metric_type) {
            let Ok(idx) = schema.index_of(&plan.name) else {
                continue;
            };
            let strings = columns[idx]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "column '{}' must be Utf8 to convert to VARIANT, found {}",
                        plan.name,
                        columns[idx].data_type()
                    ))
                })?;

            let mut variant = if plan.is_body {
                body_to_variant(strings)?
            } else {
                json_column_to_variant(&plan.name, strings)?
            };
            if let Some(shred_type) = &plan.shred {
                variant = shred_variant(&variant, shred_type)?;
            }

            fields[idx] = Arc::new(variant.field(plan.name.clone()));
            columns[idx] = Arc::new(variant.into_inner());
            changed = true;
        }

        if !changed {
            return Ok(None);
        }
        let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));
        RecordBatch::try_new(schema, columns).map(Some)
    }
}

fn arrow_type(t: ShredType) -> DataType {
    match t {
        ShredType::Bool => DataType::Boolean,
        ShredType::Int32 => DataType::Int32,
        ShredType::Int64 => DataType::Int64,
        ShredType::Float64 => DataType::Float64,
        ShredType::Utf8 => DataType::Utf8,
    }
}

/// Build the shredding schema for one column. Each configured key is a single
/// top-level field name; dots in OTel keys (`http.request.method`) are not nesting.
fn shred_schema(keys: &BTreeMap<String, ShredType>) -> Result<DataType, ArrowError> {
    let mut builder = ShreddedSchemaBuilder::new();
    for (key, ty) in keys {
        let path = VariantPath::new(vec![VariantPathElement::field(Cow::Borrowed(key.as_str()))]);
        builder = builder.with_path(path, arrow_type(*ty))?;
    }
    Ok(builder.build())
}

fn is_valid_json(s: &str) -> bool {
    serde_json::from_str::<IgnoredAny>(s).is_ok()
}

/// Convert a JSON-string column. Rows that are not valid JSON become null (with a
/// warning and a counter) instead of failing the whole batch.
fn json_column_to_variant(name: &str, strings: &StringArray) -> Result<VariantArray, ArrowError> {
    let input: ArrayRef = Arc::new(strings.clone());
    if let Ok(variant) = json_to_variant(&input) {
        return Ok(variant);
    }

    let mut invalid: u64 = 0;
    let cleaned: StringArray = strings
        .iter()
        .map(|v| {
            v.filter(|s| {
                let ok = is_valid_json(s);
                if !ok {
                    invalid += 1;
                }
                ok
            })
        })
        .collect();
    tracing::warn!(
        column = name,
        rows = invalid,
        "Invalid JSON in column; writing null VARIANT for those rows"
    );
    metrics::counter!("otlp.variant.invalid_json", "column" => name.to_string()).increment(invalid);

    let cleaned: ArrayRef = Arc::new(cleaned);
    json_to_variant(&cleaned)
}

/// Convert the log body. JSON objects/arrays keep their structure; any other text
/// (including numbers/bools, which the upstream encoder stringifies) becomes a
/// string variant, so no body is lost.
fn body_to_variant(strings: &StringArray) -> Result<VariantArray, ArrowError> {
    let normalized: StringArray = strings
        .iter()
        .map(|v| {
            v.and_then(|s| {
                let structured = matches!(s.trim_start().as_bytes().first(), Some(b'{' | b'['))
                    && is_valid_json(s);
                if structured {
                    Some(Cow::Borrowed(s))
                } else {
                    serde_json::to_string(s).ok().map(Cow::Owned)
                }
            })
        })
        .collect();
    let normalized: ArrayRef = Arc::new(normalized);
    json_to_variant(&normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ShredType, VariantConfig};
    use arrow::datatypes::Field;
    use parquet_variant::Variant;
    use parquet_variant_compute::{unshred_variant, VariantType};

    fn traces_batch(attrs: Vec<Option<&str>>) -> RecordBatch {
        let n = attrs.len();
        let schema = Arc::new(Schema::new(vec![
            Field::new("service_name", DataType::Utf8, false),
            Field::new("span_attributes", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["svc"; n])) as ArrayRef,
                Arc::new(StringArray::from(attrs)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn config(shred: &[(&str, ShredType)]) -> VariantConfig {
        let mut cfg = VariantConfig {
            enabled: true,
            ..VariantConfig::default()
        };
        if !shred.is_empty() {
            let keys = shred.iter().map(|(k, t)| (k.to_string(), *t)).collect();
            cfg.shred.traces.insert("span_attributes".to_string(), keys);
        }
        cfg
    }

    #[test]
    fn converts_json_column_to_variant() {
        let plan = VariantPlan::from_config(&config(&[])).unwrap();
        let batch = traces_batch(vec![Some(r#"{"a":1,"b":"x"}"#), None]);
        let out = plan
            .apply(&batch, SignalType::Traces, None)
            .unwrap()
            .unwrap();

        let field = out.schema().field(1).clone();
        assert!(field.has_valid_extension_type::<VariantType>());
        // untouched column keeps its type
        assert_eq!(out.schema().field(0).data_type(), &DataType::Utf8);

        let variant = VariantArray::try_new(out.column(1).as_ref()).unwrap();
        assert_eq!(variant.len(), 2);
        assert!(variant.is_null(1));
        let obj = variant.value(0);
        assert_eq!(obj.get_object_field("a"), Some(Variant::Int8(1)));
        assert_eq!(obj.get_object_field("b"), Some(Variant::from("x")));
    }

    #[test]
    fn invalid_json_row_becomes_null() {
        let plan = VariantPlan::from_config(&config(&[])).unwrap();
        let batch = traces_batch(vec![Some(r#"{"a":1}"#), Some("not json {"), Some("{}")]);
        let out = plan
            .apply(&batch, SignalType::Traces, None)
            .unwrap()
            .unwrap();
        let variant = VariantArray::try_new(out.column(1).as_ref()).unwrap();
        assert!(variant.is_valid(0));
        assert!(variant.is_null(1));
        assert!(variant.is_valid(2));
    }

    #[test]
    fn shreds_configured_keys_without_splitting_dots() {
        let plan = VariantPlan::from_config(&config(&[
            ("http.request.method", ShredType::Utf8),
            ("http.response.status_code", ShredType::Int64),
        ]))
        .unwrap();
        let batch = traces_batch(vec![
            Some(r#"{"http.request.method":"GET","http.response.status_code":200,"other":true}"#),
            Some(r#"{"http.request.method":"POST"}"#),
        ]);
        let out = plan
            .apply(&batch, SignalType::Traces, None)
            .unwrap()
            .unwrap();
        let variant = VariantArray::try_new(out.column(1).as_ref()).unwrap();

        let typed = variant.typed_value_column().expect("shredded typed_value");
        let DataType::Struct(fields) = typed.data_type() else {
            panic!("typed_value should be a struct, got {}", typed.data_type());
        };
        let names: Vec<&str> = fields.iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, ["http.request.method", "http.response.status_code"]);

        // The full value is still reconstructable, including unshredded keys.
        let first = unshred_variant(&variant).unwrap();
        let first = first.value(0);
        assert_eq!(first.get_object_field("other"), Some(Variant::BooleanTrue));
        assert_eq!(
            first.get_object_field("http.request.method"),
            Some(Variant::from("GET"))
        );
    }

    #[test]
    fn body_keeps_structure_and_wraps_plain_text() {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("hello world"),
                Some(r#"{"k":"v"}"#),
                Some("{not json"),
                None,
            ])) as ArrayRef],
        )
        .unwrap();
        let mut cfg = config(&[]);
        cfg.columns = vec!["body".to_string()];
        let plan = VariantPlan::from_config(&cfg).unwrap();
        let out = plan.apply(&batch, SignalType::Logs, None).unwrap().unwrap();
        let variant = VariantArray::try_new(out.column(0).as_ref()).unwrap();

        assert_eq!(variant.value(0), Variant::from("hello world"));
        assert_eq!(
            variant.value(1).get_object_field("k"),
            Some(Variant::from("v"))
        );
        assert_eq!(variant.value(2), Variant::from("{not json"));
        assert!(variant.is_null(3));
    }

    #[test]
    fn skips_signals_and_columns_that_do_not_apply() {
        let plan = VariantPlan::from_config(&config(&[])).unwrap();
        let batch = traces_batch(vec![Some("{}")]);
        // span_attributes is not a metrics column
        assert!(plan
            .apply(&batch, SignalType::Metrics, Some("gauge"))
            .unwrap()
            .is_none());
        assert!(plan
            .apply(&batch, SignalType::Metrics, Some("summary"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn written_parquet_has_variant_logical_type() {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let plan =
            VariantPlan::from_config(&config(&[("http.request.method", ShredType::Utf8)])).unwrap();
        let batch = traces_batch(vec![Some(r#"{"http.request.method":"GET","x":1}"#)]);
        let out = plan
            .apply(&batch, SignalType::Traces, None)
            .unwrap()
            .unwrap();

        let bytes = otlp2records::to_parquet(&out).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.parquet");
        std::fs::write(&path, bytes).unwrap();

        let builder =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap()).unwrap();
        let read_schema = builder.schema().clone();
        assert!(read_schema
            .field_with_name("span_attributes")
            .unwrap()
            .has_valid_extension_type::<VariantType>());

        let mut reader = builder.build().unwrap();
        let read = reader.next().unwrap().unwrap();
        let idx = read.schema().index_of("span_attributes").unwrap();
        let variant = VariantArray::try_new(read.column(idx).as_ref()).unwrap();
        assert!(variant.typed_value_column().is_some());
        let unshredded = unshred_variant(&variant).unwrap();
        assert_eq!(
            unshredded.value(0).get_object_field("x"),
            Some(Variant::Int8(1))
        );
    }

    #[test]
    fn value_not_matching_shred_type_is_kept_in_residual() {
        let plan =
            VariantPlan::from_config(&config(&[("http.status_code", ShredType::Int64)])).unwrap();
        let batch = traces_batch(vec![
            Some(r#"{"http.status_code":200}"#),
            Some(r#"{"http.status_code":"n/a"}"#),
        ]);
        let out = plan
            .apply(&batch, SignalType::Traces, None)
            .unwrap()
            .unwrap();
        let variant = VariantArray::try_new(out.column(1).as_ref()).unwrap();

        // Round-trips losslessly: the mismatched string is not dropped or coerced.
        let back = unshred_variant(&variant).unwrap();
        assert_eq!(
            back.value(0)
                .get_object_field("http.status_code")
                .and_then(|v| v.as_int64()),
            Some(200)
        );
        assert_eq!(
            back.value(1).get_object_field("http.status_code"),
            Some(Variant::from("n/a"))
        );
    }

    /// Every signal from real OTLP fixtures converts, encodes to Parquet, and
    /// reads back with all configured columns typed as VARIANT.
    #[test]
    fn real_fixtures_round_trip_for_all_signals() {
        use otlp2records::{transform_logs, transform_metrics, transform_traces, InputFormat};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let read = |f: &str| {
            std::fs::read(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("testdata")
                    .join(f),
            )
            .unwrap()
        };
        let mut cfg = VariantConfig {
            enabled: true,
            columns: [
                "resource_attributes",
                "scope_attributes",
                "log_attributes",
                "span_attributes",
                "metric_attributes",
                "body",
                "events_json",
                "links_json",
                "exemplars_json",
            ]
            .map(String::from)
            .to_vec(),
            ..VariantConfig::default()
        };
        let one = |k: &str| -> std::collections::BTreeMap<String, ShredType> {
            [(k.to_string(), ShredType::Utf8)].into()
        };
        cfg.shred
            .traces
            .insert("span_attributes".into(), one("http.method"));
        cfg.shred
            .traces
            .insert("resource_attributes".into(), one("service.name"));
        cfg.shred
            .logs
            .insert("resource_attributes".into(), one("service.name"));
        let plan = VariantPlan::from_config(&cfg).unwrap();

        let mut cases: Vec<(RecordBatch, SignalType, Option<&str>)> = vec![
            (
                transform_logs(&read("logs.pb"), InputFormat::Protobuf).unwrap(),
                SignalType::Logs,
                None,
            ),
            (
                transform_traces(&read("traces.pb"), InputFormat::Protobuf).unwrap(),
                SignalType::Traces,
                None,
            ),
        ];
        for (file, kind) in [
            ("metrics_gauge.pb", "gauge"),
            ("metrics_sum.pb", "sum"),
            ("metrics_histogram.pb", "histogram"),
            ("metrics_exponential_histogram.pb", "exponential_histogram"),
        ] {
            let m = transform_metrics(&read(file), InputFormat::Protobuf).unwrap();
            let batch = match kind {
                "gauge" => m.gauge,
                "sum" => m.sum,
                "histogram" => m.histogram,
                _ => m.exp_histogram,
            }
            .unwrap_or_else(|| panic!("{file} produced no {kind} batch"));
            cases.push((batch, SignalType::Metrics, Some(kind)));
        }

        let dir = tempfile::tempdir().unwrap();
        for (i, (batch, signal, kind)) in cases.iter().enumerate() {
            let out = plan.apply(batch, *signal, *kind).unwrap().unwrap();
            assert_eq!(out.num_rows(), batch.num_rows());

            let path = dir.path().join(format!("{i}.parquet"));
            std::fs::write(&path, otlp2records::to_parquet(&out).unwrap()).unwrap();
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(&path).unwrap())
                    .unwrap();
            let schema = builder.schema().clone();
            let rows: usize = builder
                .build()
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            assert_eq!(rows, batch.num_rows(), "{signal:?} {kind:?}");

            let mut converted = 0;
            for f in schema.fields() {
                let want = cfg.columns.iter().any(|c| c == f.name());
                assert_eq!(
                    f.has_valid_extension_type::<VariantType>(),
                    want,
                    "{signal:?} {kind:?} column {}",
                    f.name()
                );
                converted += usize::from(want);
            }
            assert!(converted >= 2, "{signal:?} {kind:?} converted {converted}");
        }
    }
}
