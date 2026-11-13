//! Configuration for Parquet VARIANT output.
//!
//! OTLP semi-structured columns (attribute maps, event/link/exemplar arrays and
//! the log body) are JSON strings by default. When enabled, the selected columns
//! are written as Parquet `VARIANT`, optionally with chosen attribute keys
//! "shredded" into typed columns for fast, statistics-friendly access.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// JSON-string columns that may be written as VARIANT, per signal.
pub const LOGS_VARIANT_COLUMNS: &[&str] = &[
    "resource_attributes",
    "scope_attributes",
    "log_attributes",
    "body",
];
pub const TRACES_VARIANT_COLUMNS: &[&str] = &[
    "resource_attributes",
    "scope_attributes",
    "span_attributes",
    "events_json",
    "links_json",
];
pub const METRICS_VARIANT_COLUMNS: &[&str] = &[
    "resource_attributes",
    "scope_attributes",
    "metric_attributes",
    "exemplars_json",
];

/// Columns that hold a JSON object of attributes and can therefore be shredded.
const SHREDDABLE_COLUMNS: &[&str] = &[
    "resource_attributes",
    "scope_attributes",
    "log_attributes",
    "span_attributes",
    "metric_attributes",
];

/// Metric kinds that are written to storage (summaries are skipped upstream).
pub const METRIC_KINDS: &[&str] = &["gauge", "sum", "histogram", "exponential_histogram"];

/// Parquet writer options.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ParquetConfig {
    #[serde(default)]
    pub variant: VariantConfig,
}

/// Parquet VARIANT output options (`[parquet.variant]`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantConfig {
    /// Write the selected columns as VARIANT. Requires the `variant` cargo feature.
    #[serde(default)]
    pub enabled: bool,

    /// Columns to write as VARIANT. Names not present in a signal's schema are
    /// ignored for that signal.
    #[serde(default = "default_variant_columns")]
    pub columns: Vec<String>,

    /// Attribute keys to shred into typed columns.
    #[serde(default)]
    pub shred: ShredConfig,
}

impl Default for VariantConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            columns: default_variant_columns(),
            shred: ShredConfig::default(),
        }
    }
}

fn default_variant_columns() -> Vec<String> {
    [
        "resource_attributes",
        "scope_attributes",
        "log_attributes",
        "span_attributes",
        "metric_attributes",
    ]
    .iter()
    .map(|s| (*s).to_string())
    .collect()
}

/// Primitive types an attribute key can be shredded as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ShredType {
    Bool,
    Int32,
    Int64,
    Float64,
    Utf8,
}

/// `column name -> (attribute key -> type)`.
pub type ColumnShred = BTreeMap<String, BTreeMap<String, ShredType>>;

/// Shredding configuration (`[parquet.variant.shred.*]`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShredConfig {
    #[serde(default)]
    pub logs: ColumnShred,
    #[serde(default)]
    pub traces: ColumnShred,
    #[serde(default)]
    pub metrics: MetricsShredConfig,
}

/// Metrics shredding: column tables apply to every metric type, and an optional
/// per-type table (`gauge`, `sum`, `histogram`, `exponential_histogram`)
/// replaces the shared one for that type.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsShredConfig {
    #[serde(flatten)]
    pub shared: ColumnShred,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gauge: Option<ColumnShred>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sum: Option<ColumnShred>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub histogram: Option<ColumnShred>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exponential_histogram: Option<ColumnShred>,
}

impl MetricsShredConfig {
    /// Shredding that applies to `kind`: its override if present, else the shared table.
    pub fn for_kind(&self, kind: &str) -> &ColumnShred {
        let over = match kind {
            "gauge" => self.gauge.as_ref(),
            "sum" => self.sum.as_ref(),
            "histogram" => self.histogram.as_ref(),
            "exponential_histogram" => self.exponential_histogram.as_ref(),
            _ => None,
        };
        over.unwrap_or(&self.shared)
    }
}

impl VariantConfig {
    /// Validate column names and shred tables. A disabled config is always valid.
    pub fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if !cfg!(feature = "variant") {
            bail!(
                "parquet.variant.enabled = true requires a build with the `variant` cargo feature \
                 (cargo build --features variant)"
            );
        }

        let known = |c: &str| {
            LOGS_VARIANT_COLUMNS.contains(&c)
                || TRACES_VARIANT_COLUMNS.contains(&c)
                || METRICS_VARIANT_COLUMNS.contains(&c)
        };
        for col in &self.columns {
            if !known(col) {
                bail!(
                    "parquet.variant.columns contains unsupported column '{col}'. Supported: {}",
                    all_columns().join(", ")
                );
            }
        }

        self.validate_shred("logs", LOGS_VARIANT_COLUMNS, &self.shred.logs)?;
        self.validate_shred("traces", TRACES_VARIANT_COLUMNS, &self.shred.traces)?;
        self.validate_shred(
            "metrics",
            METRICS_VARIANT_COLUMNS,
            &self.shred.metrics.shared,
        )?;
        for kind in METRIC_KINDS {
            let table = self.shred.metrics.for_kind(kind);
            self.validate_shred(&format!("metrics.{kind}"), METRICS_VARIANT_COLUMNS, table)?;
        }
        Ok(())
    }

    fn validate_shred(
        &self,
        scope: &str,
        signal_columns: &[&str],
        table: &ColumnShred,
    ) -> Result<()> {
        for (column, keys) in table {
            if !signal_columns.contains(&column.as_str()) {
                bail!("parquet.variant.shred.{scope}: '{column}' is not a column of that signal");
            }
            if !SHREDDABLE_COLUMNS.contains(&column.as_str()) {
                bail!(
                    "parquet.variant.shred.{scope}: '{column}' cannot be shredded \
                     (only attribute-map columns can)"
                );
            }
            if !self.columns.contains(column) {
                bail!(
                    "parquet.variant.shred.{scope}: '{column}' must also be listed in \
                     parquet.variant.columns"
                );
            }
            if keys.is_empty() || keys.keys().any(|k| k.is_empty()) {
                bail!("parquet.variant.shred.{scope}.{column}: keys must be non-empty");
            }
        }
        Ok(())
    }
}

fn all_columns() -> Vec<&'static str> {
    let mut all: Vec<&'static str> = Vec::new();
    for c in LOGS_VARIANT_COLUMNS
        .iter()
        .chain(TRACES_VARIANT_COLUMNS)
        .chain(METRICS_VARIANT_COLUMNS)
    {
        if !all.contains(c) {
            all.push(c);
        }
    }
    all
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RuntimeConfig;

    const TOML: &str = r#"
[storage]
backend = "fs"
[storage.fs]
path = "./data"

[parquet.variant]
enabled = true
columns = ["span_attributes", "resource_attributes", "metric_attributes", "body"]

[parquet.variant.shred.traces]
span_attributes = { "http.request.method" = "utf8", "http.response.status_code" = "int64" }

[parquet.variant.shred.metrics]
metric_attributes = { "http.route" = "utf8" }

[parquet.variant.shred.metrics.gauge]
metric_attributes = { "cpu" = "float64" }
"#;

    fn parse(toml: &str) -> VariantConfig {
        toml::from_str::<RuntimeConfig>(toml)
            .unwrap()
            .parquet
            .variant
    }

    #[test]
    fn defaults_are_disabled_and_valid() {
        let cfg = VariantConfig::default();
        assert!(!cfg.enabled);
        assert!(cfg.validate().is_ok());
        assert!(cfg.columns.contains(&"span_attributes".to_string()));
    }

    #[test]
    fn parses_toml_with_shared_and_per_type_metrics() {
        let cfg = parse(TOML);
        assert!(cfg.enabled);
        assert_eq!(
            cfg.shred.traces["span_attributes"]["http.response.status_code"],
            ShredType::Int64
        );
        // shared table applies to types without an override ...
        assert!(cfg.shred.metrics.for_kind("sum")["metric_attributes"].contains_key("http.route"));
        // ... and a per-type table replaces it
        let gauge = cfg.shred.metrics.for_kind("gauge");
        assert!(gauge["metric_attributes"].contains_key("cpu"));
        assert!(!gauge["metric_attributes"].contains_key("http.route"));
    }

    #[cfg(feature = "variant")]
    #[test]
    fn valid_config_passes_validation() {
        assert!(parse(TOML).validate().is_ok());
    }

    #[cfg(not(feature = "variant"))]
    #[test]
    fn enabling_without_the_feature_is_an_error() {
        let err = parse(TOML).validate().unwrap_err().to_string();
        assert!(err.contains("variant"), "{err}");
    }

    #[cfg(feature = "variant")]
    #[test]
    fn rejects_bad_columns_and_shred_tables() {
        let mut cfg = parse(TOML);
        cfg.columns.push("nope".into());
        assert!(cfg.validate().is_err());

        // shredded column must be enabled in `columns`
        let mut cfg = parse(TOML);
        cfg.columns.retain(|c| c != "span_attributes");
        assert!(cfg.validate().is_err());

        // arrays/body cannot be shredded
        let mut cfg = parse(TOML);
        cfg.columns.push("events_json".into());
        cfg.shred.traces.insert(
            "events_json".into(),
            [("k".to_string(), ShredType::Utf8)].into(),
        );
        assert!(cfg.validate().is_err());

        // column from another signal
        let mut cfg = parse(TOML);
        cfg.shred.traces.insert(
            "metric_attributes".into(),
            [("k".to_string(), ShredType::Utf8)].into(),
        );
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn env_overrides_apply() {
        use crate::config::{EnvSource, RuntimeConfig};
        use std::collections::HashMap;

        struct Env(HashMap<&'static str, &'static str>);
        impl EnvSource for Env {
            fn get(&self, key: &str) -> Option<String> {
                self.0.get(key).map(|v| v.to_string())
            }
            fn get_raw(&self, _key: &str) -> Option<String> {
                None
            }
        }

        let mut cfg = RuntimeConfig::from_platform_defaults(crate::config::Platform::Server);
        let env = Env(HashMap::from([
            ("PARQUET_VARIANT_ENABLED", "true"),
            ("PARQUET_VARIANT_COLUMNS", "span_attributes, body"),
            (
                "PARQUET_VARIANT_SHRED",
                r#"{"traces":{"span_attributes":{"http.route":"utf8"}},"metrics":{"gauge":{"metric_attributes":{"a":"int32"}}}}"#,
            ),
        ]));
        cfg.apply_env_overrides_from(&env).unwrap();
        let v = &cfg.parquet.variant;
        assert!(v.enabled);
        assert_eq!(v.columns, ["span_attributes", "body"]);
        assert_eq!(
            v.shred.traces["span_attributes"]["http.route"],
            ShredType::Utf8
        );
        assert_eq!(
            v.shred.metrics.for_kind("gauge")["metric_attributes"]["a"],
            ShredType::Int32
        );
    }
}
