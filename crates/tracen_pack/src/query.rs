use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use tracen_ir::TrackerDefinition;

use crate::PackError;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PackExecutionPlan {
    View(ViewQueryPlan),
    ReadModel(ReadModelQueryPlan),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ViewQueryPlan {
    pub view_name: String,
    pub metric_key: String,
    pub group_by_key: String,
    pub filters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReadModelQueryPlan {
    pub read_model_name: String,
    pub params: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawPackQuery {
    View(RawViewQuery),
    ReadModel(RawReadModelQuery),
}

#[derive(Debug, Deserialize)]
struct RawViewQuery {
    view: String,
    metric: String,
    group_by: String,
    #[serde(flatten)]
    filters: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
struct RawReadModelQuery {
    read_model: String,
    #[serde(flatten)]
    params: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeViewConfig {
    #[serde(default)]
    pub(crate) result_kind: Option<String>,
    #[serde(default)]
    pub(crate) count_metric: Option<String>,
    #[serde(default)]
    pub(crate) metrics: BTreeMap<String, RuntimeMetricConfig>,
    #[serde(default)]
    pub(crate) group_by: BTreeMap<String, RuntimeGroupByConfig>,
    #[serde(default)]
    pub(crate) filters: BTreeMap<String, RuntimeFilterConfig>,
    #[serde(default)]
    pub(crate) response_fields: BTreeMap<String, RuntimeResponseFieldConfig>,
    #[serde(default)]
    pub(crate) totals: BTreeMap<String, RuntimeTotalFieldConfig>,
    #[serde(default)]
    pub(crate) qa: BTreeMap<String, RuntimeQaFieldConfig>,
    #[serde(default)]
    pub(crate) enrich_fields: BTreeMap<String, RuntimeEnrichFieldConfig>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeMetricConfig {
    pub(crate) metric: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeGroupByConfig {
    pub(crate) field: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeFilterConfig {
    pub(crate) field: String,
    #[serde(default = "default_filter_op")]
    pub(crate) op: String,
    #[serde(rename = "type")]
    pub(crate) type_ref: String,
    #[serde(default)]
    pub(crate) optional: bool,
    #[serde(default)]
    pub(crate) metrics: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeResponseFieldConfig {
    pub(crate) from_filter: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeTotalFieldConfig {
    pub(crate) kind: String,
    #[serde(default)]
    pub(crate) metric: Option<String>,
    #[serde(default)]
    pub(crate) field: Option<String>,
    #[serde(default)]
    pub(crate) coerce: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeQaFieldConfig {
    pub(crate) kind: String,
    pub(crate) event_field: String,
    #[serde(default)]
    pub(crate) lookup_fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuntimeEnrichFieldConfig {
    pub(crate) lookup_field: String,
    #[serde(default)]
    pub(crate) lookup_fields: Vec<String>,
    pub(crate) catalog_field: String,
}

pub(crate) fn parse_query_json(
    definition: &TrackerDefinition,
    query_json: &str,
) -> Result<PackExecutionPlan, PackError> {
    let raw: RawPackQuery = serde_json::from_str(query_json)
        .map_err(|err| PackError::InvalidQuery(format!("parse pack query: {err}")))?;

    match raw {
        RawPackQuery::View(query) => {
            let view = definition
                .views()
                .iter()
                .find(|view| view.name == query.view)
                .ok_or_else(|| PackError::InvalidQuery(format!("unknown view '{}'", query.view)))?;
            let config_value = view.params.get("config").ok_or_else(|| {
                PackError::InvalidQuery(format!("view '{}' missing config", query.view))
            })?;
            let config: RuntimeViewConfig =
                serde_json::from_value(config_value.clone()).map_err(|err| {
                    PackError::InvalidQuery(format!("view '{}' config: {err}", query.view))
                })?;

            let metric_names = config
                .metrics
                .values()
                .map(|metric| metric.metric.clone())
                .collect::<BTreeSet<_>>();
            if !metric_names.contains(&query.metric) {
                return Err(PackError::InvalidQuery(format!(
                    "metric '{}' is not declared for view '{}'",
                    query.metric, query.view
                )));
            }

            if !config.group_by.contains_key(&query.group_by) {
                return Err(PackError::InvalidQuery(format!(
                    "group_by '{}' is not declared for view '{}'",
                    query.group_by, query.view
                )));
            }

            let mut filters = query.filters;
            validate_filter_map(&query.view, &config.filters, &mut filters)?;

            Ok(PackExecutionPlan::View(ViewQueryPlan {
                view_name: query.view,
                metric_key: query.metric,
                group_by_key: query.group_by,
                filters,
            }))
        }
        RawPackQuery::ReadModel(query) => {
            let read_model = definition
                .read_models()
                .iter()
                .find(|model| model.name == query.read_model)
                .ok_or_else(|| {
                    PackError::InvalidQuery(format!("unknown read_model '{}'", query.read_model))
                })?;

            let mut params = query.params;
            validate_param_map(&query.read_model, &read_model.params, &mut params)?;

            Ok(PackExecutionPlan::ReadModel(ReadModelQueryPlan {
                read_model_name: query.read_model,
                params,
            }))
        }
    }
}

fn validate_filter_map(
    view_name: &str,
    declared: &BTreeMap<String, RuntimeFilterConfig>,
    filters: &mut BTreeMap<String, Value>,
) -> Result<(), PackError> {
    for key in filters.keys() {
        if !declared.contains_key(key) {
            return Err(PackError::InvalidQuery(format!(
                "filter '{}' is not declared for view '{}'",
                key, view_name
            )));
        }
    }

    for (key, config) in declared {
        match filters.get(key) {
            Some(value) => {
                validate_type_ref(&config.type_ref, value, &format!("filter '{}'", key))?
            }
            None if !config.optional => {
                return Err(PackError::InvalidQuery(format!(
                    "required filter '{}' is missing for view '{}'",
                    key, view_name
                )))
            }
            None => {}
        }
    }
    Ok(())
}

fn default_filter_op() -> String {
    "eq".to_string()
}

fn validate_param_map(
    read_model_name: &str,
    declared: &[tracen_ir::SchemaFieldDefinition],
    params: &mut BTreeMap<String, Value>,
) -> Result<(), PackError> {
    let declared_map = declared
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect::<BTreeMap<_, _>>();

    for key in params.keys() {
        if !declared_map.contains_key(key.as_str()) {
            return Err(PackError::InvalidQuery(format!(
                "param '{}' is not declared for read_model '{}'",
                key, read_model_name
            )));
        }
    }

    for field in declared {
        match params.get(&field.name) {
            Some(value) => {
                validate_type_ref(&field.type_ref, value, &format!("param '{}'", field.name))?
            }
            None if !field.optional => {
                return Err(PackError::InvalidQuery(format!(
                    "required param '{}' is missing for read_model '{}'",
                    field.name, read_model_name
                )))
            }
            None => {}
        }
    }
    Ok(())
}

fn validate_type_ref(type_ref: &str, value: &Value, context: &str) -> Result<(), PackError> {
    let valid = match type_ref.trim() {
        "string" => value.is_string(),
        "number" => value.is_number(),
        "int" => value.as_i64().is_some(),
        "float" => value.as_f64().is_some(),
        "boolean" => value.is_boolean(),
        "string[]" => value
            .as_array()
            .is_some_and(|items| items.iter().all(Value::is_string)),
        "number[]" => value
            .as_array()
            .is_some_and(|items| items.iter().all(Value::is_number)),
        "int[]" => value
            .as_array()
            .is_some_and(|items| items.iter().all(|item| item.as_i64().is_some())),
        "float[]" => value
            .as_array()
            .is_some_and(|items| items.iter().all(|item| item.as_f64().is_some())),
        "boolean[]" => value
            .as_array()
            .is_some_and(|items| items.iter().all(Value::is_boolean)),
        "json" | "unknown" => true,
        "json[]" | "unknown[]" => value.as_array().is_some(),
        _ => true,
    };

    if valid {
        Ok(())
    } else {
        Err(PackError::InvalidQuery(format!(
            "{context} does not match declared type '{}'",
            type_ref
        )))
    }
}
