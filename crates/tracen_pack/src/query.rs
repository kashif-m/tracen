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
            validate_filter_map(definition, &query.view, &config.filters, &mut filters)?;

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
            validate_param_map(
                definition,
                &query.read_model,
                &read_model.params,
                &mut params,
            )?;

            Ok(PackExecutionPlan::ReadModel(ReadModelQueryPlan {
                read_model_name: query.read_model,
                params,
            }))
        }
    }
}

fn validate_filter_map(
    definition: &TrackerDefinition,
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
        if config.optional && filters.get(key).is_some_and(Value::is_null) {
            filters.remove(key);
        }
        match filters.get(key) {
            Some(value) => validate_type_ref(
                definition,
                &config.type_ref,
                value,
                &format!("filter '{}'", key),
            )?,
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
    definition: &TrackerDefinition,
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
        if field.optional && params.get(&field.name).is_some_and(Value::is_null) {
            params.remove(&field.name);
        }
        match params.get(&field.name) {
            Some(value) => validate_type_ref(
                definition,
                &field.type_ref,
                value,
                &format!("param '{}'", field.name),
            )?,
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

fn validate_type_ref(
    definition: &TrackerDefinition,
    type_ref: &str,
    value: &Value,
    context: &str,
) -> Result<(), PackError> {
    if matches_type_ref(definition, type_ref, value, 0) {
        Ok(())
    } else {
        Err(PackError::InvalidQuery(format!(
            "{context} does not match declared type '{}'",
            type_ref
        )))
    }
}

fn matches_type_ref(
    definition: &TrackerDefinition,
    type_ref: &str,
    value: &Value,
    depth: usize,
) -> bool {
    // Bound recursive aliases/objects independently of untrusted input size.
    if depth > 64 {
        return false;
    }
    let type_ref = type_ref.trim();
    if let Some(inner) = type_ref.strip_suffix("[]") {
        return value.as_array().is_some_and(|items| {
            items
                .iter()
                .all(|item| matches_type_ref(definition, inner, item, depth + 1))
        });
    }
    match type_ref {
        "string" | "text" => value.is_string(),
        "number" | "float" => value.as_f64().is_some(),
        "int" => value.as_i64().is_some(),
        "bool" | "boolean" => value.is_boolean(),
        "json" | "unknown" | "any" => true,
        "null" => value.is_null(),
        _ => match definition.types().iter().find(|ty| ty.name == type_ref) {
            Some(ty) => match ty.kind {
                tracen_ir::PackTypeKind::Enum => value
                    .as_str()
                    .is_some_and(|item| ty.variants.iter().any(|variant| variant == item)),
                tracen_ir::PackTypeKind::Alias => ty
                    .target
                    .as_ref()
                    .is_some_and(|target| matches_type_ref(definition, target, value, depth + 1)),
                tracen_ir::PackTypeKind::Object => value.as_object().is_some_and(|object| {
                    ty.fields.iter().all(|field| match object.get(&field.name) {
                        None => field.optional,
                        Some(value) if value.is_null() && field.optional => true,
                        Some(value) => {
                            matches_type_ref(definition, &field.type_ref, value, depth + 1)
                        }
                    })
                }),
            },
            // Extern types and opaque TS expressions are checked by the typed adapter.
            None => true,
        },
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use serde_json::json;

    fn definition() -> TrackerDefinition {
        tracen_dsl::compile(r#"
tracker "hydration" v1 {
  fields { amount: float }
  types {
    type "Unit" {
      kind = "enum"
      variants = ["ml", "oz"]
      emit_rust = true
    }
    type "Filter" {
      fields = {"unit":{"type":"Unit"},"limit":{"type":"int","optional":true}}
      emit_rust = true
    }
  }
  read_models {
    read_model "history" {
      params = {"label":{"type":"text","optional":true},"enabled":{"type":"bool","optional":true},"filter":{"type":"Filter","optional":true},"units":{"type":"Unit[]","optional":true}}
      fields = {"count":{"type":"int"}}
    }
  }
}
"#).expect("valid definition")
    }

    #[test]
    fn query_contract_rejects_invalid_aliases_and_named_values() {
        let def = definition();
        for params in [
            json!({"label":42}),
            json!({"enabled":"yes"}),
            json!({"filter":{"unit":"litres"}}),
            json!({"filter":{}}),
            json!({"filter":{"unit":"ml","limit":1.5}}),
            json!({"units":["ml",42]}),
        ] {
            let mut query = params;
            query["read_model"] = json!("history");
            assert!(
                parse_query_json(&def, &query.to_string()).is_err(),
                "accepted {query}"
            );
        }
    }

    #[test]
    fn optional_query_null_matches_omission_and_accepts_valid_named_values() {
        let def = definition();
        let absent = parse_query_json(&def, r#"{"read_model":"history"}"#).unwrap();
        let null = parse_query_json(
            &def,
            r#"{"read_model":"history","label":null,"enabled":null,"filter":null,"units":null}"#,
        )
        .unwrap();
        assert_eq!(absent, null);
        parse_query_json(&def, r#"{"read_model":"history","label":"water","enabled":true,"filter":{"unit":"ml","limit":null},"units":["ml","oz"]}"#).unwrap();
    }
}
