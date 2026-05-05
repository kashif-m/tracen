use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use tracen_analytics::Distribution;
use tracen_engine::{self, MetricComputeOptions, MetricFilter, MetricFilterOp};
use tracen_ir::{EventId, GroupByDimension, NormalizedEvent, Timestamp, TrackerDefinition};

use crate::query::{RuntimeQaFieldConfig, RuntimeViewConfig, ViewQueryPlan};
use crate::runtime_events::{to_pack_event_error, PackInputEvent};
use crate::PackError;

pub(crate) fn execute_view_query(
    definition: &TrackerDefinition,
    events: &[PackInputEvent],
    offset_minutes: i32,
    catalog_json: &Value,
    query: &ViewQueryPlan,
) -> Result<Value, PackError> {
    let view = definition
        .views()
        .iter()
        .find(|view| view.name == query.view_name)
        .ok_or_else(|| PackError::InvalidQuery(format!("unknown view '{}'", query.view_name)))?;
    let config_value = view.params.get("config").ok_or_else(|| {
        PackError::InvalidQuery(format!("view '{}' missing config", query.view_name))
    })?;
    let config: RuntimeViewConfig =
        serde_json::from_value(config_value.clone()).map_err(|err| {
            PackError::InvalidQuery(format!("view '{}' config: {err}", query.view_name))
        })?;
    let engine_events = pack_events_for_engine(definition, events, catalog_json, &config)?;

    match config.result_kind.as_deref().unwrap_or("metric_series") {
        "metric_series" => execute_metric_series_view(definition, &engine_events, &config, query),
        "distribution" => execute_distribution_view(
            definition,
            &engine_events,
            catalog_json,
            &config,
            query,
            offset_minutes,
        ),
        other => Err(PackError::InvalidQuery(format!(
            "unsupported result_kind '{}' for view '{}'",
            other, query.view_name
        ))),
    }
}

fn execute_metric_series_view(
    definition: &TrackerDefinition,
    events: &[NormalizedEvent],
    config: &RuntimeViewConfig,
    query: &ViewQueryPlan,
) -> Result<Value, PackError> {
    let metric_name = config
        .metrics
        .get(&query.metric_key)
        .map(|metric| metric.metric.clone())
        .ok_or_else(|| {
            PackError::InvalidQuery(format!(
                "metric '{}' is not declared for view '{}'",
                query.metric_key, query.view_name
            ))
        })?;
    let group_field = config
        .group_by
        .get(&query.group_by_key)
        .map(|group| group.field.clone())
        .ok_or_else(|| {
            PackError::InvalidQuery(format!(
                "group_by '{}' is not declared for view '{}'",
                query.group_by_key, query.view_name
            ))
        })?;
    let filters = build_metric_filters(config, query)?;
    let value_map = grouped_metric_values(
        tracen_engine::compute_metric_by_name(
            definition,
            events,
            &metric_name,
            MetricComputeOptions {
                group_by: Some(vec![GroupByDimension::Field(group_field.clone())]),
                time_window: None,
                filters: filters.clone(),
            },
        )
        .map_err(to_pack_event_error)?,
    );

    let count_metric = config
        .count_metric
        .clone()
        .unwrap_or_else(|| query.metric_key.clone());
    let count_map = grouped_metric_counts(
        tracen_engine::compute_metric_by_name(
            definition,
            events,
            &count_metric,
            MetricComputeOptions {
                group_by: Some(vec![GroupByDimension::Field(group_field)]),
                time_window: None,
                filters,
            },
        )
        .map_err(to_pack_event_error)?,
    );

    let mut points = value_map
        .into_iter()
        .filter_map(|(key, value)| {
            if value <= 0.0 {
                return None;
            }
            let bucket = parse_bucket_key(&key)?;
            let count = *count_map.get(&key).unwrap_or(&0);
            Some(serde_json::json!({
                "label": "",
                "value": value,
                "count": count,
                "bucket": bucket,
            }))
        })
        .collect::<Vec<_>>();
    points.sort_by_key(|point| {
        point
            .get("bucket")
            .and_then(Value::as_i64)
            .unwrap_or_default()
    });

    let mut response = serde_json::Map::new();
    response.insert("metric".into(), Value::String(query.metric_key.clone()));
    response.insert("group_by".into(), Value::String(query.group_by_key.clone()));
    response.insert("points".into(), Value::Array(points));
    for (field_name, field_config) in &config.response_fields {
        if let Some(value) = query.filters.get(&field_config.from_filter) {
            response.insert(field_name.clone(), value.clone());
        }
    }

    Ok(Value::Object(response))
}

fn execute_distribution_view(
    definition: &TrackerDefinition,
    events: &[NormalizedEvent],
    catalog_json: &Value,
    config: &RuntimeViewConfig,
    query: &ViewQueryPlan,
    _offset_minutes: i32,
) -> Result<Value, PackError> {
    let metric_name = config
        .metrics
        .get(&query.metric_key)
        .map(|metric| metric.metric.clone())
        .ok_or_else(|| {
            PackError::InvalidQuery(format!(
                "metric '{}' is not declared for view '{}'",
                query.metric_key, query.view_name
            ))
        })?;
    let group_field = config
        .group_by
        .get(&query.group_by_key)
        .map(|group| group.field.clone())
        .ok_or_else(|| {
            PackError::InvalidQuery(format!(
                "group_by '{}' is not declared for view '{}'",
                query.group_by_key, query.view_name
            ))
        })?;
    let filters = build_metric_filters(config, query)?;
    let grouped = grouped_metric_values(
        tracen_engine::compute_metric_by_name(
            definition,
            events,
            &metric_name,
            MetricComputeOptions {
                group_by: Some(vec![GroupByDimension::Field(group_field)]),
                time_window: None,
                filters,
            },
        )
        .map_err(to_pack_event_error)?,
    );
    let items = Distribution::calculate(
        grouped
            .into_iter()
            .filter(|(_, value)| *value > 0.0)
            .collect::<Vec<_>>(),
    );

    let mut response = serde_json::Map::new();
    response.insert("metric".into(), Value::String(query.metric_key.clone()));
    response.insert("group_by".into(), Value::String(query.group_by_key.clone()));
    response.insert(
        "items".into(),
        serde_json::to_value(items).map_err(|err| PackError::Adapter(err.to_string()))?,
    );
    response.insert(
        "totals".into(),
        Value::Object(compute_totals_block(definition, events, config)?),
    );
    for (field_name, qa_config) in &config.qa {
        if let Some(value) = compute_qa_value(events, catalog_json, qa_config)? {
            response.insert(field_name.clone(), value);
        }
    }
    Ok(Value::Object(response))
}

fn build_metric_filters(
    config: &RuntimeViewConfig,
    query: &ViewQueryPlan,
) -> Result<Vec<MetricFilter>, PackError> {
    let mut filters = Vec::new();
    for (key, value) in &query.filters {
        let Some(filter_config) = config.filters.get(key) else {
            return Err(PackError::InvalidQuery(format!(
                "filter '{}' is not declared for view '{}'",
                key, query.view_name
            )));
        };
        if !filter_config.metrics.is_empty() && !filter_config.metrics.contains(&query.metric_key) {
            continue;
        }
        filters.push(MetricFilter {
            field: filter_config.field.clone(),
            op: parse_metric_filter_op(&filter_config.op)?,
            value: value.clone(),
        });
    }
    Ok(filters)
}

fn parse_metric_filter_op(op: &str) -> Result<MetricFilterOp, PackError> {
    match op {
        "eq" => Ok(MetricFilterOp::Eq),
        "neq" => Ok(MetricFilterOp::Neq),
        "gt" => Ok(MetricFilterOp::Gt),
        "gte" => Ok(MetricFilterOp::Gte),
        "lt" => Ok(MetricFilterOp::Lt),
        "lte" => Ok(MetricFilterOp::Lte),
        other => Err(PackError::InvalidQuery(format!(
            "unsupported filter op '{}'",
            other
        ))),
    }
}

fn compute_totals_block(
    definition: &TrackerDefinition,
    events: &[NormalizedEvent],
    config: &RuntimeViewConfig,
) -> Result<serde_json::Map<String, Value>, PackError> {
    let mut totals = serde_json::Map::new();
    for (output_key, total_config) in &config.totals {
        let value = match total_config.kind.as_str() {
            "metric_total" => {
                let metric_name = total_config.metric.as_ref().ok_or_else(|| {
                    PackError::InvalidQuery(format!(
                        "totals '{}' missing metric declaration",
                        output_key
                    ))
                })?;
                tracen_engine::compute_metric_by_name(
                    definition,
                    events,
                    metric_name,
                    MetricComputeOptions::default(),
                )
                .map_err(to_pack_event_error)?
            }
            "distinct_count" => {
                let field = total_config.field.as_ref().ok_or_else(|| {
                    PackError::InvalidQuery(format!(
                        "totals '{}' missing field declaration",
                        output_key
                    ))
                })?;
                serde_json::json!(distinct_count(events, field))
            }
            other => {
                return Err(PackError::InvalidQuery(format!(
                    "unsupported totals kind '{}'",
                    other
                )))
            }
        };
        totals.insert(
            output_key.clone(),
            coerce_total_value(value, total_config.coerce.as_deref())?,
        );
    }
    Ok(totals)
}

fn coerce_total_value(value: Value, coerce: Option<&str>) -> Result<Value, PackError> {
    match coerce {
        None => Ok(value),
        Some("integer") => {
            let Some(number) = value.as_f64() else {
                return Err(PackError::InvalidQuery(
                    "integer total coercion requires numeric value".to_string(),
                ));
            };
            Ok(serde_json::json!(number.round() as i64))
        }
        Some("float") => {
            let Some(number) = value.as_f64() else {
                return Err(PackError::InvalidQuery(
                    "float total coercion requires numeric value".to_string(),
                ));
            };
            Ok(serde_json::json!(number))
        }
        Some(other) => Err(PackError::InvalidQuery(format!(
            "unsupported totals coercion '{}'",
            other
        ))),
    }
}

fn compute_qa_value(
    events: &[NormalizedEvent],
    catalog_json: &Value,
    config: &RuntimeQaFieldConfig,
) -> Result<Option<Value>, PackError> {
    match config.kind.as_str() {
        "catalog_lookup_miss" => {
            let lookup_fields = if config.lookup_fields.is_empty() {
                vec!["slug".to_string(), "display_name".to_string()]
            } else {
                config.lookup_fields.clone()
            };
            let catalog = catalog_json
                .as_array()
                .ok_or_else(|| PackError::InvalidQuery("catalog must be an array".into()))?;
            let mut lookup = HashSet::new();
            for entry in catalog {
                let Some(object) = entry.as_object() else {
                    continue;
                };
                for field in &lookup_fields {
                    if let Some(value) = object.get(field).and_then(Value::as_str) {
                        lookup.insert(value.to_string());
                        lookup.insert(normalize_lookup_key(value));
                    }
                }
            }

            let misses = events
                .iter()
                .filter(|event| {
                    event
                        .payload()
                        .get(&config.event_field)
                        .and_then(Value::as_str)
                        .map(|value| {
                            !lookup.contains(value)
                                && !lookup.contains(&normalize_lookup_key(value))
                        })
                        .unwrap_or(false)
                })
                .count();
            Ok(Some(serde_json::json!(misses)))
        }
        other => Err(PackError::InvalidQuery(format!(
            "unsupported qa kind '{}'",
            other
        ))),
    }
}

fn pack_events_for_engine(
    definition: &TrackerDefinition,
    events: &[PackInputEvent],
    catalog_json: &Value,
    config: &RuntimeViewConfig,
) -> Result<Vec<NormalizedEvent>, PackError> {
    let catalog_entries = catalog_json
        .as_array()
        .ok_or_else(|| PackError::InvalidQuery("catalog must be an array".into()))?;

    let mut lookup_maps: BTreeMap<String, HashMap<String, Value>> = BTreeMap::new();
    for (output_field, enrich_config) in &config.enrich_fields {
        lookup_maps.insert(
            output_field.clone(),
            build_catalog_lookup_map(catalog_entries, &enrich_config.lookup_fields),
        );
    }

    events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let mut payload = event.payload.clone();
            if let Some(payload_obj) = payload.as_object_mut() {
                for (output_field, enrich_config) in &config.enrich_fields {
                    if payload_obj.contains_key(output_field) {
                        continue;
                    }
                    let Some(lookup_value) = payload_obj
                        .get(&enrich_config.lookup_field)
                        .and_then(Value::as_str)
                    else {
                        continue;
                    };
                    let Some(entry) = lookup_maps
                        .get(output_field)
                        .and_then(|map| map.get(lookup_value))
                    else {
                        continue;
                    };
                    let Some(value) = entry.get(&enrich_config.catalog_field) else {
                        continue;
                    };
                    payload_obj.insert(output_field.clone(), value.clone());
                }
            }

            Ok(NormalizedEvent::new(
                EventId::new(format!("view-{index}-{}", event.ts)),
                definition.tracker_id().clone(),
                Timestamp::new(event.ts),
                payload,
                serde_json::json!({}),
            ))
        })
        .collect()
}

fn build_catalog_lookup_map(entries: &[Value], lookup_fields: &[String]) -> HashMap<String, Value> {
    let mut map = HashMap::new();
    for entry in entries {
        let Some(object) = entry.as_object() else {
            continue;
        };
        for lookup_field in lookup_fields {
            if let Some(value) = object.get(lookup_field).and_then(Value::as_str) {
                map.entry(value.to_string())
                    .or_insert_with(|| entry.clone());
                let normalized = normalize_lookup_key(value);
                if !normalized.is_empty() {
                    map.entry(normalized).or_insert_with(|| entry.clone());
                }
            }
        }
    }
    map
}
fn grouped_metric_values(value: Value) -> HashMap<String, f32> {
    match value {
        Value::Object(map) => map
            .into_iter()
            .filter_map(|(key, value)| value.as_f64().map(|number| (key, number as f32)))
            .collect(),
        Value::Number(number) => number
            .as_f64()
            .map(|value| {
                [("__total__".to_string(), value as f32)]
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default(),
        _ => HashMap::new(),
    }
}

fn grouped_metric_counts(value: Value) -> HashMap<String, i32> {
    match value {
        Value::Object(map) => map
            .into_iter()
            .filter_map(|(key, value)| value.as_f64().map(|number| (key, number as i32)))
            .collect(),
        Value::Number(number) => number
            .as_f64()
            .map(|value| {
                [("__total__".to_string(), value as i32)]
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default(),
        _ => HashMap::new(),
    }
}

fn parse_bucket_key(key: &str) -> Option<i64> {
    if let Ok(value) = key.parse::<i64>() {
        Some(value)
    } else {
        key.parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(|value| value.round() as i64)
    }
}

fn distinct_count(events: &[NormalizedEvent], field: &str) -> usize {
    let mut seen = HashSet::new();
    for event in events {
        if let Some(value) = event.payload().get(field) {
            seen.insert(value.to_string());
        }
    }
    seen.len()
}

fn normalize_lookup_key(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(|ch| ch.to_lowercase())
        .collect()
}
