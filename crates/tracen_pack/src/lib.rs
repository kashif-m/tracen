//! Build-time pack integration and generic runtime execution boundaries.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tracen_analytics::Distribution;
use tracen_engine::{self, EngineError, MetricComputeOptions, MetricFilter, MetricFilterOp};
use tracen_ir::{EventId, GroupByDimension, NormalizedEvent, Timestamp, TrackerDefinition};

#[derive(Debug, Error)]
pub enum PackError {
    #[error("dsl compile failed: {0}")]
    Compile(String),
    #[error("build io failed: {0}")]
    Io(String),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("event preparation failed: {0}")]
    Event(String),
    #[error("adapter failed: {0}")]
    Adapter(String),
}

#[derive(Debug, Clone)]
pub struct CompiledPack {
    definition: TrackerDefinition,
    capabilities: Value,
}

impl CompiledPack {
    pub fn compile(dsl: &str) -> Result<Self, PackError> {
        let definition = tracen_dsl::compile(dsl).map_err(|err| PackError::Compile(err.message))?;
        Self::from_definition(definition)
    }

    pub fn from_definition(definition: TrackerDefinition) -> Result<Self, PackError> {
        let model = tracen_pack_codegen::PackGenModel::from_tracker(&definition)
            .map_err(PackError::Compile)?;
        let capabilities = serde_json::from_str(&model.capabilities_json)
            .map_err(|err| PackError::Compile(format!("deserialize capabilities: {err}")))?;
        Ok(Self::from_precomputed(definition, capabilities))
    }

    pub fn from_precomputed(definition: TrackerDefinition, capabilities: Value) -> Self {
        Self {
            definition,
            capabilities,
        }
    }

    pub fn definition(&self) -> &TrackerDefinition {
        &self.definition
    }

    pub fn capabilities(&self) -> &Value {
        &self.capabilities
    }
}

#[derive(Debug, Clone)]
pub struct PackRuntime<A> {
    compiled: Arc<CompiledPack>,
    adapter: A,
    options: PackRuntimeOptions,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PackRuntimeOptions {
    pub use_legacy_adapter_for_queries: bool,
}

impl<A> PackRuntime<A>
where
    A: PackExecutionAdapter,
{
    pub fn new(compiled: CompiledPack, adapter: A) -> Self {
        Self::new_with_options(compiled, adapter, PackRuntimeOptions::default())
    }

    pub fn new_shared(compiled: Arc<CompiledPack>, adapter: A) -> Self {
        Self::new_shared_with_options(compiled, adapter, PackRuntimeOptions::default())
    }

    pub fn new_with_options(
        compiled: CompiledPack,
        adapter: A,
        options: PackRuntimeOptions,
    ) -> Self {
        Self::new_shared_with_options(Arc::new(compiled), adapter, options)
    }

    pub fn new_shared_with_options(
        compiled: Arc<CompiledPack>,
        adapter: A,
        options: PackRuntimeOptions,
    ) -> Self {
        Self {
            compiled,
            adapter,
            options,
        }
    }

    pub fn compiled(&self) -> &CompiledPack {
        self.compiled.as_ref()
    }

    pub fn pack_capabilities(&self) -> Value {
        self.compiled.capabilities().clone()
    }

    pub fn pack_base_catalog(&self) -> Result<Value, PackError> {
        self.adapter.base_catalog().map_err(PackError::Adapter)
    }

    pub fn validate_pack_event(&self, event_json: &str) -> Result<Value, PackError> {
        let normalized = tracen_engine::validate_event(self.compiled.definition(), event_json)
            .map_err(to_pack_event_error)?;
        serde_json::to_value(normalized).map_err(|err| PackError::Event(err.to_string()))
    }

    pub fn validate_pack_catalog_entry(
        &self,
        entry_type: &str,
        entry_json: &str,
    ) -> Result<Value, PackError> {
        self.adapter
            .validate_catalog_entry(entry_type, entry_json)
            .map_err(PackError::Adapter)
    }

    pub fn parse_query_json(&self, query_json: &str) -> Result<PackExecutionPlan, PackError> {
        parse_query_json(self.compiled.definition(), query_json)
    }

    pub fn prepare_events_json(&self, events_json: &str) -> Result<Vec<PackInputEvent>, PackError> {
        let events: Vec<PackInputEvent> = serde_json::from_str(events_json)
            .map_err(|err| PackError::Event(format!("parse pack events: {err}")))?;
        prepare_pack_events(self.compiled.definition(), &events)
    }

    pub fn pack_query(
        &self,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &Value,
        query_json: &str,
    ) -> Result<Value, PackError> {
        let plan = self.parse_query_json(query_json)?;
        if self.options.use_legacy_adapter_for_queries {
            return self
                .adapter
                .execute(
                    self.compiled.definition(),
                    events,
                    offset_minutes,
                    catalog_json,
                    &plan,
                )
                .map_err(PackError::Adapter);
        }
        match &plan {
            PackExecutionPlan::View(view) => execute_view_query(
                self.compiled.definition(),
                events,
                offset_minutes,
                catalog_json,
                view,
            ),
            PackExecutionPlan::ReadModel(read_model) => self
                .adapter
                .execute_read_model(
                    self.compiled.definition(),
                    events,
                    offset_minutes,
                    catalog_json,
                    read_model,
                )
                .map_err(PackError::Adapter),
        }
    }
}

pub trait PackExecutionAdapter {
    fn base_catalog(&self) -> Result<Value, String>;
    fn validate_catalog_entry(&self, entry_type: &str, entry_json: &str) -> Result<Value, String>;
    fn execute(
        &self,
        definition: &TrackerDefinition,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &Value,
        plan: &PackExecutionPlan,
    ) -> Result<Value, String>;
    fn execute_read_model(
        &self,
        definition: &TrackerDefinition,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &Value,
        query: &ReadModelQueryPlan,
    ) -> Result<Value, String>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackInputEvent {
    pub ts: i64,
    pub payload: Value,
}

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

#[derive(Debug, Clone)]
pub struct PackBuildConfig {
    pub dsl_path: PathBuf,
    pub out_dir: PathBuf,
    pub generated_ts_dir: PathBuf,
    pub base_source_paths: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone)]
pub struct PackBuildOutput {
    pub compiled: CompiledPack,
    pub rust_artifact_path: PathBuf,
    pub rust_ffi_glue_path: PathBuf,
    pub dsl_contract_path: PathBuf,
    pub api_contract_path: PathBuf,
    pub domain_contract_path: PathBuf,
    pub compat_api_contract_path: PathBuf,
    pub compat_domain_contract_path: PathBuf,
}

pub fn build(config: &PackBuildConfig) -> Result<PackBuildOutput, PackError> {
    let dsl = fs::read_to_string(&config.dsl_path)
        .map_err(|err| PackError::Io(format!("read {}: {err}", config.dsl_path.display())))?;
    let compiled = CompiledPack::compile(&dsl)?;
    let mut base_source_payloads = BTreeMap::new();
    for (name, path) in &config.base_source_paths {
        let payload = fs::read_to_string(path)
            .map_err(|err| PackError::Io(format!("read {}: {err}", path.display())))?;
        base_source_payloads.insert(name.clone(), payload);
    }
    let model = tracen_pack_codegen::PackGenModel::from_tracker_with_base_sources(
        compiled.definition(),
        &base_source_payloads,
    )
    .map_err(PackError::Compile)?;
    let generator = tracen_pack_codegen::with_builtin_templates()
        .map_err(|err| PackError::Compile(err.to_string()))?;
    let artifacts = generator
        .generate_all_from_model(&model)
        .map_err(|err| PackError::Compile(err.to_string()))?;

    fs::create_dir_all(&config.out_dir)
        .map_err(|err| PackError::Io(format!("create {}: {err}", config.out_dir.display())))?;
    fs::create_dir_all(&config.generated_ts_dir).map_err(|err| {
        PackError::Io(format!(
            "create {}: {err}",
            config.generated_ts_dir.display()
        ))
    })?;

    let rust_artifact_path = config
        .out_dir
        .join(format!("{}_tracker_compiled.rs", model.tracker_fn));
    let rust_ffi_glue_path = config
        .out_dir
        .join(format!("{}_tracker_ffi.rs", model.tracker_fn));
    let dsl_contract_path = config
        .generated_ts_dir
        .join(format!("{}DslContract.ts", model.tracker_fn));
    let api_contract_path = config
        .generated_ts_dir
        .join(format!("{}PackCoreApiContract.ts", model.tracker_fn));
    let domain_contract_path = config
        .generated_ts_dir
        .join(format!("{}PackCoreDomainContract.ts", model.tracker_fn));
    let compat_api_contract_path = config
        .generated_ts_dir
        .join(&model.compat_api_contract_file);
    let compat_domain_contract_path = config
        .generated_ts_dir
        .join(&model.compat_domain_contract_file);

    fs::write(&rust_artifact_path, artifacts.rust_pack_runtime)
        .map_err(|err| PackError::Io(format!("write {}: {err}", rust_artifact_path.display())))?;
    fs::write(&rust_ffi_glue_path, artifacts.rust_ffi_glue)
        .map_err(|err| PackError::Io(format!("write {}: {err}", rust_ffi_glue_path.display())))?;
    fs::write(&dsl_contract_path, artifacts.ts_dsl_contract)
        .map_err(|err| PackError::Io(format!("write {}: {err}", dsl_contract_path.display())))?;
    fs::write(&api_contract_path, artifacts.ts_api_contract)
        .map_err(|err| PackError::Io(format!("write {}: {err}", api_contract_path.display())))?;
    fs::write(&domain_contract_path, artifacts.ts_domain_contract)
        .map_err(|err| PackError::Io(format!("write {}: {err}", domain_contract_path.display())))?;
    fs::write(&compat_api_contract_path, artifacts.ts_compat_api_contract).map_err(|err| {
        PackError::Io(format!(
            "write {}: {err}",
            compat_api_contract_path.display()
        ))
    })?;
    fs::write(
        &compat_domain_contract_path,
        artifacts.ts_compat_domain_contract,
    )
    .map_err(|err| {
        PackError::Io(format!(
            "write {}: {err}",
            compat_domain_contract_path.display()
        ))
    })?;

    Ok(PackBuildOutput {
        compiled,
        rust_artifact_path,
        rust_ffi_glue_path,
        dsl_contract_path,
        api_contract_path,
        domain_contract_path,
        compat_api_contract_path,
        compat_domain_contract_path,
    })
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
struct RuntimeViewConfig {
    #[serde(default)]
    result_kind: Option<String>,
    #[serde(default)]
    count_metric: Option<String>,
    #[serde(default)]
    metrics: BTreeMap<String, RuntimeMetricConfig>,
    #[serde(default)]
    group_by: BTreeMap<String, RuntimeGroupByConfig>,
    #[serde(default)]
    filters: BTreeMap<String, RuntimeFilterConfig>,
    #[serde(default)]
    response_fields: BTreeMap<String, RuntimeResponseFieldConfig>,
    #[serde(default)]
    totals: BTreeMap<String, RuntimeTotalFieldConfig>,
    #[serde(default)]
    qa: BTreeMap<String, RuntimeQaFieldConfig>,
    #[serde(default)]
    enrich_fields: BTreeMap<String, RuntimeEnrichFieldConfig>,
}

#[derive(Debug, Deserialize)]
struct RuntimeMetricConfig {
    metric: String,
}

#[derive(Debug, Deserialize)]
struct RuntimeGroupByConfig {
    field: String,
}

#[derive(Debug, Deserialize)]
struct RuntimeFilterConfig {
    field: String,
    #[serde(default = "default_filter_op")]
    op: String,
    #[serde(rename = "type")]
    type_ref: String,
    #[serde(default)]
    optional: bool,
    #[serde(default)]
    metrics: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeResponseFieldConfig {
    from_filter: String,
}

#[derive(Debug, Deserialize)]
struct RuntimeTotalFieldConfig {
    kind: String,
    #[serde(default)]
    metric: Option<String>,
    #[serde(default)]
    field: Option<String>,
    #[serde(default)]
    coerce: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeQaFieldConfig {
    kind: String,
    event_field: String,
    #[serde(default)]
    lookup_fields: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeEnrichFieldConfig {
    lookup_field: String,
    #[serde(default)]
    lookup_fields: Vec<String>,
    catalog_field: String,
}

fn parse_query_json(
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

fn prepare_pack_events(
    definition: &TrackerDefinition,
    events: &[PackInputEvent],
) -> Result<Vec<PackInputEvent>, PackError> {
    events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let mut normalized = tracen_ir::NormalizedEvent::new(
                tracen_ir::EventId::new(format!("pack-{index}-{}", event.ts)),
                definition.tracker_id().clone(),
                tracen_ir::Timestamp::new(event.ts),
                event.payload.clone(),
                serde_json::json!({}),
            );
            tracen_engine::derive_event(definition, &mut normalized)
                .map_err(to_pack_event_error)?;
            Ok(PackInputEvent {
                ts: event.ts,
                payload: normalized.payload().clone(),
            })
        })
        .collect()
}

fn to_pack_event_error(error: EngineError) -> PackError {
    PackError::Event(error.to_string())
}

fn execute_view_query(
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

#[cfg(test)]
mod tests {
    use super::{
        build, CompiledPack, PackBuildConfig, PackInputEvent, PackRuntime, PackRuntimeOptions,
    };
    use serde_json::{json, Value};
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::tempdir;

    struct StubAdapter;

    impl super::PackExecutionAdapter for StubAdapter {
        fn base_catalog(&self) -> Result<serde_json::Value, String> {
            Ok(json!([{ "slug": "base" }]))
        }

        fn validate_catalog_entry(
            &self,
            entry_type: &str,
            entry_json: &str,
        ) -> Result<serde_json::Value, String> {
            Ok(
                json!({ "entry_type": entry_type, "entry": serde_json::from_str::<serde_json::Value>(entry_json).unwrap() }),
            )
        }

        fn execute(
            &self,
            _definition: &tracen_ir::TrackerDefinition,
            events: &[PackInputEvent],
            offset_minutes: i32,
            catalog_json: &serde_json::Value,
            plan: &super::PackExecutionPlan,
        ) -> Result<serde_json::Value, String> {
            Ok(json!({
                "events": events.len(),
                "offset_minutes": offset_minutes,
                "catalog": catalog_json,
                "plan": plan,
            }))
        }

        fn execute_read_model(
            &self,
            _definition: &tracen_ir::TrackerDefinition,
            _events: &[PackInputEvent],
            _offset_minutes: i32,
            _catalog_json: &serde_json::Value,
            query: &super::ReadModelQueryPlan,
        ) -> Result<serde_json::Value, String> {
            Ok(json!({
                "read_model": query.read_model_name,
                "params": query.params,
            }))
        }
    }

    fn sample_dsl() -> &'static str {
        r#"
tracker "sample_pack" v1 {
  fields {
    category: text optional
    bucket: int optional
    score: int optional
  }
  metrics {
    total_sets = count() over all_time
  }
  views {
    view "metric_series" {
      config = {"query_type":"MetricSeriesQuery","response_type":"MetricSeriesResponse","result_kind":"metric_series","count_metric":"total_sets","group_by":{"bucket":{"field":"bucket"}},"filters":{"category":{"field":"category","op":"eq","type":"string","optional":true}},"metrics":{"total_sets":{"metric":"total_sets","label":"Sets"}}}
    }
    view "category_dist" {
      config = {"query_type":"CategoryDistQuery","response_type":"CategoryDistResponse","result_kind":"distribution","group_by":{"category":{"field":"category"}},"metrics":{"total_sets":{"metric":"total_sets","label":"Sets"}}}
    }
  }
  catalog {
    entry "thing" {
      fields = {"slug":{"type":"string"}}
    }
  }
  read_models {
    read_model "daily_rollup" {
      query_type = "DailyRollupQuery"
      response_type = "DailyRollupResponse"
      params = {"bucket":{"type":"number"}}
      fields = {"total":{"type":"number"}}
    }
  }
}
"#
    }

    fn filtered_scope_dsl() -> &'static str {
        r#"
tracker "filtered_scope_pack" v1 {
  fields {
    segment: text optional
    day_bucket: int optional
    week_bucket: int optional
    month_bucket: int optional
    amount: float optional
    units: int optional
  }
  derive {
    derived_total = if (amount > 0 && units > 0) then amount * units else 0
  }
  metrics {
    total_records = count() over all_time
    total_derived = sum(derived_total) over all_time
    max_derived = max(derived_total) over all_time
  }
  views {
    view "series" {
      config = {"query_type":"SeriesQuery","response_type":"SeriesResponse","result_kind":"metric_series","count_metric":"total_records","group_by":{"bucket":{"field":"day_bucket"}},"filters":{"segment":{"field":"segment","op":"eq","type":"string","optional":true}},"metrics":{"total_derived":{"metric":"total_derived","label":"Total"}}}
    }
    view "scoped_series" {
      config = {"query_type":"ScopedSeriesQuery","response_type":"ScopedSeriesResponse","result_kind":"metric_series","count_metric":"total_records","group_by":{"bucket":{"field":"day_bucket"}},"filters":{"segment":{"field":"segment","op":"eq","type":"string"}},"response_fields":{"segment":{"from_filter":"segment"}},"metrics":{"total_derived":{"metric":"total_derived","label":"Total"},"max_derived":{"metric":"max_derived","label":"Max"}}}
    }
  }
}
"#
    }

    #[test]
    fn build_generates_core_artifacts_for_fixture_integration() {
        let temp = tempdir().expect("tempdir");
        let dsl_path = temp.path().join("sample_pack.tracker");
        let out_dir = temp.path().join("out");
        let generated_ts_dir = temp.path().join("generated");
        fs::write(&dsl_path, sample_dsl()).expect("write dsl");

        let output = build(&PackBuildConfig {
            dsl_path,
            out_dir,
            generated_ts_dir,
            base_source_paths: BTreeMap::new(),
        })
        .expect("build pack");

        assert!(output.rust_artifact_path.exists());
        assert!(output.dsl_contract_path.exists());
        assert!(output.api_contract_path.exists());
        assert!(output.domain_contract_path.exists());
    }

    #[test]
    fn runtime_validates_queries_against_declared_views_and_read_models() {
        let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
        let runtime = PackRuntime::new(compiled, StubAdapter);

        let view_plan = runtime
            .parse_query_json(r#"{"view":"metric_series","metric":"total_sets","group_by":"bucket","category":"focus"}"#)
            .expect("parse view query");
        assert!(matches!(view_plan, super::PackExecutionPlan::View(_)));

        let read_model_plan = runtime
            .parse_query_json(r#"{"read_model":"daily_rollup","bucket":1}"#)
            .expect("parse read model query");
        assert!(matches!(
            read_model_plan,
            super::PackExecutionPlan::ReadModel(_)
        ));

        let err = runtime
            .parse_query_json(r#"{"view":"metric_series","metric":"unknown","group_by":"bucket"}"#)
            .expect_err("unknown metric should fail");
        assert!(err.to_string().contains("metric"));

        let err = runtime
            .parse_query_json(r#"{"view":"metric_series","metric":"total_sets","group_by":"bucket","extra":"nope"}"#)
            .expect_err("undeclared filter should fail");
        assert!(err.to_string().contains("filter"));
    }

    #[derive(Debug, Default)]
    struct PanicAdapter;

    impl super::PackExecutionAdapter for PanicAdapter {
        fn base_catalog(&self) -> Result<serde_json::Value, String> {
            Ok(serde_json::json!([]))
        }

        fn validate_catalog_entry(
            &self,
            _entry_type: &str,
            _entry_json: &str,
        ) -> Result<serde_json::Value, String> {
            Ok(serde_json::json!({}))
        }

        fn execute(
            &self,
            _definition: &tracen_ir::TrackerDefinition,
            _events: &[PackInputEvent],
            _offset_minutes: i32,
            _catalog_json: &serde_json::Value,
            _plan: &super::PackExecutionPlan,
        ) -> Result<serde_json::Value, String> {
            panic!("view execution should not delegate to the adapter");
        }

        fn execute_read_model(
            &self,
            _definition: &tracen_ir::TrackerDefinition,
            _events: &[PackInputEvent],
            _offset_minutes: i32,
            _catalog_json: &serde_json::Value,
            _query: &super::ReadModelQueryPlan,
        ) -> Result<serde_json::Value, String> {
            panic!("read-model execution should not delegate to this adapter in generic tests");
        }
    }

    #[derive(Debug, Default)]
    struct LegacyMirrorAdapter;

    impl super::PackExecutionAdapter for LegacyMirrorAdapter {
        fn base_catalog(&self) -> Result<serde_json::Value, String> {
            Ok(serde_json::json!([]))
        }

        fn validate_catalog_entry(
            &self,
            _entry_type: &str,
            _entry_json: &str,
        ) -> Result<serde_json::Value, String> {
            Ok(serde_json::json!({}))
        }

        fn execute(
            &self,
            definition: &tracen_ir::TrackerDefinition,
            events: &[PackInputEvent],
            offset_minutes: i32,
            catalog_json: &serde_json::Value,
            plan: &super::PackExecutionPlan,
        ) -> Result<serde_json::Value, String> {
            match plan {
                super::PackExecutionPlan::View(view) => super::execute_view_query(
                    definition,
                    events,
                    offset_minutes,
                    catalog_json,
                    view,
                )
                .map_err(|err| err.to_string()),
                super::PackExecutionPlan::ReadModel(_) => {
                    Err("legacy adapter path no longer owns read-model execution".to_string())
                }
            }
        }

        fn execute_read_model(
            &self,
            _definition: &tracen_ir::TrackerDefinition,
            _events: &[PackInputEvent],
            _offset_minutes: i32,
            _catalog_json: &serde_json::Value,
            _query: &super::ReadModelQueryPlan,
        ) -> Result<serde_json::Value, String> {
            Err("legacy adapter path no longer owns read-model execution".to_string())
        }
    }

    #[test]
    fn runtime_executes_new_view_without_adapter_support() {
        let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
        let runtime = PackRuntime::new(compiled, PanicAdapter);
        let events = runtime
            .prepare_events_json(
                r#"[{"ts":1,"payload":{"category":"a","bucket":1}},{"ts":2,"payload":{"category":"b","bucket":1}},{"ts":3,"payload":{"category":"a","bucket":2}}]"#,
            )
            .expect("prepare events");
        let result = runtime
            .pack_query(
                &events,
                0,
                &serde_json::json!([]),
                r#"{"view":"category_dist","metric":"total_sets","group_by":"category"}"#,
            )
            .expect("execute view");

        let items = result
            .get("items")
            .and_then(Value::as_array)
            .expect("items");
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn runtime_applies_optional_filters_during_native_view_execution() {
        let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
        let runtime = PackRuntime::new(compiled, PanicAdapter);
        let events = runtime
            .prepare_events_json(
                r#"[{"ts":1,"payload":{"category":"a","bucket":1}},{"ts":2,"payload":{"category":"b","bucket":1}},{"ts":3,"payload":{"category":"a","bucket":2}}]"#,
            )
            .expect("prepare events");

        let filtered = runtime
            .pack_query(
                &events,
                0,
                &serde_json::json!([]),
                r#"{"view":"metric_series","metric":"total_sets","group_by":"bucket","category":"a"}"#,
            )
            .expect("execute filtered view");
        let points = filtered
            .get("points")
            .and_then(Value::as_array)
            .expect("points");
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].get("count").and_then(Value::as_i64), Some(1));
        assert_eq!(points[1].get("count").and_then(Value::as_i64), Some(1));
    }

    #[test]
    fn runtime_scopes_metrics_to_selected_filter() {
        let compiled = CompiledPack::compile(filtered_scope_dsl()).expect("compile pack");
        let runtime = PackRuntime::new(compiled, PanicAdapter);
        let events = runtime
            .prepare_events_json(
                r#"[{"ts":1,"payload":{"segment":"alpha","day_bucket":1000,"amount":100,"units":5}},{"ts":2,"payload":{"segment":"alpha","day_bucket":1000,"amount":80,"units":8}},{"ts":3,"payload":{"segment":"beta","day_bucket":1000,"amount":150,"units":5}},{"ts":4,"payload":{"segment":"alpha","day_bucket":2000,"amount":90,"units":5}},{"ts":5,"payload":{"segment":"beta","day_bucket":2000,"amount":160,"units":5}}]"#,
            )
            .expect("prepare events");

        let series_filtered = runtime
            .pack_query(
                &events,
                0,
                &serde_json::json!([]),
                r#"{"view":"series","metric":"total_derived","group_by":"bucket","segment":"alpha"}"#,
            )
            .expect("execute series query");
        let series_points = series_filtered
            .get("points")
            .and_then(Value::as_array)
            .expect("series points");
        assert_eq!(series_points.len(), 2);
        assert_eq!(
            series_points[0].get("bucket").and_then(Value::as_i64),
            Some(1000)
        );
        assert_eq!(
            series_points[0].get("value").and_then(Value::as_f64),
            Some(1140.0)
        );
        assert_eq!(
            series_points[0].get("count").and_then(Value::as_i64),
            Some(2)
        );
        assert_eq!(
            series_points[1].get("bucket").and_then(Value::as_i64),
            Some(2000)
        );
        assert_eq!(
            series_points[1].get("value").and_then(Value::as_f64),
            Some(450.0)
        );
        assert_eq!(
            series_points[1].get("count").and_then(Value::as_i64),
            Some(1)
        );

        let scoped_total = runtime
            .pack_query(
                &events,
                0,
                &serde_json::json!([]),
                r#"{"view":"scoped_series","metric":"total_derived","group_by":"bucket","segment":"alpha"}"#,
            )
            .expect("execute scoped total query");
        let total_points = scoped_total
            .get("points")
            .and_then(Value::as_array)
            .expect("total points");
        assert_eq!(
            total_points[0].get("value").and_then(Value::as_f64),
            Some(1140.0)
        );
        assert_eq!(
            total_points[0].get("count").and_then(Value::as_i64),
            Some(2)
        );
        assert_eq!(
            total_points[1].get("value").and_then(Value::as_f64),
            Some(450.0)
        );
        assert_eq!(
            total_points[1].get("count").and_then(Value::as_i64),
            Some(1)
        );

        let scoped_max = runtime
            .pack_query(
                &events,
                0,
                &serde_json::json!([]),
                r#"{"view":"scoped_series","metric":"max_derived","group_by":"bucket","segment":"alpha"}"#,
            )
            .expect("execute scoped max query");
        let max_points = scoped_max
            .get("points")
            .and_then(Value::as_array)
            .expect("max points");
        assert_eq!(
            max_points[0].get("value").and_then(Value::as_f64),
            Some(640.0)
        );
        assert_eq!(max_points[0].get("count").and_then(Value::as_i64), Some(2));
        assert_eq!(
            max_points[1].get("value").and_then(Value::as_f64),
            Some(450.0)
        );
        assert_eq!(max_points[1].get("count").and_then(Value::as_i64), Some(1));
    }

    #[test]
    fn runtime_legacy_fallback_matches_native_for_views() {
        let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
        let runtime_native = PackRuntime::new(compiled.clone(), PanicAdapter);
        let runtime_legacy = PackRuntime::new_with_options(
            compiled,
            LegacyMirrorAdapter,
            PackRuntimeOptions {
                use_legacy_adapter_for_queries: true,
            },
        );
        let events = runtime_native
            .prepare_events_json(
                r#"[{"ts":1,"payload":{"category":"a","bucket":1}},{"ts":2,"payload":{"category":"b","bucket":1}}]"#,
            )
            .expect("prepare events");
        let query = r#"{"view":"category_dist","metric":"total_sets","group_by":"category"}"#;
        let mut native_result = runtime_native
            .pack_query(&events, 0, &serde_json::json!([]), query)
            .expect("execute native view");
        let mut legacy_result = runtime_legacy
            .pack_query(&events, 0, &serde_json::json!([]), query)
            .expect("execute legacy view");
        for result in [&mut native_result, &mut legacy_result] {
            if let Some(items) = result.get_mut("items").and_then(Value::as_array_mut) {
                items.sort_by(|lhs, rhs| {
                    let left = lhs.get("label").and_then(Value::as_str).unwrap_or_default();
                    let right = rhs.get("label").and_then(Value::as_str).unwrap_or_default();
                    left.cmp(right)
                });
            }
        }

        assert_eq!(legacy_result, native_result);
    }

    #[test]
    fn runtime_delegates_read_models_through_adapter_contract() {
        let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
        let runtime = PackRuntime::new(compiled, StubAdapter);
        let result = runtime
            .pack_query(
                &[],
                0,
                &serde_json::json!([]),
                r#"{"read_model":"daily_rollup","bucket":42}"#,
            )
            .expect("execute read model");

        assert_eq!(
            result.get("read_model").and_then(Value::as_str),
            Some("daily_rollup")
        );
        assert_eq!(
            result
                .get("params")
                .and_then(Value::as_object)
                .and_then(|params| params.get("bucket"))
                .and_then(Value::as_i64),
            Some(42)
        );
    }
}
