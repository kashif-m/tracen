//! Core intermediate representation shared across tracker engine crates.
//!
//! The types in this crate stay domain agnostic and encode the deterministic API surface of the
//! engine: tracker definitions, normalized events, query inputs, and output envelopes.

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

// Error handling modules (Phase 1)
pub mod error;

/// Uniquely identifies a tracker configuration.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TrackerId(String);

impl TrackerId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TrackerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Uniquely identifies an event appended to a tracker.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventId(String);

impl EventId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Timestamp in milliseconds since epoch.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(i64);

impl Timestamp {
    pub fn new(epoch_ms: i64) -> Self {
        Self(epoch_ms)
    }

    pub fn as_millis(&self) -> i64 {
        self.0
    }
}

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Timestamp::new(value)
    }
}

/// Semantic version attached to tracker definitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TrackerVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl TrackerVersion {
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

impl Default for TrackerVersion {
    fn default() -> Self {
        Self::new(1, 0, 0)
    }
}

/// Schema field type supported by tracker payloads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum FieldType {
    Text,
    Float,
    Int,
    Bool,
    Duration,
    Timestamp,
    Enum(Vec<String>),
}

/// Field specification declared in DSL schema.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub name: String,
    pub field_type: FieldType,
    pub optional: bool,
    pub default_value: Option<Value>,
}

/// Scalar and conditional expression model used by derives/metrics/alerts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Number(f64),
    Int(i64),
    Bool(bool),
    Text(String),
    Null,
    Field(String),
    Binary {
        op: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Conditional {
        condition: Box<Condition>,
        then_expr: Box<Expression>,
        else_expr: Box<Expression>,
    },
    Function {
        name: String,
        args: Vec<Expression>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum BinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    True,
    False,
    Comparison {
        op: ComparisonOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOperator {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Derived field declaration.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeriveDefinition {
    pub name: String,
    pub expr: Expression,
}

/// Supported time grains for aggregations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeGrain {
    Day,
    Week,
    Month,
    Quarter,
    Year,
    AllTime,
    Custom,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregationFunc {
    Sum,
    Max,
    Min,
    Avg,
    Count,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum GroupByDimension {
    Field(String),
    Time(TimeGrain),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterOperator {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Aggregation declared for one metric.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregationDefinition {
    pub func: AggregationFunc,
    pub target: Option<Expression>,
    pub group_by: Vec<GroupByDimension>,
    pub over: Option<TimeGrain>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetricDefinition {
    pub name: String,
    pub aggregation: AggregationDefinition,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AlertDefinition {
    pub name: String,
    pub expr: Expression,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlanningStrategyDefinition {
    pub name: String,
    pub params: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct PlanningDefinition {
    pub strategies: Vec<PlanningStrategyDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ViewDefinition {
    pub name: String,
    pub params: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaFieldDefinition {
    pub name: String,
    pub type_ref: String,
    pub optional: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PackTypeKind {
    #[default]
    Object,
    Enum,
    Alias,
}

fn default_emit_ts() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PackTypeDefinition {
    pub name: String,
    #[serde(default)]
    pub kind: PackTypeKind,
    #[serde(default = "default_emit_ts")]
    pub emit_ts: bool,
    #[serde(default)]
    pub emit_rust: bool,
    #[serde(default)]
    pub contract: Option<String>,
    #[serde(default)]
    pub fields: Vec<SchemaFieldDefinition>,
    #[serde(default)]
    pub variants: Vec<String>,
    #[serde(default)]
    pub target: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HelperDefinition {
    pub name: String,
    #[serde(default)]
    pub compat_ts_name: Option<String>,
    #[serde(default)]
    pub compat_native_name: Option<String>,
    #[serde(default)]
    pub fallible: bool,
    #[serde(default)]
    pub params: Vec<SchemaFieldDefinition>,
    pub return_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImportDefinition {
    pub name: String,
    #[serde(default)]
    pub compat_ts_name: Option<String>,
    #[serde(default)]
    pub compat_native_name: Option<String>,
    #[serde(default)]
    pub fallible: bool,
    #[serde(default)]
    pub params: Vec<SchemaFieldDefinition>,
    pub return_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternTsItemDefinition {
    pub name: String,
    pub rust_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExternTsImportDefinition {
    pub module: String,
    #[serde(default)]
    pub items: Vec<ExternTsItemDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ViewCompatDefinition {
    pub view: String,
    #[serde(default)]
    pub metric_alias_type: Option<String>,
    #[serde(default)]
    pub group_by_alias_type: Option<String>,
    #[serde(default)]
    pub pack_query_type: Option<String>,
    #[serde(default)]
    pub point_type: Option<String>,
    #[serde(default)]
    pub totals_type: Option<String>,
    #[serde(default)]
    pub query_filter_field: Option<String>,
    #[serde(default)]
    pub query_filter_type: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct CompatDefinition {
    #[serde(default)]
    pub tracker_id_override: Option<String>,
    #[serde(default)]
    pub ts_dsl_contract: Option<String>,
    #[serde(default)]
    pub ts_api_contract: Option<String>,
    #[serde(default)]
    pub ts_domain_contract: Option<String>,
    #[serde(default)]
    pub analytics_capabilities_type: Option<String>,
    #[serde(default)]
    pub native_exports: BTreeMap<String, String>,
    #[serde(default)]
    pub view_aliases: Vec<ViewCompatDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CatalogEntryDefinition {
    pub name: String,
    #[serde(default)]
    pub base_source: Option<String>,
    #[serde(default)]
    pub compat_base_type: Option<String>,
    #[serde(default)]
    pub compat_overlay_type: Option<String>,
    #[serde(default)]
    pub compat_overlay_source_type: Option<String>,
    #[serde(default)]
    pub validate_helper: Option<String>,
    #[serde(default)]
    pub fields: Vec<SchemaFieldDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FilterDefinition {
    pub key: String,
    pub field: String,
    pub op: FilterOperator,
    pub type_ref: String,
    pub optional: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReadModelDefinition {
    pub name: String,
    #[serde(default)]
    pub query_type: Option<String>,
    #[serde(default)]
    pub response_type: Option<String>,
    #[serde(default)]
    pub config: Value,
    #[serde(default)]
    pub params: Vec<SchemaFieldDefinition>,
    #[serde(default)]
    pub filters: Vec<FilterDefinition>,
    #[serde(default)]
    pub fields: Vec<SchemaFieldDefinition>,
}

/// Result of compiling tracker DSL into validated IR.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TrackerDefinition {
    tracker_id: TrackerId,
    #[serde(default)]
    tracker_id_override: Option<String>,
    tracker_name: String,
    version: TrackerVersion,
    dsl: String,
    fields: Vec<FieldDefinition>,
    derives: Vec<DeriveDefinition>,
    metrics: Vec<MetricDefinition>,
    alerts: Vec<AlertDefinition>,
    planning: Option<PlanningDefinition>,
    #[serde(default)]
    views: Vec<ViewDefinition>,
    #[serde(default)]
    catalog: Vec<CatalogEntryDefinition>,
    #[serde(default)]
    read_models: Vec<ReadModelDefinition>,
    #[serde(default)]
    types: Vec<PackTypeDefinition>,
    #[serde(default)]
    helpers: Vec<HelperDefinition>,
    #[serde(default)]
    imports: Vec<ImportDefinition>,
    #[serde(default)]
    extern_ts: Vec<ExternTsImportDefinition>,
    #[serde(default)]
    compat: Option<CompatDefinition>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TrackerDefinitionInput {
    #[serde(default)]
    pub tracker_id_override: Option<String>,
    pub tracker_name: String,
    pub version: TrackerVersion,
    pub dsl: String,
    pub fields: Vec<FieldDefinition>,
    pub derives: Vec<DeriveDefinition>,
    pub metrics: Vec<MetricDefinition>,
    pub alerts: Vec<AlertDefinition>,
    pub planning: Option<PlanningDefinition>,
    #[serde(default)]
    pub views: Vec<ViewDefinition>,
    #[serde(default)]
    pub catalog: Vec<CatalogEntryDefinition>,
    #[serde(default)]
    pub read_models: Vec<ReadModelDefinition>,
    #[serde(default)]
    pub types: Vec<PackTypeDefinition>,
    #[serde(default)]
    pub helpers: Vec<HelperDefinition>,
    #[serde(default)]
    pub imports: Vec<ImportDefinition>,
    #[serde(default)]
    pub extern_ts: Vec<ExternTsImportDefinition>,
    #[serde(default)]
    pub compat: Option<CompatDefinition>,
}

impl TrackerDefinition {
    pub fn new(input: TrackerDefinitionInput) -> Self {
        let TrackerDefinitionInput {
            tracker_id_override,
            tracker_name,
            version,
            dsl,
            fields,
            derives,
            metrics,
            alerts,
            planning,
            views,
            catalog,
            read_models,
            types,
            helpers,
            imports,
            extern_ts,
            compat,
        } = input;
        let tracker_id =
            build_tracker_id(&tracker_name, version, &dsl, tracker_id_override.as_deref());
        Self {
            tracker_id,
            tracker_id_override,
            tracker_name,
            version,
            dsl,
            fields,
            derives,
            metrics,
            alerts,
            planning,
            views,
            catalog,
            read_models,
            types,
            helpers,
            imports,
            extern_ts,
            compat,
        }
    }

    pub fn tracker_id(&self) -> &TrackerId {
        &self.tracker_id
    }

    pub fn tracker_name(&self) -> &str {
        &self.tracker_name
    }

    pub fn tracker_id_override(&self) -> Option<&str> {
        self.tracker_id_override.as_deref()
    }

    pub fn version(&self) -> TrackerVersion {
        self.version
    }

    pub fn dsl(&self) -> &str {
        &self.dsl
    }

    pub fn fields(&self) -> &[FieldDefinition] {
        &self.fields
    }

    pub fn derives(&self) -> &[DeriveDefinition] {
        &self.derives
    }

    pub fn metrics(&self) -> &[MetricDefinition] {
        &self.metrics
    }

    pub fn alerts(&self) -> &[AlertDefinition] {
        &self.alerts
    }

    pub fn planning(&self) -> Option<&PlanningDefinition> {
        self.planning.as_ref()
    }

    pub fn views(&self) -> &[ViewDefinition] {
        &self.views
    }

    pub fn catalog(&self) -> &[CatalogEntryDefinition] {
        &self.catalog
    }

    pub fn read_models(&self) -> &[ReadModelDefinition] {
        &self.read_models
    }

    pub fn types(&self) -> &[PackTypeDefinition] {
        &self.types
    }

    pub fn helpers(&self) -> &[HelperDefinition] {
        &self.helpers
    }

    pub fn imports(&self) -> &[ImportDefinition] {
        &self.imports
    }

    pub fn extern_ts(&self) -> &[ExternTsImportDefinition] {
        &self.extern_ts
    }

    pub fn compat(&self) -> Option<&CompatDefinition> {
        self.compat.as_ref()
    }
}

fn build_tracker_id(
    name: &str,
    version: TrackerVersion,
    dsl: &str,
    tracker_id_override: Option<&str>,
) -> TrackerId {
    if let Some(value) = tracker_id_override {
        return TrackerId::new(value.to_string());
    }
    let hash = blake3::hash(dsl.as_bytes()).to_hex();
    let normalized = name
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>()
        .to_lowercase();
    TrackerId::new(format!("{}_v{}_{}", normalized, version.major, &hash[..8]))
}

/// Normalized event shape consumed by the engine.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NormalizedEvent {
    event_id: EventId,
    tracker_id: TrackerId,
    ts: Timestamp,
    payload: Value,
    meta: Value,
}

impl NormalizedEvent {
    pub fn new(
        event_id: EventId,
        tracker_id: TrackerId,
        ts: Timestamp,
        payload: Value,
        meta: Value,
    ) -> Self {
        Self {
            event_id,
            tracker_id,
            ts,
            payload,
            meta,
        }
    }

    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }

    pub fn tracker_id(&self) -> &TrackerId {
        &self.tracker_id
    }

    pub fn ts(&self) -> Timestamp {
        self.ts
    }

    pub fn payload(&self) -> &Value {
        &self.payload
    }

    pub fn payload_mut(&mut self) -> &mut Value {
        &mut self.payload
    }

    pub fn meta(&self) -> &Value {
        &self.meta
    }
}

/// Time window filter applied during compute/simulate queries.
#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct TimeWindow {
    pub start: Timestamp,
    pub end: Timestamp,
}

impl TimeWindow {
    pub fn contains(&self, ts: Timestamp) -> bool {
        ts >= self.start && ts <= self.end
    }
}

/// Query input for compute/simulate.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Query {
    pub time_window: Option<TimeWindow>,
    pub grains: Vec<TimeGrain>,
}

/// Mutable state container for incremental engine application.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EngineState {
    tracker_id: TrackerId,
    events: Vec<NormalizedEvent>,
}

impl EngineState {
    pub fn new(tracker_id: TrackerId) -> Self {
        Self {
            tracker_id,
            events: Vec::new(),
        }
    }

    pub fn for_definition(def: &TrackerDefinition) -> Self {
        Self::new(def.tracker_id().clone())
    }

    pub fn tracker_id(&self) -> &TrackerId {
        &self.tracker_id
    }

    pub fn push(&mut self, event: NormalizedEvent) {
        self.events.push(event);
    }

    pub fn total_events(&self) -> usize {
        self.events.len()
    }

    pub fn events(&self) -> &[NormalizedEvent] {
        &self.events
    }
}

/// Engine output returned by stateless compute.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct EngineOutput {
    pub total_events: usize,
    pub window_events: usize,
    pub metrics: BTreeMap<String, Value>,
    pub alerts: Vec<Value>,
}

/// Delta emitted by incremental apply/simulate.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct EngineOutputDelta {
    pub total_events_delta: isize,
    pub metrics: BTreeMap<String, Value>,
}

/// Output returned by planning simulations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SimulationOutput {
    pub base: EngineOutput,
    pub hypothetical: EngineOutput,
    pub delta: EngineOutputDelta,
}

/// Helper for building deterministic metric maps.
pub fn empty_object() -> Value {
    Value::Object(Map::new())
}

/// Utility to compute delta metrics between two maps.
pub fn metric_delta(
    base: &BTreeMap<String, Value>,
    hypothetical: &BTreeMap<String, Value>,
) -> BTreeMap<String, Value> {
    let mut keys = BTreeSet::new();
    keys.extend(base.keys().cloned());
    keys.extend(hypothetical.keys().cloned());

    let mut delta = BTreeMap::new();
    for key in keys {
        match (base.get(&key), hypothetical.get(&key)) {
            (Some(Value::Number(lhs)), Some(Value::Number(rhs))) => {
                if let (Some(lhs), Some(rhs)) = (lhs.as_f64(), rhs.as_f64()) {
                    delta.insert(key, json!(rhs - lhs));
                }
            }
            (_, Some(value)) => {
                delta.insert(key, value.clone());
            }
            _ => {}
        }
    }
    delta
}
