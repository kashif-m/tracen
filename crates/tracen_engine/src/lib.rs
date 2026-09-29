//! Deterministic tracker engine public API surface.
//!
//! The goal is to expose pure functions that can be called from native or JS runtimes through FFI.

mod event_intake;
mod state;

pub use state::{restore_state, EngineState};

use event_intake::{build_pack_event, parse_event_from_json};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use thiserror::Error;
use tracen_eval::{
    evaluate_metrics, AggregationFunc as EvalAggregationFunc, AggregationSpec, ConditionExpr,
    EvalError, FieldPath, GroupExpr, MetricName, MetricSpec, QueryConstraints, ScalarExpr,
};
use tracen_ir::{
    metric_delta, schema_validation::PayloadValidationPolicy, AlertDefinition, BinaryOperator,
    ComparisonOperator, Condition, EngineOutput, EngineOutputDelta, EventId, Expression,
    GroupByDimension, MetricDefinition, NormalizedEvent, Query, SimulationOutput, TimeGrain,
    TimeWindow, Timestamp, TrackerDefinition, TrackerId,
};

/// Engine-level error codes surfaced across FFI boundaries.
#[derive(Debug, Error)]
pub enum EngineError {
    #[error("DSL parse error: {0}")]
    DslParse(String),
    #[error("event validation error: {0}")]
    EventValidation(String),
    #[error("tracker mismatch (expected {expected}, found {actual})")]
    TrackerMismatch {
        expected: TrackerId,
        actual: TrackerId,
    },
    #[error("state tracker mismatch (expected {expected}, found {actual})")]
    StateMismatch {
        expected: TrackerId,
        actual: TrackerId,
    },
    #[error("evaluation error: {0}")]
    Evaluation(String),
}

/// Supported comparison operators for metric filters passed to [`compute_metric_by_name`].
#[derive(Clone, Debug)]
pub enum MetricFilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// A runtime filter applied when computing a metric by name.
#[derive(Clone, Debug)]
pub struct MetricFilter {
    /// Field name inside event payload/root scope.
    pub field: String,
    /// Comparison operator to apply.
    pub op: MetricFilterOp,
    /// Filter value.
    pub value: Value,
}

/// Optional overrides for metric-by-name execution.
#[derive(Clone, Debug, Default)]
pub struct MetricComputeOptions {
    /// Grouping override. If omitted, metric's DSL grouping is used.
    pub group_by: Option<Vec<GroupByDimension>>,
    /// Time window override.
    pub time_window: Option<TimeWindow>,
    /// Additional runtime filters.
    pub filters: Vec<MetricFilter>,
}

/// Compiles DSL text into a deterministic tracker definition.
pub fn compile_tracker(dsl: &str) -> Result<TrackerDefinition, EngineError> {
    if dsl.trim().is_empty() {
        Err(EngineError::DslParse("DSL cannot be empty".into()))?;
    }
    tracen_dsl::compile(dsl).map_err(|err| EngineError::DslParse(err.message))
}

/// Validates and normalizes event JSON against the tracker definition.
pub fn validate_event(
    def: &TrackerDefinition,
    event_json: &str,
) -> Result<NormalizedEvent, EngineError> {
    parse_event_from_json(def, event_json, PayloadValidationPolicy::Event)
}

/// Compiles compute-time metric plan and validates definitions once.
pub fn compile_compute_plan(def: &TrackerDefinition) -> Result<ComputePlan<'_>, EngineError> {
    Ok(ComputePlan {
        definition: def,
        metric_specs: compile_metric_specs(def.metrics())?,
    })
}

#[derive(Clone, Debug)]
pub struct ComputePlan<'a> {
    definition: &'a TrackerDefinition,
    metric_specs: Vec<MetricSpec>,
}

/// Stateless compute over the provided event slice.
pub fn compute(
    def: &TrackerDefinition,
    events: &[NormalizedEvent],
    query: Query,
) -> Result<EngineOutput, EngineError> {
    let prepared = prepare_events_for_compute(def, events)?;
    let plan = compile_compute_plan(def)?;
    compute_with_plan(def, &plan, &prepared, query)
}

/// Precompute derived fields for one-pass compute paths.
///
/// High-throughput pipeline:
/// 1) parse/compile tracker
/// 2) `prepare_events_for_compute` once per immutable batch
/// 3) call `compute_with_prepared_events` / `compute_metric_by_name_with_prepared_events`
///    repeatedly without recomputing derives.
pub fn prepare_events_for_compute<'a>(
    def: &'a TrackerDefinition,
    events: &[NormalizedEvent],
) -> Result<PreparedEvents<'a>, EngineError> {
    Ok(PreparedEvents {
        definition: def,
        events: prepare_events(def, events)?,
    })
}

/// Read-only derived events bound to their source definition. Preparation is a
/// read operation, not write acceptance: producers must still validate writes.
/// Raw event slices cannot be passed as prepared batches, and derived values
/// cannot be edited through this container.
///
/// ```compile_fail
/// use tracen_engine::{compile_tracker, prepare_events_for_compute};
/// let def = compile_tracker("tracker \"sample\" v1 { fields { amount: float } }").unwrap();
/// let mut batch = prepare_events_for_compute(&def, &[]).unwrap();
/// batch[0].payload_mut()["amount"] = serde_json::json!(42);
/// ```
///
/// ```compile_fail
/// let batch: tracen_engine::PreparedEvents<'_> = serde_json::from_str("[]").unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct PreparedEvents<'a> {
    definition: &'a TrackerDefinition,
    events: Vec<NormalizedEvent>,
}

impl std::ops::Deref for PreparedEvents<'_> {
    type Target = [NormalizedEvent];

    fn deref(&self) -> &Self::Target {
        &self.events
    }
}

fn ensure_prepared(def: &TrackerDefinition, batch: &PreparedEvents<'_>) -> Result<(), EngineError> {
    ensure_tracker(def, batch.definition.tracker_id())?;
    if !std::ptr::eq(def, batch.definition) && def != batch.definition {
        return Err(EngineError::Evaluation(
            "events were prepared for a different tracker definition".into(),
        ));
    }
    Ok(())
}

/// Builds a normalized event for pack-style inputs without full event JSON parsing.
pub fn prepare_pack_event(
    def: &TrackerDefinition,
    event_id: &str,
    ts: i64,
    payload: Value,
) -> Result<NormalizedEvent, EngineError> {
    prepare_pack_event_with_metadata(def, event_id, ts, payload, json!({}))
}

/// Prepare a read-only pack event while retaining metadata used by expressions.
pub fn prepare_pack_event_with_metadata(
    def: &TrackerDefinition,
    event_id: &str,
    ts: i64,
    payload: Value,
    meta: Value,
) -> Result<NormalizedEvent, EngineError> {
    build_pack_event(
        def,
        EventId::new(event_id),
        Timestamp::new(ts),
        payload,
        meta,
    )
}

/// Execute a previously prepared compute plan against an already prepared event slice.
pub fn compute_with_plan(
    def: &TrackerDefinition,
    plan: &ComputePlan<'_>,
    prepared_events: &PreparedEvents<'_>,
    query: Query,
) -> Result<EngineOutput, EngineError> {
    ensure_tracker(def, plan.definition.tracker_id())?;
    if !std::ptr::eq(def, plan.definition) && def != plan.definition {
        return Err(EngineError::Evaluation(
            "compute plan was compiled for a different tracker definition".into(),
        ));
    }
    ensure_prepared(def, prepared_events)?;

    let constraints = QueryConstraints::from_query(&query);
    let total_events = prepared_events.len();
    let window_events = if query.time_window.is_none() {
        total_events
    } else {
        constraints.select_events(prepared_events).len()
    };

    let metrics =
        evaluate_metrics(&plan.metric_specs, prepared_events, &query).map_err(EngineError::from)?;

    let relevant_events = constraints.select_events(prepared_events);
    let mut metrics = metrics;
    if query.time_window.is_some() {
        metrics.insert("window_event_count".into(), json!(window_events));
    }

    let alerts = evaluate_alerts(def.alerts(), &relevant_events)?;

    Ok(EngineOutput {
        total_events,
        window_events,
        metrics,
        alerts,
    })
}

/// Compute over an already prepared event slice (derived fields already populated).
///
/// This avoids reapplying derives when callers supply pre-derived payloads.
pub fn compute_with_prepared_events(
    def: &TrackerDefinition,
    prepared_events: &PreparedEvents<'_>,
    query: Query,
) -> Result<EngineOutput, EngineError> {
    let plan = compile_compute_plan(def)?;
    compute_with_plan(def, &plan, prepared_events, query)
}

/// Applies a new normalized event to the engine state and returns metric deltas.
pub fn apply(
    def: &TrackerDefinition,
    state: &mut EngineState<'_>,
    mut event: NormalizedEvent,
) -> Result<EngineOutputDelta, EngineError> {
    ensure_tracker(def, event.tracker_id())?;
    ensure_state(def, state)?;
    // A public NormalizedEvent can be constructed or mutated after validation.
    // Enforce acceptance immediately before the state mutation.
    event_intake::validate_normalized_event(def, &mut event, PayloadValidationPolicy::Event)?;
    let prev_total = state.total_events() as isize;
    apply_derives(def, &mut event)?;
    let ts = event.ts().as_millis();
    let event_id = event.event_id().as_str().to_owned();

    state.push(event);

    let mut metrics = BTreeMap::new();
    metrics.insert("last_event_ms".into(), json!(ts));
    metrics.insert("last_event_id".into(), json!(event_id));

    Ok(EngineOutputDelta {
        total_events_delta: state.total_events() as isize - prev_total,
        metrics,
    })
}

/// Applies DSL-derived fields to a single normalized event.
pub fn derive_event(
    def: &TrackerDefinition,
    event: &mut NormalizedEvent,
) -> Result<(), EngineError> {
    ensure_tracker(def, event.tracker_id())?;
    apply_derives(def, event)
}

/// Simulates hypothetical events by comparing outputs for base vs. augmented logs.
pub fn simulate(
    def: &TrackerDefinition,
    base_events: &[NormalizedEvent],
    hypothetical_events: &[NormalizedEvent],
    query: Query,
) -> Result<SimulationOutput, EngineError> {
    let base_prepared = prepare_events_for_compute(def, base_events)?;
    let hypothetical_prepared = prepare_events_for_compute(def, hypothetical_events)?;
    let plan = compile_compute_plan(def)?;

    let mut future = base_prepared.clone();
    future.events.extend_from_slice(&hypothetical_prepared);

    let base_output = compute_with_plan(def, &plan, &base_prepared, query.clone())?;
    let hypothetical_output = compute_with_plan(def, &plan, &future, query)?;

    let delta = EngineOutputDelta {
        total_events_delta: hypothetical_output.total_events as isize
            - base_output.total_events as isize,
        metrics: metric_delta(&base_output.metrics, &hypothetical_output.metrics),
    };

    Ok(SimulationOutput {
        base: base_output,
        hypothetical: hypothetical_output,
        delta,
    })
}

#[derive(Debug, Deserialize)]
struct EngineViewConfig {
    #[serde(default)]
    metrics: BTreeMap<String, EngineViewMetric>,
}

#[derive(Debug, Deserialize)]
struct EngineViewMetric {
    #[serde(default)]
    metric: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    aggregation: Option<String>,
}

/// Compute one view metric declared in DSL `views` config.
pub fn compute_view_metric(
    def: &TrackerDefinition,
    events: &[NormalizedEvent],
    view_name: &str,
    metric_key: &str,
    group_by: Vec<GroupByDimension>,
    query: Query,
) -> Result<Value, EngineError> {
    let prepared = prepare_events_for_compute(def, events)?;
    let view = def
        .views()
        .iter()
        .find(|view| view.name == view_name)
        .ok_or_else(|| EngineError::Evaluation(format!("unknown view: {view_name}")))?;
    let config_value = view
        .params
        .get("config")
        .ok_or_else(|| EngineError::Evaluation(format!("view '{}' missing config", view_name)))?;
    let config: EngineViewConfig = serde_json::from_value(config_value.clone()).map_err(|err| {
        EngineError::Evaluation(format!("invalid view config for '{}': {}", view_name, err))
    })?;
    let metric = config.metrics.get(metric_key).ok_or_else(|| {
        EngineError::Evaluation(format!(
            "unknown metric '{}' for view '{}'",
            metric_key, view_name
        ))
    })?;

    let metric_name = metric
        .metric
        .as_ref()
        .or(metric.source.as_ref())
        .cloned()
        .unwrap_or_else(|| metric_key.to_string());

    if def
        .metrics()
        .iter()
        .any(|candidate| candidate.name == metric_name)
    {
        compute_metric_by_name_with_prepared_events(
            def,
            &prepared,
            &metric_name,
            MetricComputeOptions {
                group_by: Some(group_by),
                time_window: query.time_window,
                filters: vec![],
            },
        )
    } else {
        let func = match metric.aggregation.as_deref().unwrap_or("sum") {
            "sum" => EvalAggregationFunc::Sum,
            "max" => EvalAggregationFunc::Max,
            "min" => EvalAggregationFunc::Min,
            "avg" => EvalAggregationFunc::Avg,
            "count" => EvalAggregationFunc::Count,
            "distinct_count" => EvalAggregationFunc::DistinctCount,
            other => Err(EngineError::Evaluation(format!(
                "unsupported aggregation '{}'",
                other
            )))?,
        };
        let target = if matches!(func, EvalAggregationFunc::Count) {
            None
        } else {
            Some(ScalarExpr::Field(FieldPath::new(normalize_field_path(
                metric.source.as_deref().ok_or_else(|| {
                    EngineError::Evaluation(format!(
                        "view '{}' metric '{}' missing source",
                        view_name, metric_key
                    ))
                })?,
            ))))
        };
        let groups = group_by
            .into_iter()
            .map(|dim| match dim {
                GroupByDimension::Field(name) => {
                    GroupExpr::Field(FieldPath::new(normalize_field_path(&name)))
                }
                GroupByDimension::Time(grain) => GroupExpr::Time(grain),
            })
            .collect::<Vec<_>>();

        let spec = MetricSpec {
            name: MetricName::new(format!("{}_{}", view_name, metric_key)),
            aggregation: AggregationSpec {
                weight: None,
                func,
                target,
                filter: None,
                group_by: groups,
            },
        };

        let metrics = evaluate_metrics(&[spec], &prepared, &query).map_err(EngineError::from)?;
        metrics
            .into_values()
            .next()
            .ok_or_else(|| EngineError::Evaluation("view metric produced no value".into()))
    }
}

/// Compute one DSL metric by name with optional group/time/filter overrides.
pub fn compute_metric_by_name(
    def: &TrackerDefinition,
    events: &[NormalizedEvent],
    metric_name: &str,
    options: MetricComputeOptions,
) -> Result<Value, EngineError> {
    let prepared = prepare_events_for_compute(def, events)?;
    compute_metric_by_name_with_prepared_events(def, &prepared, metric_name, options)
}

/// Compute a metric by name from already prepared events.
pub fn compute_metric_by_name_with_prepared_events(
    def: &TrackerDefinition,
    prepared_events: &PreparedEvents<'_>,
    metric_name: &str,
    options: MetricComputeOptions,
) -> Result<Value, EngineError> {
    ensure_prepared(def, prepared_events)?;
    let metric = def
        .metrics()
        .iter()
        .find(|metric| metric.name == metric_name)
        .ok_or_else(|| EngineError::Evaluation(format!("unknown metric: {metric_name}")))?;

    let mut spec = compile_metric_spec(metric, options.group_by.as_ref())?;
    if !options.filters.is_empty() {
        spec.aggregation.filter = Some(filters_to_condition(&options.filters)?);
    }

    let query = Query {
        time_window: options.time_window,
        grains: vec![],
        metric_precision: None,
        group_key_encoding: Default::default(),
    };
    let metrics = evaluate_metrics(&[spec], prepared_events, &query).map_err(EngineError::from)?;
    metrics
        .into_values()
        .next()
        .ok_or_else(|| EngineError::Evaluation("metric produced no value".into()))
}

fn prepare_events(
    def: &TrackerDefinition,
    events: &[NormalizedEvent],
) -> Result<Vec<NormalizedEvent>, EngineError> {
    ensure_events(def, events)?;
    events
        .iter()
        .cloned()
        .map(|mut event| {
            apply_derives(def, &mut event)?;
            Ok(event)
        })
        .collect::<Result<Vec<_>, EngineError>>()
}

fn ensure_tracker(def: &TrackerDefinition, tracker_id: &TrackerId) -> Result<(), EngineError> {
    if tracker_id != def.tracker_id() {
        Err(EngineError::TrackerMismatch {
            expected: def.tracker_id().clone(),
            actual: tracker_id.clone(),
        })?;
    }
    Ok(())
}

fn ensure_state(def: &TrackerDefinition, state: &EngineState<'_>) -> Result<(), EngineError> {
    if state.tracker_id() != def.tracker_id() {
        Err(EngineError::StateMismatch {
            expected: def.tracker_id().clone(),
            actual: state.tracker_id().clone(),
        })?;
    }
    if !std::ptr::eq(def, state.definition()) && def != state.definition() {
        return Err(EngineError::Evaluation(
            "state was created for a different tracker definition".into(),
        ));
    }
    Ok(())
}

fn ensure_events(def: &TrackerDefinition, events: &[NormalizedEvent]) -> Result<(), EngineError> {
    for event in events {
        ensure_tracker(def, event.tracker_id())?;
    }
    Ok(())
}

fn apply_derives(def: &TrackerDefinition, event: &mut NormalizedEvent) -> Result<(), EngineError> {
    let mut derived = BTreeMap::<String, Value>::new();
    for derive in def.derives() {
        let value = eval_expression(&derive.expr, event, &derived)?;
        derived.insert(derive.name.clone(), value);
    }

    if let Some(payload) = event.payload_mut().as_object_mut() {
        for (key, value) in derived {
            payload.insert(key, value);
        }
    }

    Ok(())
}

fn compile_metric_specs(metrics: &[MetricDefinition]) -> Result<Vec<MetricSpec>, EngineError> {
    metrics
        .iter()
        .map(|metric| compile_metric_spec(metric, None))
        .collect()
}

fn compile_metric_spec(
    metric: &MetricDefinition,
    group_by_override: Option<&Vec<GroupByDimension>>,
) -> Result<MetricSpec, EngineError> {
    let func = match metric.aggregation.func {
        tracen_ir::AggregationFunc::Sum => EvalAggregationFunc::Sum,
        tracen_ir::AggregationFunc::Max => EvalAggregationFunc::Max,
        tracen_ir::AggregationFunc::Min => EvalAggregationFunc::Min,
        tracen_ir::AggregationFunc::Avg => EvalAggregationFunc::Avg,
        tracen_ir::AggregationFunc::Count => EvalAggregationFunc::Count,
        tracen_ir::AggregationFunc::DistinctCount => EvalAggregationFunc::DistinctCount,
        tracen_ir::AggregationFunc::WeightedAvg => EvalAggregationFunc::WeightedAvg,
    };

    let target = metric
        .aggregation
        .target
        .as_ref()
        .map(to_scalar_expr)
        .transpose()?;

    let mut group_by = Vec::new();
    let input_group_by = group_by_override.unwrap_or(&metric.aggregation.group_by);
    for group in input_group_by {
        match group {
            GroupByDimension::Field(name) => {
                group_by.push(GroupExpr::Field(FieldPath::new(normalize_field_path(name))));
            }
            GroupByDimension::Time(grain) => group_by.push(GroupExpr::Time(*grain)),
        }
    }

    if group_by_override.is_none() {
        if let Some(over) = metric.aggregation.over {
            if !matches!(over, TimeGrain::AllTime) && group_by.is_empty() {
                group_by.push(GroupExpr::Time(over));
            }
        }
    }

    Ok(MetricSpec {
        name: MetricName::new(metric.name.clone()),
        aggregation: AggregationSpec {
            weight: metric
                .aggregation
                .weight
                .as_ref()
                .map(to_scalar_expr)
                .transpose()?,
            func,
            target,
            filter: None,
            group_by,
        },
    })
}

fn literal_to_scalar_expr(value: &Value) -> Result<ScalarExpr, EngineError> {
    match value {
        Value::Number(number) => number
            .as_f64()
            .map(ScalarExpr::Number)
            .ok_or_else(|| EngineError::Evaluation("invalid numeric filter literal".into())),
        Value::String(text) => Ok(ScalarExpr::String(text.clone())),
        Value::Bool(flag) => Ok(ScalarExpr::Bool(*flag)),
        Value::Null => Err(EngineError::Evaluation(
            "null filter literals are unsupported".into(),
        )),
        Value::Array(_) | Value::Object(_) => Err(EngineError::Evaluation(
            "complex filter literals are unsupported".into(),
        )),
    }
}

fn filters_to_condition(filters: &[MetricFilter]) -> Result<ConditionExpr, EngineError> {
    let mut parts = Vec::with_capacity(filters.len());
    for filter in filters {
        let lhs = ScalarExpr::Field(FieldPath::new(normalize_field_path(&filter.field)));
        let rhs = literal_to_scalar_expr(&filter.value)?;
        let expr = match filter.op {
            MetricFilterOp::Eq => ConditionExpr::Eq(Box::new(lhs), Box::new(rhs)),
            MetricFilterOp::Neq => ConditionExpr::Neq(Box::new(lhs), Box::new(rhs)),
            MetricFilterOp::Gt => ConditionExpr::Gt(Box::new(lhs), Box::new(rhs)),
            MetricFilterOp::Gte => ConditionExpr::Gte(Box::new(lhs), Box::new(rhs)),
            MetricFilterOp::Lt => ConditionExpr::Lt(Box::new(lhs), Box::new(rhs)),
            MetricFilterOp::Lte => ConditionExpr::Lte(Box::new(lhs), Box::new(rhs)),
        };
        parts.push(expr);
    }

    if parts.len() == 1 {
        Ok(parts.remove(0))
    } else {
        Ok(ConditionExpr::And(parts))
    }
}

fn evaluate_alerts(
    alerts: &[AlertDefinition],
    events: &[&NormalizedEvent],
) -> Result<Vec<Value>, EngineError> {
    if alerts.is_empty() || events.is_empty() {
        Ok(Vec::new())
    } else {
        let mut output = Vec::new();
        for event in events {
            for alert in alerts {
                let value = eval_expression(&alert.expr, event, &BTreeMap::new())?;
                if is_alert_signal(&value) {
                    output.push(json!({
                        "alert": alert.name,
                        "event_id": event.event_id().as_str(),
                        "value": value,
                    }));
                }
            }
        }
        Ok(output)
    }
}

fn is_alert_signal(value: &Value) -> bool {
    match value {
        Value::Bool(flag) => *flag,
        Value::Null => false,
        Value::Number(number) => number.as_f64().map(|v| v != 0.0).unwrap_or(false),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(_) => true,
    }
}

fn eval_expression(
    expr: &Expression,
    event: &NormalizedEvent,
    derived: &BTreeMap<String, Value>,
) -> Result<Value, EngineError> {
    match expr {
        Expression::Number(v) if !v.is_finite() => {
            Err(EngineError::Evaluation("non-finite numeric value".into()))
        }
        Expression::Number(v) => Ok(json!(v)),
        Expression::Int(v) => Ok(json!(v)),
        Expression::Bool(v) => Ok(json!(v)),
        Expression::Text(v) => Ok(json!(v)),
        Expression::Null => Ok(Value::Null),
        Expression::Field(path) => {
            Ok(resolve_field_value(path, event, derived).unwrap_or(Value::Null))
        }
        Expression::Binary { op, left, right } => {
            let lhs = eval_expression(left, event, derived)?;
            let rhs = eval_expression(right, event, derived)?;
            let lhs_num = lhs
                .as_f64()
                .ok_or_else(|| EngineError::Evaluation("left operand must be numeric".into()))?;
            let rhs_num = rhs
                .as_f64()
                .ok_or_else(|| EngineError::Evaluation("right operand must be numeric".into()))?;
            let result = match op {
                BinaryOperator::Add => lhs_num + rhs_num,
                BinaryOperator::Sub => lhs_num - rhs_num,
                BinaryOperator::Mul => lhs_num * rhs_num,
                BinaryOperator::Div => {
                    if rhs_num == 0.0 {
                        Err(EngineError::Evaluation("division by zero".into()))?;
                    }
                    lhs_num / rhs_num
                }
                BinaryOperator::Mod => lhs_num % rhs_num,
            };
            if !result.is_finite() {
                return Err(EngineError::Evaluation(
                    "non-finite arithmetic result".into(),
                ));
            }
            Ok(json!(result))
        }
        Expression::Conditional {
            condition,
            then_expr,
            else_expr,
        } => {
            if eval_condition(condition, event, derived)? {
                eval_expression(then_expr, event, derived)
            } else {
                eval_expression(else_expr, event, derived)
            }
        }
        Expression::Function { name, args } => {
            if name == "signal" {
                let signal_name = args
                    .first()
                    .map(|arg| eval_expression(arg, event, derived))
                    .transpose()?
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| "signal".to_string());
                let payload = args
                    .get(1)
                    .map(|arg| eval_expression(arg, event, derived))
                    .transpose()?
                    .unwrap_or(Value::Null);
                Ok(json!({ "type": signal_name, "payload": payload }))
            } else {
                Err(EngineError::Evaluation(format!(
                    "unsupported function in expression evaluation: {name}"
                )))
            }
        }
    }
}

fn eval_condition(
    condition: &Condition,
    event: &NormalizedEvent,
    derived: &BTreeMap<String, Value>,
) -> Result<bool, EngineError> {
    match condition {
        Condition::True => Ok(true),
        Condition::False => Ok(false),
        Condition::Not(inner) => Ok(!eval_condition(inner, event, derived)?),
        Condition::And(parts) => {
            let mut all_true = true;
            for part in parts {
                if !eval_condition(part, event, derived)? {
                    all_true = false;
                    break;
                }
            }
            Ok(all_true)
        }
        Condition::Or(parts) => {
            let mut any_true = false;
            for part in parts {
                if eval_condition(part, event, derived)? {
                    any_true = true;
                    break;
                }
            }
            Ok(any_true)
        }
        Condition::Comparison { op, left, right } => {
            let lhs = eval_expression(left, event, derived)?;
            let rhs = eval_expression(right, event, derived)?;
            match op {
                ComparisonOperator::Eq => Ok(values_equal(&lhs, &rhs)),
                ComparisonOperator::Neq => Ok(!values_equal(&lhs, &rhs)),
                ComparisonOperator::Gt => compare_number(lhs, rhs, |a, b| a > b),
                ComparisonOperator::Gte => compare_number(lhs, rhs, |a, b| a >= b),
                ComparisonOperator::Lt => compare_number(lhs, rhs, |a, b| a < b),
                ComparisonOperator::Lte => compare_number(lhs, rhs, |a, b| a <= b),
            }
        }
    }
}

// JSON retains integer and floating representations. Equality must compare their
// values without rounding distinct 64-bit integers through f64.
fn values_equal(lhs: &Value, rhs: &Value) -> bool {
    fn integer(number: &serde_json::Number) -> Option<i128> {
        number
            .as_i64()
            .map(i128::from)
            .or_else(|| number.as_u64().map(i128::from))
    }
    fn float_integer(float: f64, integer: i128) -> bool {
        float.is_finite()
            && float.fract() == 0.0
            && float >= i64::MIN as f64
            && float < 18_446_744_073_709_551_616.0
            && float as i128 == integer
    }
    match (lhs, rhs) {
        (Value::Number(a), Value::Number(b)) => match (integer(a), integer(b)) {
            (Some(a), Some(b)) => a == b,
            (Some(a), None) => b.as_f64().is_some_and(|b| float_integer(b, a)),
            (None, Some(b)) => a.as_f64().is_some_and(|a| float_integer(a, b)),
            (None, None) => a.as_f64() == b.as_f64(),
        },
        _ => lhs == rhs,
    }
}

fn compare_number(
    lhs: Value,
    rhs: Value,
    predicate: impl FnOnce(f64, f64) -> bool,
) -> Result<bool, EngineError> {
    match (lhs.as_f64(), rhs.as_f64()) {
        (Some(lhs), Some(rhs)) => Ok(predicate(lhs, rhs)),
        _ => Ok(false),
    }
}

fn resolve_field_value(
    raw_path: &str,
    event: &NormalizedEvent,
    derived: &BTreeMap<String, Value>,
) -> Option<Value> {
    if let Some(value) = derived.get(raw_path) {
        Some(value.clone())
    } else {
        let path = normalize_field_path(raw_path);
        let mut segments = path.split('.');
        let root = segments.next()?;

        if root == "event" {
            let field = segments.next()?;
            match field {
                "id" => Some(json!(event.event_id().as_str())),
                "tracker_id" => Some(json!(event.tracker_id().as_str())),
                "ts" => Some(json!(event.ts().as_millis())),
                _ => None,
            }
        } else if let Some(mut current) = match root {
            "payload" => Some(event.payload()),
            "meta" => Some(event.meta()),
            _ => None,
        } {
            for segment in segments {
                current = current.get(segment)?;
            }
            Some(current.clone())
        } else {
            None
        }
    }
}

fn normalize_field_path(path: &str) -> String {
    let trimmed = path.trim();
    if trimmed.starts_with("payload.")
        || trimmed.starts_with("meta.")
        || trimmed.starts_with("event.")
    {
        trimmed.to_string()
    } else {
        format!("payload.{trimmed}")
    }
}

fn to_scalar_expr(expr: &Expression) -> Result<ScalarExpr, EngineError> {
    Ok(match expr {
        Expression::Number(v) => ScalarExpr::Number(*v),
        Expression::Int(v) => ScalarExpr::Number(*v as f64),
        Expression::Bool(v) => ScalarExpr::Bool(*v),
        Expression::Text(v) => ScalarExpr::String(v.clone()),
        Expression::Null => ScalarExpr::Null,
        Expression::Field(path) => ScalarExpr::Field(FieldPath::new(normalize_field_path(path))),
        Expression::Binary { op, left, right } => {
            let mapped = match op {
                BinaryOperator::Add => tracen_eval::BinaryOp::Add,
                BinaryOperator::Sub => tracen_eval::BinaryOp::Sub,
                BinaryOperator::Mul => tracen_eval::BinaryOp::Mul,
                BinaryOperator::Div => tracen_eval::BinaryOp::Div,
                BinaryOperator::Mod => Err(EngineError::Evaluation(
                    "mod operator unsupported in metric expressions".into(),
                ))?,
            };
            ScalarExpr::Binary {
                op: mapped,
                left: Box::new(to_scalar_expr(left)?),
                right: Box::new(to_scalar_expr(right)?),
            }
        }
        Expression::Conditional {
            condition,
            then_expr,
            else_expr,
        } => ScalarExpr::Conditional {
            condition: Box::new(to_condition_expr(condition)?),
            then_expr: Box::new(to_scalar_expr(then_expr)?),
            else_expr: Box::new(to_scalar_expr(else_expr)?),
        },
        Expression::Function { name, .. } => Err(EngineError::Evaluation(format!(
            "function '{name}' cannot be used in metric aggregation target"
        )))?,
    })
}

fn to_condition_expr(condition: &Condition) -> Result<ConditionExpr, EngineError> {
    Ok(match condition {
        Condition::True => ConditionExpr::True,
        Condition::False => ConditionExpr::False,
        Condition::Not(inner) => ConditionExpr::Not(Box::new(to_condition_expr(inner)?)),
        Condition::And(parts) => ConditionExpr::And(
            parts
                .iter()
                .map(to_condition_expr)
                .collect::<Result<Vec<_>, EngineError>>()?,
        ),
        Condition::Or(parts) => ConditionExpr::Or(
            parts
                .iter()
                .map(to_condition_expr)
                .collect::<Result<Vec<_>, EngineError>>()?,
        ),
        Condition::Comparison { op, left, right } => {
            let lhs = Box::new(to_scalar_expr(left)?);
            let rhs = Box::new(to_scalar_expr(right)?);
            match op {
                ComparisonOperator::Eq => ConditionExpr::Eq(lhs, rhs),
                ComparisonOperator::Neq => ConditionExpr::Neq(lhs, rhs),
                ComparisonOperator::Gt => ConditionExpr::Gt(lhs, rhs),
                ComparisonOperator::Gte => ConditionExpr::Gte(lhs, rhs),
                ComparisonOperator::Lt => ConditionExpr::Lt(lhs, rhs),
                ComparisonOperator::Lte => ConditionExpr::Lte(lhs, rhs),
            }
        }
    })
}

impl From<EvalError> for EngineError {
    fn from(value: EvalError) -> Self {
        EngineError::Evaluation(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tracen_ir::{EventId, NormalizedEvent, TimeWindow, Timestamp};

    fn sample_definition() -> TrackerDefinition {
        compile_tracker(
            r#"
            tracker "sample" v1 {
              fields {
                value_a: float optional
                value_b: int optional
              }
              derive {
                combined_value = if (value_a > 0 && value_b > 0) then value_a * value_b else 0
              }
              metrics {
                total_value = sum(combined_value)
              }
            }
            "#,
        )
        .expect("compile")
    }

    fn sample_event(
        def: &TrackerDefinition,
        ts: i64,
        value_a: i64,
        value_b: i64,
    ) -> NormalizedEvent {
        NormalizedEvent::new(
            EventId::new(format!("event-{ts}")),
            def.tracker_id().clone(),
            Timestamp::new(ts),
            json!({"value_a": value_a, "value_b": value_b}),
            json!({}),
        )
    }

    #[test]
    fn non_finite_calculations_fail_instead_of_becoming_null() {
        for expression in ["value_a % 0", "value_a * value_a"] {
            let def = compile_tracker(&format!(
                "tracker \"finite\" v1 {{ fields {{ value_a: float }} derive {{ result = {expression} }} }}"
            )).unwrap();
            let event = NormalizedEvent::new(
                EventId::new("overflow"),
                def.tracker_id().clone(),
                Timestamp::new(0),
                json!({"value_a": 1e308}),
                json!({}),
            );
            let mut state = EngineState::for_definition(&def);
            assert!(
                apply(&def, &mut state, event.clone()).is_err(),
                "{expression}"
            );
            assert_eq!(state.total_events(), 0);
            assert!(
                prepare_events_for_compute(&def, &[event]).is_err(),
                "{expression}"
            );
        }
        for metric in ["sum(value_a * value_a)", "sum(value_a)"] {
            let def = compile_tracker(&format!(
                "tracker \"finite\" v1 {{ fields {{ value_a: float }} metrics {{ total = {metric} over all_time }} }}"
            )).unwrap();
            let events: Vec<_> = (0..2)
                .map(|i| {
                    NormalizedEvent::new(
                        EventId::new(format!("e{i}")),
                        def.tracker_id().clone(),
                        Timestamp::new(i),
                        json!({"value_a": 1e308}),
                        json!({}),
                    )
                })
                .collect();
            assert!(
                compute(&def, &events, Query::default()).is_err(),
                "{metric}"
            );
        }
        let def = compile_tracker("tracker \"finite\" v1 { fields { value_a: float } metrics { largest = max(value_a) over all_time } }").unwrap();
        let events: Vec<_> = (0..2)
            .map(|i| {
                NormalizedEvent::new(
                    EventId::new(format!("e{i}")),
                    def.tracker_id().clone(),
                    Timestamp::new(i),
                    json!({"value_a": 1e308}),
                    json!({}),
                )
            })
            .collect();
        assert_eq!(
            compute(&def, &events, Query::default()).unwrap().metrics["largest"],
            json!(1e308)
        );
    }

    #[test]
    fn compute_ir_metric() {
        let def = sample_definition();
        let events = vec![
            sample_event(&def, 1_000, 80, 5),
            sample_event(&def, 2_000, 60, 8),
        ];
        let out = compute(&def, &events, Query::default()).expect("compute");
        assert_eq!(out.metrics.get("total_value"), Some(&json!(880.0)));
    }

    #[test]
    fn validate_against_schema() {
        let def = sample_definition();
        let event = validate_event(
            &def,
            r#"{"event_id":"e1","ts":1,"payload":{"value_a":"oops"}}"#,
        );
        assert!(event.is_err());
    }

    #[test]
    fn validate_event_rejects_unknown_fields_by_default() {
        let def = sample_definition();
        let event = validate_event(
            &def,
            r#"{"event_id":"e1","ts":1,"payload":{"value_a":1,"unknown":"x"}}"#,
        );
        assert!(event.is_err());
    }

    #[test]
    fn event_alerts_evaluate_derived_signal_payloads() {
        let def = compile_tracker(
            r#"tracker "signals" v1 {
            fields { amount: float }
            derive { doubled = amount * 2 }
            alerts { warning = if payload.doubled > 8 then signal("HIGH", doubled) else null }
        }"#,
        )
        .unwrap();
        let event =
            validate_event(&def, r#"{"event_id":"high","ts":1,"payload":{"amount":5}}"#).unwrap();
        let output = compute(&def, &[event], Query::default()).unwrap();
        assert_eq!(
            output.alerts,
            vec![
                json!({"alert":"warning", "event_id":"high", "value":{"type":"HIGH", "payload":10.0}})
            ]
        );
    }

    #[test]
    fn compute_alerts_respect_query_time_window() {
        let def = compile_tracker(
            r#"
            tracker "sample" v1 {
              fields {
                value_a: float optional
              }
              alerts {
                too_high = if value_a > 50 then true else false
              }
            }
            "#,
        )
        .expect("compile");

        let events = vec![
            NormalizedEvent::new(
                EventId::new("e1"),
                def.tracker_id().clone(),
                Timestamp::new(1_000),
                json!({"value_a": 75.0}),
                json!({}),
            ),
            NormalizedEvent::new(
                EventId::new("e2"),
                def.tracker_id().clone(),
                Timestamp::new(2_000),
                json!({"value_a": 25.0}),
                json!({}),
            ),
            NormalizedEvent::new(
                EventId::new("e3"),
                def.tracker_id().clone(),
                Timestamp::new(3_000),
                json!({"value_a": 90.0}),
                json!({}),
            ),
        ];
        let output = compute(
            &def,
            &events,
            Query {
                time_window: Some(TimeWindow {
                    start: Timestamp::new(1_500),
                    end: Timestamp::new(2_500),
                }),
                grains: vec![],
                metric_precision: None,
                group_key_encoding: Default::default(),
            },
        )
        .expect("compute");

        assert_eq!(output.alerts.len(), 0);
    }

    #[test]
    fn compute_view_metric_from_dsl_view_config() {
        let def = compile_tracker(
            r#"
            tracker "sample" v1 {
              fields {
                value_b: int optional
                value_a: float optional
              }
              derive {
                combined_value = if (value_a > 0 && value_b > 0) then value_a * value_b else 0
              }
              metrics {
                total_value = sum(combined_value) over all_time
              }
              views {
                view "summary" {
                  config = {"metrics":{"total_value":{"metric":"total_value"}}}
                }
              }
            }
            "#,
        )
        .expect("compile");
        let events = vec![
            sample_event(&def, 1_000, 80, 5),
            sample_event(&def, 2_000, 60, 8),
        ];
        let value = compute_view_metric(
            &def,
            &events,
            "summary",
            "total_value",
            vec![],
            Query::default(),
        )
        .expect("view metric");
        assert_eq!(value, json!(880.0));
    }

    #[test]
    fn compute_metric_by_name_count_with_grouping() {
        let def = compile_tracker(
            r#"
            tracker "sample" v1 {
              fields {
                group_key: text
                value_b: int optional
              }
              metrics {
                total_items = count() over all_time
              }
            }
            "#,
        )
        .expect("compile");

        let events = vec![
            NormalizedEvent::new(
                EventId::new("e1"),
                def.tracker_id().clone(),
                Timestamp::new(1_000),
                json!({"group_key":"segment_a","value_b":5}),
                json!({}),
            ),
            NormalizedEvent::new(
                EventId::new("e2"),
                def.tracker_id().clone(),
                Timestamp::new(2_000),
                json!({"group_key":"segment_a","value_b":8}),
                json!({}),
            ),
            NormalizedEvent::new(
                EventId::new("e3"),
                def.tracker_id().clone(),
                Timestamp::new(3_000),
                json!({"group_key":"segment_b","value_b":10}),
                json!({}),
            ),
        ];

        let grouped = compute_metric_by_name(
            &def,
            &events,
            "total_items",
            MetricComputeOptions {
                group_by: Some(vec![GroupByDimension::Field("group_key".to_string())]),
                time_window: None,
                filters: vec![],
            },
        )
        .expect("count metric");

        let map = grouped.as_object().expect("grouped object");
        assert_eq!(map.get("segment_a"), Some(&json!(2)));
        assert_eq!(map.get("segment_b"), Some(&json!(1)));
    }

    #[test]
    fn compute_with_prepared_events_reuses_derives() {
        let def = sample_definition();
        let events = vec![
            sample_event(&def, 1_000, 80, 5),
            sample_event(&def, 2_000, 60, 8),
        ];
        let prepared = prepare_events_for_compute(&def, &events).expect("prepare events");
        let compute_result = compute_with_prepared_events(&def, &prepared, Query::default())
            .expect("compute prepared");
        let direct_result = compute(&def, &events, Query::default()).expect("compute direct");
        assert_eq!(compute_result.metrics, direct_result.metrics);
        assert!(prepared[0]
            .payload()
            .as_object()
            .expect("payload object")
            .contains_key("combined_value"));
    }

    #[test]
    fn compute_metric_by_name_with_prepared_events_matches_regular_path() {
        let def = compile_tracker(
            r#"
            tracker "sample" v1 {
              fields {
                value_b: int optional
                value_a: float optional
              }
              metrics {
                total_value = sum(value_a) over all_time
              }
            }
            "#,
        )
        .expect("compile");
        let events = vec![
            sample_event(&def, 1_000, 80, 5),
            sample_event(&def, 2_000, 60, 8),
        ];
        let prepared = prepare_events_for_compute(&def, &events).expect("prepare");
        let by_name = compute_metric_by_name_with_prepared_events(
            &def,
            &prepared,
            "total_value",
            MetricComputeOptions::default(),
        )
        .expect("prepared metric");
        let by_name_direct = compute_metric_by_name(
            &def,
            &events,
            "total_value",
            MetricComputeOptions::default(),
        )
        .expect("direct metric");
        assert_eq!(by_name, by_name_direct);
    }
}

#[cfg(test)]
mod equality_regression {
    use super::*;
    #[test]
    fn numeric_equality_keeps_large_integers_exact() {
        for (a, b, equal) in [
            (json!(0), json!(-0.0), true),
            (json!(500u64), json!(500.0), true),
            (json!(9007199254740993u64), json!(9007199254740992.0), false),
            (
                json!(9007199254740992u64),
                json!(9007199254740993u64),
                false,
            ),
            (json!(i64::MIN), json!(i64::MIN as f64), true),
            (json!(i64::MAX), json!(i64::MAX as f64), false),
            (json!(u64::MAX), json!(u64::MAX as f64), false),
            (json!(-1), json!(u64::MAX), false),
            (json!(1), json!(1.5), false),
            (json!(null), json!(null), true),
            (json!(false), json!(0), false),
            (json!("0"), json!(0), false),
        ] {
            assert_eq!(values_equal(&a, &b), equal, "{a} vs {b}");
            assert_eq!(values_equal(&b, &a), equal);
        }
    }
    #[test]
    fn acceptance_numeric_spelling_and_exact_integer_boundaries() {
        for (rule, value, accepted) in [
            ("value_a != 0", json!(0), false),
            ("value_a != 0", json!(0.0), false),
            ("value_a == 500.0", json!(500), true),
            ("value_a * 2 == 1000", json!(500), true),
        ] {
            let dsl = format!("tracker \"equality\" v1 {{ fields {{ value_a: float }} validations {{ exact = {rule} }} }}");
            let definition = compile_tracker(&dsl).unwrap();
            let result = validate_event(
                &definition,
                &json!({"event_id":"one","ts":0,"payload":{"value_a":value}}).to_string(),
            );
            assert_eq!(result.is_ok(), accepted, "{rule}: {value}");
        }
    }
}
