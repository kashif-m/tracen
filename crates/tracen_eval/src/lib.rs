//! Expression evaluation and aggregation helpers for tracker metrics.

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;
use time::OffsetDateTime;
use tracen_ir::{NormalizedEvent, Query, TimeGrain, TimeWindow, Timestamp};

/// Errors produced while evaluating expressions or aggregations.
#[derive(Debug, Error)]
pub enum EvalError {
    #[error("field not found: {0}")]
    FieldNotFound(String),
    #[error("type mismatch: {0}")]
    TypeMismatch(&'static str),
    #[error("division by zero")]
    DivisionByZero,
}

/// Result alias for expression evaluation.
type EvalResult<T> = Result<T, EvalError>;

/// Scalar value used within expressions.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    Number(f64),
    Bool(bool),
    Text(String),
    Null,
}

impl ScalarValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarValue::Number(v) => Some(*v),
            ScalarValue::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ScalarValue::Bool(v) => Some(*v),
            ScalarValue::Number(v) => Some(*v != 0.0),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ScalarValue::Text(v) => Some(v),
            _ => None,
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            ScalarValue::Number(v) => json!(v),
            ScalarValue::Bool(v) => json!(v),
            ScalarValue::Text(v) => json!(v),
            ScalarValue::Null => Value::Null,
        }
    }
}

impl From<&Value> for ScalarValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Number(num) => match num.as_f64() {
                Some(v) => ScalarValue::Number(v),
                None => ScalarValue::Null,
            },
            Value::Bool(v) => ScalarValue::Bool(*v),
            Value::String(v) => ScalarValue::Text(v.clone()),
            Value::Null => ScalarValue::Null,
            Value::Array(_) | Value::Object(_) => ScalarValue::Null,
        }
    }
}

/// Binary operators supported in scalar expressions.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

/// Boolean condition expression.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConditionExpr {
    True,
    False,
    Eq(Box<ScalarExpr>, Box<ScalarExpr>),
    Neq(Box<ScalarExpr>, Box<ScalarExpr>),
    Gt(Box<ScalarExpr>, Box<ScalarExpr>),
    Gte(Box<ScalarExpr>, Box<ScalarExpr>),
    Lt(Box<ScalarExpr>, Box<ScalarExpr>),
    Lte(Box<ScalarExpr>, Box<ScalarExpr>),
    And(Vec<ConditionExpr>),
    Or(Vec<ConditionExpr>),
    Not(Box<ConditionExpr>),
}

/// Scalar expressions supporting literals, field lookups, arithmetic, and conditionals.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ScalarExpr {
    Number(f64),
    Bool(bool),
    String(String),
    Field(FieldPath),
    Binary {
        op: BinaryOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    Conditional {
        condition: Box<ConditionExpr>,
        then_expr: Box<ScalarExpr>,
        else_expr: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub fn evaluate(&self, event: &NormalizedEvent) -> EvalResult<ScalarValue> {
        match self {
            ScalarExpr::Number(v) => Ok(ScalarValue::Number(*v)),
            ScalarExpr::Bool(v) => Ok(ScalarValue::Bool(*v)),
            ScalarExpr::String(v) => Ok(ScalarValue::Text(v.clone())),
            ScalarExpr::Field(path) => path.evaluate(event),
            ScalarExpr::Binary { op, left, right } => {
                let lhs = left
                    .evaluate(event)?
                    .as_f64()
                    .ok_or(EvalError::TypeMismatch(
                        "binary operations expect numeric operands",
                    ))?;
                let rhs = right
                    .evaluate(event)?
                    .as_f64()
                    .ok_or(EvalError::TypeMismatch(
                        "binary operations expect numeric operands",
                    ))?;

                let result = match op {
                    BinaryOp::Add => lhs + rhs,
                    BinaryOp::Sub => lhs - rhs,
                    BinaryOp::Mul => lhs * rhs,
                    BinaryOp::Div => {
                        if rhs == 0.0 {
                            Err(EvalError::DivisionByZero)?;
                        }
                        lhs / rhs
                    }
                };

                Ok(ScalarValue::Number(result))
            }
            ScalarExpr::Conditional {
                condition,
                then_expr,
                else_expr,
            } => {
                if condition.evaluate(event)? {
                    then_expr.evaluate(event)
                } else {
                    else_expr.evaluate(event)
                }
            }
        }
    }
}

impl ConditionExpr {
    pub fn evaluate(&self, event: &NormalizedEvent) -> EvalResult<bool> {
        match self {
            ConditionExpr::True => Ok(true),
            ConditionExpr::False => Ok(false),
            ConditionExpr::Eq(a, b) => Ok(a.evaluate(event)? == b.evaluate(event)?),
            ConditionExpr::Neq(a, b) => Ok(a.evaluate(event)? != b.evaluate(event)?),
            ConditionExpr::Gt(a, b) => Ok(
                match (a.evaluate(event)?.as_f64(), b.evaluate(event)?.as_f64()) {
                    (Some(lhs), Some(rhs)) => lhs > rhs,
                    _ => false,
                },
            ),
            ConditionExpr::Gte(a, b) => Ok(
                match (a.evaluate(event)?.as_f64(), b.evaluate(event)?.as_f64()) {
                    (Some(lhs), Some(rhs)) => lhs >= rhs,
                    _ => false,
                },
            ),
            ConditionExpr::Lt(a, b) => Ok(
                match (a.evaluate(event)?.as_f64(), b.evaluate(event)?.as_f64()) {
                    (Some(lhs), Some(rhs)) => lhs < rhs,
                    _ => false,
                },
            ),
            ConditionExpr::Lte(a, b) => Ok(
                match (a.evaluate(event)?.as_f64(), b.evaluate(event)?.as_f64()) {
                    (Some(lhs), Some(rhs)) => lhs <= rhs,
                    _ => false,
                },
            ),
            ConditionExpr::And(conditions) => {
                let mut all_true = true;
                for condition in conditions {
                    if !condition.evaluate(event)? {
                        all_true = false;
                        break;
                    }
                }
                Ok(all_true)
            }
            ConditionExpr::Or(conditions) => {
                let mut any_true = false;
                for condition in conditions {
                    if condition.evaluate(event)? {
                        any_true = true;
                        break;
                    }
                }
                Ok(any_true)
            }
            ConditionExpr::Not(condition) => Ok(!condition.evaluate(event)?),
        }
    }
}

/// Path-based field lookup within payload/meta/event metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldPath(Vec<String>);

impl FieldPath {
    pub fn new(path: impl Into<String>) -> Self {
        FieldPath(
            path.into()
                .split('.')
                .map(|segment| segment.trim().to_string())
                .filter(|segment| !segment.is_empty())
                .collect(),
        )
    }

    fn evaluate(&self, event: &NormalizedEvent) -> EvalResult<ScalarValue> {
        if self.0.is_empty() {
            return Ok(ScalarValue::Null);
        }

        let mut segments = self.0.iter();
        let Some(first) = segments.next() else {
            return Ok(ScalarValue::Null);
        };

        match first.as_str() {
            "payload" => {
                let mut current = event.payload();
                for segment in segments {
                    let Some(next) = current.get(segment) else {
                        return Ok(ScalarValue::Null);
                    };
                    current = next;
                }
                Ok(ScalarValue::from(current))
            }
            "meta" => {
                let mut current = event.meta();
                for segment in segments {
                    let Some(next) = current.get(segment) else {
                        return Ok(ScalarValue::Null);
                    };
                    current = next;
                }
                Ok(ScalarValue::from(current))
            }
            "event" => {
                let Some(next) = segments.next() else {
                    return Ok(ScalarValue::Null);
                };
                match next.as_str() {
                    "id" => Ok(ScalarValue::Text(event.event_id().as_str().to_string())),
                    "tracker_id" => Ok(ScalarValue::Text(event.tracker_id().as_str().to_string())),
                    "ts" => Ok(ScalarValue::Number(event.ts().as_millis() as f64)),
                    _ => Ok(ScalarValue::Null),
                }
            }
            _ => Ok(ScalarValue::Null),
        }
    }
}

impl From<&str> for FieldPath {
    fn from(value: &str) -> Self {
        FieldPath::new(value)
    }
}

/// Aggregation function supported by the evaluator.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum AggregationFunc {
    Sum,
    Max,
    Min,
    Avg,
    Count,
}

/// Grouping specification for aggregations.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum GroupExpr {
    Field(FieldPath),
    Time(TimeGrain),
}

/// Aggregation specification describing metric computation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AggregationSpec {
    pub func: AggregationFunc,
    pub target: Option<ScalarExpr>,
    pub filter: Option<ConditionExpr>,
    pub group_by: Vec<GroupExpr>,
}

/// Metric definition referencing an aggregation spec.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricSpec {
    pub name: MetricName,
    pub aggregation: AggregationSpec,
}

/// Strongly typed metric name to avoid accidental mismatches.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MetricName(String);

impl MetricName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MetricName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&str> for MetricName {
    fn from(value: &str) -> Self {
        MetricName::new(value)
    }
}

impl From<String> for MetricName {
    fn from(value: String) -> Self {
        MetricName(value)
    }
}

/// Evaluate multiple metric specifications across the event slice.
pub fn evaluate_metrics(
    specs: &[MetricSpec],
    events: &[NormalizedEvent],
    query: &Query,
) -> EvalResult<BTreeMap<String, Value>> {
    let filtered_events = filter_events(events, query.time_window);
    let mut metrics = BTreeMap::new();
    for spec in specs {
        let value = evaluate_aggregation(&spec.aggregation, &filtered_events)?;
        metrics.insert(spec.name.to_string(), value);
    }
    Ok(metrics)
}

fn evaluate_aggregation(spec: &AggregationSpec, events: &[&NormalizedEvent]) -> EvalResult<Value> {
    let mut buckets: BTreeMap<String, BucketState> = BTreeMap::new();

    for event in events {
        if let Some(filter) = &spec.filter {
            if !filter.evaluate(event)? {
                continue;
            }
        }

        let key = aggregation_key(event, &spec.group_by)?;
        let bucket = buckets
            .entry(key)
            .or_insert_with(|| BucketState::new(spec.func));

        let target_value = match &spec.func {
            AggregationFunc::Count => None,
            _ => {
                let expr = spec.target.as_ref().ok_or(EvalError::TypeMismatch(
                    "aggregation target required for non-count metrics",
                ))?;
                expr.evaluate(event)?.as_f64()
            }
        };

        bucket.update(spec.func, target_value)?;
    }

    if buckets.is_empty() && !spec.has_grouping() {
        buckets.insert("__total__".into(), BucketState::new(spec.func));
    }

    let mut result_object = Map::new();
    for (key, bucket) in buckets.into_iter() {
        result_object.insert(key, bucket.finalize(spec.func));
    }

    if result_object.len() == 1 && !spec.has_grouping() {
        Ok(match result_object.into_values().next() {
            Some(value) => value,
            None => Value::Null,
        })
    } else {
        Ok(Value::Object(result_object))
    }
}

fn filter_events(events: &[NormalizedEvent], window: Option<TimeWindow>) -> Vec<&NormalizedEvent> {
    match window {
        Some(window) => events
            .iter()
            .filter(|event| window.contains(event.ts()))
            .collect(),
        None => events.iter().collect(),
    }
}

fn aggregation_key(event: &NormalizedEvent, groups: &[GroupExpr]) -> EvalResult<String> {
    if groups.is_empty() {
        Ok("__total__".into())
    } else {
        let mut parts = Vec::with_capacity(groups.len());
        for expr in groups {
            let value = match expr {
                GroupExpr::Field(path) => path.evaluate(event)?.to_json(),
                GroupExpr::Time(grain) => Value::String(time_bucket(event.ts(), *grain)),
            };
            parts.push(value_to_key(value));
        }
        Ok(parts.join("|"))
    }
}

fn value_to_key(value: Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v,
        other => other.to_string(),
    }
}

fn time_bucket(ts: Timestamp, grain: TimeGrain) -> String {
    if matches!(grain, TimeGrain::AllTime) {
        "all_time".into()
    } else {
        let nanos = ts.as_millis() as i128 * 1_000_000;
        let dt = match OffsetDateTime::from_unix_timestamp_nanos(nanos) {
            Ok(value) => value,
            Err(_) => OffsetDateTime::UNIX_EPOCH,
        };
        match grain {
            TimeGrain::Day => dt.date().to_string(),
            TimeGrain::Week => format!("{}-W{:02}", dt.year(), dt.iso_week()),
            TimeGrain::Month => format!("{}-{:02}", dt.year(), dt.month() as u8),
            TimeGrain::Quarter => {
                let quarter = ((dt.month() as u8 - 1) / 3) + 1;
                format!("{}-Q{}", dt.year(), quarter)
            }
            TimeGrain::Year => format!("{}", dt.year()),
            TimeGrain::AllTime => "all_time".into(),
            TimeGrain::Custom => dt.date().to_string(),
        }
    }
}

impl AggregationSpec {
    fn has_grouping(&self) -> bool {
        !self.group_by.is_empty()
    }
}

#[derive(Debug)]
enum BucketState {
    Count(usize),
    Number {
        count: usize,
        sum: f64,
        max: Option<f64>,
        min: Option<f64>,
    },
}

impl BucketState {
    fn round_metric_value(value: f64) -> f64 {
        let rounded = (value * 100.0).round() / 100.0;
        if rounded == -0.0 {
            0.0
        } else {
            rounded
        }
    }

    fn new(func: AggregationFunc) -> Self {
        match func {
            AggregationFunc::Count => BucketState::Count(0),
            _ => BucketState::Number {
                count: 0,
                sum: 0.0,
                max: None,
                min: None,
            },
        }
    }

    fn update(&mut self, func: AggregationFunc, value: Option<f64>) -> EvalResult<()> {
        match (self, func) {
            (BucketState::Count(count), AggregationFunc::Count) => {
                *count += 1;
            }
            (
                BucketState::Number {
                    count,
                    sum,
                    max,
                    min,
                },
                _,
            ) => {
                if let Some(v) = value {
                    *count += 1;
                    *sum += v;
                    *max = Some(max.map_or(v, |current| current.max(v)));
                    *min = Some(min.map_or(v, |current| current.min(v)));
                } else {
                    Ok(())?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn finalize(self, func: AggregationFunc) -> Value {
        match (self, func) {
            (BucketState::Count(count), AggregationFunc::Count) => json!(count),
            (BucketState::Number { sum, .. }, AggregationFunc::Sum) => {
                json!(Self::round_metric_value(sum))
            }
            (BucketState::Number { count, sum, .. }, AggregationFunc::Avg) => {
                if count == 0 {
                    Value::Null
                } else {
                    json!(Self::round_metric_value(sum / count as f64))
                }
            }
            (BucketState::Number { max, .. }, AggregationFunc::Max) => {
                max.map_or(Value::Null, |v| json!(Self::round_metric_value(v)))
            }
            (BucketState::Number { min, .. }, AggregationFunc::Min) => {
                min.map_or(Value::Null, |v| json!(Self::round_metric_value(v)))
            }
            (BucketState::Number { .. }, AggregationFunc::Count) | (BucketState::Count(_), _) => {
                Value::Null
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tracen_ir::{EventId, NormalizedEvent, Timestamp, TrackerId};

    fn sample_event(payload: Value, ts: i64) -> NormalizedEvent {
        NormalizedEvent::new(
            EventId::new(format!("event-{ts}")),
            TrackerId::new("sample"),
            Timestamp::new(ts),
            payload,
            json!({}),
        )
    }

    #[test]
    fn evaluates_binary_expression() {
        let event = sample_event(json!({"value_a": 80, "value_b": 5}), 0);
        let expr = ScalarExpr::Binary {
            op: BinaryOp::Mul,
            left: Box::new(ScalarExpr::Field(FieldPath::from("payload.value_a"))),
            right: Box::new(ScalarExpr::Field(FieldPath::from("payload.value_b"))),
        };

        let result = expr
            .evaluate(&event)
            .expect("expression evaluation succeeds");
        assert_eq!(result, ScalarValue::Number(400.0));
    }

    #[test]
    fn missing_fields_evaluate_to_null_instead_of_erroring() {
        let event = sample_event(json!({"value_a": 80}), 0);
        let expr = ScalarExpr::Field(FieldPath::from("payload.missing"));

        let result = expr.evaluate(&event).expect("missing field yields null");
        assert_eq!(result, ScalarValue::Null);
    }

    #[test]
    fn comparisons_against_missing_fields_are_false() {
        let event = sample_event(json!({"value_a": 80}), 0);
        let condition = ConditionExpr::Gt(
            Box::new(ScalarExpr::Field(FieldPath::from("payload.missing"))),
            Box::new(ScalarExpr::Number(0.0)),
        );

        assert!(!condition.evaluate(&event).expect("comparison succeeds"));
    }

    #[test]
    fn aggregates_sum_with_group_by() {
        let events = [
            sample_event(json!({"group_key": "segment_a", "value_a": 80}), 1_000),
            sample_event(json!({"group_key": "segment_a", "value_a": 85}), 2_000),
            sample_event(json!({"group_key": "segment_b", "value_a": 120}), 3_000),
        ];

        let spec = AggregationSpec {
            func: AggregationFunc::Sum,
            target: Some(ScalarExpr::Field(FieldPath::from("payload.value_a"))),
            filter: None,
            group_by: vec![GroupExpr::Field(FieldPath::from("payload.group_key"))],
        };

        let results = evaluate_aggregation(&spec, &events.iter().collect::<Vec<_>>())
            .expect("aggregation should succeed");
        let map = results
            .as_object()
            .expect("expect object when grouping by field");
        assert_eq!(
            map.get("segment_a").expect("segment_a group"),
            &json!(165.0)
        );
        assert_eq!(
            map.get("segment_b").expect("segment_b group"),
            &json!(120.0)
        );
    }

    #[test]
    fn rounds_aggregations_to_two_decimals() {
        let events = [
            sample_event(json!({"group_key": "segment_a", "value_a": 1.0}), 1_000),
            sample_event(json!({"group_key": "segment_a", "value_a": 2.0}), 2_000),
            sample_event(json!({"group_key": "segment_a", "value_a": 2.0}), 3_000),
        ];

        let avg_spec = AggregationSpec {
            func: AggregationFunc::Avg,
            target: Some(ScalarExpr::Field(FieldPath::from("payload.value_a"))),
            filter: None,
            group_by: vec![GroupExpr::Field(FieldPath::from("payload.group_key"))],
        };
        let avg = evaluate_aggregation(&avg_spec, &events.iter().collect::<Vec<_>>())
            .expect("avg aggregation should succeed");
        let avg_map = avg.as_object().expect("grouped avg result");
        assert_eq!(avg_map.get("segment_a"), Some(&json!(1.67)));

        let sum_spec = AggregationSpec {
            func: AggregationFunc::Sum,
            target: Some(ScalarExpr::Field(FieldPath::from("payload.value_a"))),
            filter: None,
            group_by: vec![],
        };
        let sum = evaluate_aggregation(&sum_spec, &events.iter().collect::<Vec<_>>())
            .expect("sum aggregation should succeed");
        assert_eq!(sum, json!(5.0));
    }

    #[test]
    fn counts_events_by_day() {
        let day_ms = 24 * 60 * 60 * 1_000;
        let events = [
            sample_event(json!({}), 0),
            sample_event(json!({}), day_ms),
            sample_event(json!({}), day_ms + 10),
        ];

        let spec = AggregationSpec {
            func: AggregationFunc::Count,
            target: None,
            filter: None,
            group_by: vec![GroupExpr::Time(TimeGrain::Day)],
        };

        let results = evaluate_aggregation(&spec, &events.iter().collect::<Vec<_>>())
            .expect("aggregation succeeds");
        assert!(results.is_object());
    }
}
