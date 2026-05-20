use serde_json::Value;

use crate::EngineError;
use tracen_ir::{
    schema_validation::{validate_payload_fields, PayloadValidationError, PayloadValidationPolicy},
    EventId, NormalizedEvent, Timestamp, TrackerDefinition, TrackerId,
};

fn ensure_object(value: Option<&Value>, label: &str) -> Result<Value, EngineError> {
    match value {
        Some(Value::Object(map)) => Ok(Value::Object(map.clone())),
        Some(Value::Null) | None => Ok(Value::Object(Default::default())),
        _ => Err(EngineError::EventValidation(format!(
            "{label} must be a JSON object"
        ))),
    }
}

fn ensure_tracker_id(
    definition: &TrackerDefinition,
    tracker_id: TrackerId,
) -> Result<(), EngineError> {
    if tracker_id == *definition.tracker_id() {
        Ok(())
    } else {
        Err(EngineError::TrackerMismatch {
            expected: definition.tracker_id().clone(),
            actual: tracker_id,
        })
    }
}

fn normalize_payload(
    definition: &TrackerDefinition,
    payload: Value,
    policy: PayloadValidationPolicy,
) -> Result<Value, EngineError> {
    let mut payload = payload;
    validate_payload_fields(definition.fields(), &mut payload, policy)
        .map_err(payload_validation_error)?;
    Ok(payload)
}

pub(crate) fn parse_event_from_json(
    definition: &TrackerDefinition,
    event_json: &str,
    payload_policy: PayloadValidationPolicy,
) -> Result<NormalizedEvent, EngineError> {
    let value: Value = serde_json::from_str(event_json)
        .map_err(|err| EngineError::EventValidation(err.to_string()))?;

    let event_id = value
        .get("event_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| EngineError::EventValidation("event_id is required".into()))?;

    let ts = value
        .get("ts")
        .and_then(Value::as_i64)
        .ok_or_else(|| EngineError::EventValidation("ts must be an integer timestamp".into()))?;

    let tracker_id = value
        .get("tracker_id")
        .and_then(Value::as_str)
        .map(TrackerId::new)
        .unwrap_or_else(|| definition.tracker_id().clone());
    ensure_tracker_id(definition, tracker_id.clone())?;

    let payload = ensure_object(value.get("payload"), "payload")?;
    let meta = ensure_object(value.get("meta"), "meta")?;

    build_event_from_parts(
        definition,
        EventId::new(event_id),
        Timestamp::new(ts),
        payload,
        meta,
        payload_policy,
    )
}

pub(crate) fn build_event_from_parts(
    definition: &TrackerDefinition,
    event_id: EventId,
    ts: Timestamp,
    payload: Value,
    meta: Value,
    payload_policy: PayloadValidationPolicy,
) -> Result<NormalizedEvent, EngineError> {
    let normalized_payload = normalize_payload(definition, payload, payload_policy)?;
    Ok(NormalizedEvent::new(
        event_id,
        definition.tracker_id().clone(),
        ts,
        normalized_payload,
        meta,
    ))
}

pub(crate) fn build_pack_event(
    definition: &TrackerDefinition,
    event_id: EventId,
    ts: Timestamp,
    payload: Value,
) -> Result<NormalizedEvent, EngineError> {
    build_event_from_parts(
        definition,
        event_id,
        ts,
        payload,
        serde_json::json!({}),
        PayloadValidationPolicy::PackQueryLax,
    )
}

fn payload_validation_error(error: PayloadValidationError) -> EngineError {
    EngineError::EventValidation(error.to_string())
}
