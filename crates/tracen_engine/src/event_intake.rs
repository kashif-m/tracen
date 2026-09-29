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
    let mut event =
        NormalizedEvent::new(event_id, definition.tracker_id().clone(), ts, payload, meta);
    validate_normalized_event(definition, &mut event, payload_policy)?;
    Ok(event)
}

pub(crate) fn validate_normalized_event(
    definition: &TrackerDefinition,
    event: &mut NormalizedEvent,
    payload_policy: PayloadValidationPolicy,
) -> Result<(), EngineError> {
    ensure_tracker_id(definition, event.tracker_id().clone())?;
    if event.event_id().as_str().trim().is_empty() {
        return Err(EngineError::EventValidation("event_id is required".into()));
    }
    if !event.meta().is_object() {
        return Err(EngineError::EventValidation(
            "meta must be a JSON object".into(),
        ));
    }
    validate_payload_fields(definition.fields(), event.payload_mut(), payload_policy)
        .map_err(payload_validation_error)?;
    if matches!(
        payload_policy,
        PayloadValidationPolicy::Event | PayloadValidationPolicy::EventLax
    ) {
        for rule in definition.validations() {
            let valid = crate::eval_condition(&rule.condition, event, &Default::default())
                .map_err(|error| {
                    EngineError::EventValidation(format!("validation '{}': {error}", rule.name))
                })?;
            if !valid {
                return Err(EngineError::EventValidation(format!(
                    "validation '{}' failed",
                    rule.name
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn build_pack_event(
    definition: &TrackerDefinition,
    event_id: EventId,
    ts: Timestamp,
    payload: Value,
    meta: Value,
) -> Result<NormalizedEvent, EngineError> {
    build_event_from_parts(
        definition,
        event_id,
        ts,
        payload,
        meta,
        PayloadValidationPolicy::PackQueryLax,
    )
}

fn payload_validation_error(error: PayloadValidationError) -> EngineError {
    EngineError::EventValidation(error.to_string())
}
