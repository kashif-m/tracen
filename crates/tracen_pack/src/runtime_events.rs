use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracen_engine::EngineError;
use tracen_ir::{NormalizedEvent, TrackerDefinition};

use crate::{catalog_references, PackError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackInputEvent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracker_id: Option<String>,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub meta: Value,
    pub ts: i64,
    pub payload: Value,
}

/// Read routes may accept legacy payload fields, but never an invalid envelope.
pub(crate) fn validate_pack_event_envelope(
    definition: &TrackerDefinition,
    event: &PackInputEvent,
) -> Result<(), PackError> {
    if let Some(tracker_id) = &event.tracker_id {
        if tracker_id != definition.tracker_id().as_str() {
            return Err(to_pack_event_error(EngineError::TrackerMismatch {
                expected: definition.tracker_id().clone(),
                actual: tracen_ir::TrackerId::new(tracker_id.clone()),
            }));
        }
    }
    if event
        .event_id
        .as_ref()
        .is_some_and(|id| id.trim().is_empty())
    {
        return Err(PackError::Event("event_id must not be empty".into()));
    }
    if !event.meta.is_null() && !event.meta.is_object() {
        return Err(PackError::Event("meta must be a JSON object".into()));
    }
    if !event.payload.is_object() {
        return Err(PackError::Event("payload must be a JSON object".into()));
    }
    Ok(())
}

pub(crate) fn prepare_pack_events(
    definition: &TrackerDefinition,
    events: &[PackInputEvent],
) -> Result<Vec<PackInputEvent>, PackError> {
    let normalized = events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            validate_pack_event_envelope(definition, event)?;
            let fallback_id = format!("pack-{index}-{}", event.ts);
            let event_id = event.event_id.as_deref().unwrap_or(&fallback_id);
            tracen_engine::prepare_pack_event_with_metadata(
                definition,
                event_id,
                event.ts,
                event.payload.clone(),
                if event.meta.is_null() {
                    serde_json::json!({})
                } else {
                    event.meta.clone()
                },
            )
            .map_err(to_pack_event_error)
        })
        .collect::<Result<Vec<_>, PackError>>()?;
    let prepared = tracen_engine::prepare_events_for_compute(definition, &normalized)
        .map_err(to_pack_event_error)?;
    Ok(prepared
        .iter()
        .zip(events)
        .map(|(prepared, original)| PackInputEvent {
            payload: prepared.payload().clone(),
            ..original.clone()
        })
        .collect())
}

pub(crate) fn apply_runtime_time_semantics(
    events: &[PackInputEvent],
    offset_minutes: i32,
) -> Vec<PackInputEvent> {
    events
        .iter()
        .map(|event| {
            let mut payload = event.payload.clone();
            tracen_analytics::event_semantics::normalize_event_payload_buckets(
                &mut payload,
                event.ts,
                offset_minutes,
            );
            PackInputEvent {
                payload,
                ..event.clone()
            }
        })
        .collect()
}

pub(crate) fn to_pack_event_error(error: EngineError) -> PackError {
    PackError::Event(error.to_string())
}

pub(crate) fn validate_event_references(
    definition: &TrackerDefinition,
    event: &NormalizedEvent,
    catalog_json: &Value,
) -> Result<(), PackError> {
    catalog_references::validate_payload_references(definition, event.payload(), catalog_json)
}
