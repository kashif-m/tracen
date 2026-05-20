use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracen_engine::EngineError;
use tracen_ir::{NormalizedEvent, TrackerDefinition};

use crate::{catalog_references, PackError};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackInputEvent {
    pub ts: i64,
    pub payload: Value,
}

pub(crate) fn prepare_pack_events(
    definition: &TrackerDefinition,
    events: &[PackInputEvent],
) -> Result<Vec<PackInputEvent>, PackError> {
    let normalized = events
        .iter()
        .enumerate()
        .map(|(index, event)| {
            let event_id = format!("pack-{index}-{}", event.ts);
            tracen_engine::prepare_pack_event(
                definition,
                &event_id,
                event.ts,
                event.payload.clone(),
            )
            .map_err(to_pack_event_error)
        })
        .collect::<Result<Vec<_>, PackError>>()?;
    let prepared = tracen_engine::prepare_events_for_compute(definition, &normalized)
        .map_err(to_pack_event_error)?;
    Ok(prepared
        .into_iter()
        .map(|event| PackInputEvent {
            ts: event.ts().as_millis(),
            payload: event.payload().clone(),
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
                ts: event.ts,
                payload,
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
