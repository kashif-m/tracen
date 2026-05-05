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
