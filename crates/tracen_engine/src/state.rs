//! Validated incremental state and its untrusted snapshot restoration boundary.
use serde::{Deserialize, Serialize};
use tracen_ir::{NormalizedEvent, TrackerDefinition, TrackerId};

use crate::{apply, ensure_tracker, EngineError};

/// Incremental state bound to an immutable definition. Insert using [`apply`]
/// and restore serialized state using [`restore_state`]. Raw insertion and
/// unchecked deserialization are intentionally unavailable.
///
/// ```compile_fail
/// use tracen_engine::{compile_tracker, EngineState, validate_event};
/// let def = compile_tracker("tracker \"sample\" v1 { fields { amount: float } }").unwrap();
/// let mut state = EngineState::new(&def);
/// let event = validate_event(&def, r#"{"event_id":"e","ts":1,"payload":{"amount":1}}"#).unwrap();
/// state.push(event);
/// ```
///
/// ```compile_fail
/// let state: tracen_engine::EngineState<'_> = serde_json::from_str("{}").unwrap();
/// ```
#[derive(Clone, Debug, Serialize)]
pub struct EngineState<'a> {
    #[serde(skip)]
    definition: &'a TrackerDefinition,
    tracker_id: TrackerId,
    events: Vec<NormalizedEvent>,
}

impl<'a> EngineState<'a> {
    pub fn new(definition: &'a TrackerDefinition) -> Self {
        Self {
            definition,
            tracker_id: definition.tracker_id().clone(),
            events: Vec::new(),
        }
    }

    pub fn for_definition(definition: &'a TrackerDefinition) -> Self {
        Self::new(definition)
    }

    pub fn tracker_id(&self) -> &TrackerId {
        &self.tracker_id
    }

    pub fn total_events(&self) -> usize {
        self.events.len()
    }

    pub fn events(&self) -> &[NormalizedEvent] {
        &self.events
    }

    pub(super) fn definition(&self) -> &TrackerDefinition {
        self.definition
    }

    pub(super) fn push(&mut self, event: NormalizedEvent) {
        self.events.push(event);
    }
}

/// Validate a whole stored snapshot before returning usable state. Stored
/// derived values are discarded and recomputed under the supplied definition.
/// A failed restore cannot mutate any existing state.
pub fn restore_state<'a>(
    definition: &'a TrackerDefinition,
    state_json: &str,
) -> Result<EngineState<'a>, EngineError> {
    #[derive(Deserialize)]
    struct StoredState {
        tracker_id: TrackerId,
        events: Vec<NormalizedEvent>,
    }
    let stored: StoredState = serde_json::from_str(state_json).map_err(|error| {
        EngineError::EventValidation(format!("invalid state snapshot: {error}"))
    })?;
    ensure_tracker(definition, &stored.tracker_id)?;
    let mut state = EngineState::new(definition);
    for mut event in stored.events {
        if let Some(payload) = event.payload_mut().as_object_mut() {
            for derive in definition.derives() {
                payload.remove(&derive.name);
            }
        }
        apply(definition, &mut state, event)?;
    }
    Ok(state)
}
