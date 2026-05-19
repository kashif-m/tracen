use tracen_ir::{NormalizedEvent, Query, TimeWindow, Timestamp};

/// Temporal constraints applied during metric evaluation.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryConstraints {
    pub time_window: Option<TimeWindow>,
}

impl QueryConstraints {
    pub fn from_query(query: &Query) -> Self {
        Self {
            time_window: query.time_window,
        }
    }

    pub fn with_time_window(time_window: Option<TimeWindow>) -> Self {
        Self { time_window }
    }

    pub fn select_events<'a>(&self, events: &'a [NormalizedEvent]) -> Vec<&'a NormalizedEvent> {
        match self.time_window {
            Some(window) => events.iter().filter(|event| window.contains(event.ts())).collect(),
            None => events.iter().collect(),
        }
    }

    pub fn contains(&self, ts: Timestamp) -> bool {
        match self.time_window {
            Some(window) => window.contains(ts),
            None => true,
        }
    }

}
