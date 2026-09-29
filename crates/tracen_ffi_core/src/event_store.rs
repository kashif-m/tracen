//! Indexed, transactional storage for native hosts. Tracker semantics stay in the
//! pack/engine: producers publish through accepted commands and a native Adapter.
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, path::Path, time::Duration};
pub use tracen_export::GenericEventRecord;

pub mod commands;
pub mod migration;

/// Producers publish through [`Self::execute_command`]. Migration callbacks are
/// trusted native storage machinery, not a bridge-accessible producer interface.
/// ```compile_fail
/// use tracen_ffi_core::event_store::EventStore;
/// let mut store = EventStore::open(std::path::Path::new(":memory:")).unwrap();
/// store.commit(None, "chosen", &[], &[], &Default::default(), false);
/// ```
pub struct EventStore(Connection);

/// A host-selected time key, separate from the original event timestamp.
/// For example a tracker may explicitly assign an event to a recorded day.
/// All queries against this store use this same host-defined time policy.
#[derive(Debug, Clone)]
pub struct IndexedEvent {
    pub event: GenericEventRecord,
    pub indexed_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventCursor {
    pub revision: String,
    pub tracker_id: String,
    pub start: i64,
    pub end: i64,
    pub ts: i64,
    pub event_id: String,
}

#[derive(Debug, Serialize)]
pub struct EventPage {
    pub revision: String,
    pub events: Vec<GenericEventRecord>,
    pub next: Option<EventCursor>,
}

fn error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

impl EventStore {
    pub fn open(path: &Path) -> Result<Self, String> {
        let connection = Connection::open(path).map_err(error)?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .map_err(error)?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(error)?;
        connection
            .pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        let version: i64 = connection
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .map_err(error)?;
        if version > 3 {
            return Err("event store schema is newer than this host".into());
        }
        if version < 3 {
            // Another connection may finish migration while this one waits.
            connection.execute_batch("BEGIN IMMEDIATE").map_err(error)?;
            let version: i64 = connection
                .pragma_query_value(None, "user_version", |row| row.get(0))
                .map_err(error)?;
            if version > 3 {
                return Err("event store schema is newer than this host".into());
            }
            if version == 0 {
                connection.execute_batch(
            "             CREATE TABLE IF NOT EXISTS store_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS store_events (
                 event_id TEXT PRIMARY KEY, tracker_id TEXT NOT NULL, ts INTEGER NOT NULL, indexed_at INTEGER NOT NULL,
                 payload TEXT NOT NULL, meta TEXT NOT NULL);
             CREATE INDEX IF NOT EXISTS store_events_by_time ON store_events(tracker_id, indexed_at, event_id);
             PRAGMA user_version=1;
             "
        ).map_err(error)?;
            }
            if version < 2 {
                connection.execute_batch(
                "                 INSERT OR IGNORE INTO store_state(key,value) VALUES('_store_id',json_quote(lower(hex(randomblob(16)))));
                 INSERT OR IGNORE INTO store_state(key,value) VALUES('_revision_counter','0');
                 CREATE TABLE IF NOT EXISTS store_operations (
                     tracker_id TEXT NOT NULL, operation_id TEXT NOT NULL, intent TEXT NOT NULL,
                     status TEXT NOT NULL CHECK(status IN ('pending','accepted','rejected')),
                     expected_revision TEXT, result TEXT, revision TEXT,
                     PRIMARY KEY(tracker_id,operation_id));
                 CREATE INDEX IF NOT EXISTS store_pending_operations ON store_operations(tracker_id,operation_id) WHERE status='pending';
                 PRAGMA user_version=2;
                 "
            ).map_err(error)?;
            }
            if version < 3 {
                connection.execute_batch("                ALTER TABLE store_operations ADD COLUMN delivered INTEGER NOT NULL DEFAULT 0 CHECK(delivered IN (0,1));
                CREATE INDEX store_undelivered_operations ON store_operations(tracker_id,operation_id) WHERE delivered=0;
                PRAGMA user_version=3;
                ").map_err(error)?;
            }
            connection.execute_batch("COMMIT").map_err(error)?;
        }
        Ok(Self(connection))
    }

    pub fn revision(&self) -> Result<Option<String>, String> {
        self.0
            .query_row(
                "SELECT value FROM store_state WHERE key='revision'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(error)
    }

    pub fn tracker_ids(&self) -> Result<Vec<String>, String> {
        let mut statement = self
            .0
            .prepare("SELECT DISTINCT tracker_id FROM store_events ORDER BY tracker_id")
            .map_err(error)?;
        let rows = statement.query_map([], |row| row.get(0)).map_err(error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(error)
    }

    /// Read only requested metadata; startup must not pull receipts or history.
    /// Revision and values are taken from the same SQLite read transaction.
    pub fn metadata(
        &mut self,
        keys: &[String],
    ) -> Result<(Option<String>, BTreeMap<String, Value>), String> {
        let tx = self.0.transaction().map_err(error)?;
        let revision = tx
            .query_row(
                "SELECT value FROM store_state WHERE key='revision'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(error)?;
        let mut values = BTreeMap::new();
        for key in keys {
            if key == "revision" {
                return Err("revision is reserved".into());
            }
            let raw: Option<String> = tx
                .query_row("SELECT value FROM store_state WHERE key=?1", [key], |row| {
                    row.get(0)
                })
                .optional()
                .map_err(error)?;
            if let Some(raw) = raw {
                values.insert(key.clone(), serde_json::from_str(&raw).map_err(error)?);
            }
        }
        tx.commit().map_err(error)?;
        Ok((revision, values))
    }

    /// Cursor continuation is tied to both the accepted revision and exact range.
    /// A concurrent write requires restarting the query, never mixing two totals.
    pub fn page(
        &mut self,
        tracker_id: &str,
        start: i64,
        end: i64,
        limit: usize,
        cursor: Option<&EventCursor>,
        expected_revision: &str,
    ) -> Result<EventPage, String> {
        if tracker_id.is_empty() || start > end || !(1..=512).contains(&limit) {
            return Err("invalid event page range or limit".into());
        }
        if cursor.is_some_and(|c| {
            c.revision != expected_revision
                || c.tracker_id != tracker_id
                || c.start != start
                || c.end != end
        }) {
            return Err("event cursor does not belong to this query".into());
        }
        let tx = self.0.transaction().map_err(error)?;
        let revision_json: Option<String> = tx
            .query_row(
                "SELECT value FROM store_state WHERE key='_events_revision'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(error)?;
        let revision: Option<String> = revision_json
            .map(|raw| serde_json::from_str(&raw).map_err(error))
            .transpose()?;
        if revision.as_deref() != Some(expected_revision) {
            return Err("event store revision conflict".into());
        }
        // Separate first/continuation statements keep both paths as indexed seeks.
        let sql = if cursor.is_some() {
            "SELECT event_id,tracker_id,ts,payload,meta,indexed_at FROM store_events WHERE tracker_id=?1 AND indexed_at>=?2 AND indexed_at<=?3 AND (indexed_at,event_id)>(?5,?6) ORDER BY indexed_at,event_id LIMIT ?4"
        } else {
            "SELECT event_id,tracker_id,ts,payload,meta,indexed_at FROM store_events WHERE tracker_id=?1 AND indexed_at>=?2 AND indexed_at<=?3 ORDER BY indexed_at,event_id LIMIT ?4"
        };
        let mut statement = tx.prepare(sql).map_err(error)?;
        let mut rows = match cursor {
            Some(c) => statement.query(params![
                tracker_id,
                start,
                end,
                (limit + 1) as i64,
                c.ts,
                c.event_id
            ]),
            None => statement.query(params![tracker_id, start, end, (limit + 1) as i64]),
        }
        .map_err(error)?;
        let mut events = Vec::with_capacity(limit + 1);
        let mut last_indexed_at = 0;
        while let Some(row) = rows.next().map_err(error)? {
            if events.len() < limit {
                last_indexed_at = row.get(5).map_err(error)?;
            }
            let payload: String = row.get(3).map_err(error)?;
            let meta: String = row.get(4).map_err(error)?;
            events.push(GenericEventRecord {
                event_id: row.get(0).map_err(error)?,
                tracker_id: row.get(1).map_err(error)?,
                ts: row.get(2).map_err(error)?,
                payload: serde_json::from_str(&payload).map_err(error)?,
                meta: serde_json::from_str(&meta).map_err(error)?,
            });
        }
        let next = if events.len() > limit {
            events.pop();
            let last = events.last().expect("positive page limit");
            Some(EventCursor {
                revision: expected_revision.into(),
                tracker_id: tracker_id.into(),
                start,
                end,
                ts: last_indexed_at,
                event_id: last.event_id.clone(),
            })
        } else {
            None
        };
        drop(rows);
        drop(statement);
        tx.commit().map_err(error)?;
        Ok(EventPage {
            revision: expected_revision.into(),
            events,
            next,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use commands::{Command, CommandAdapter, CommandChange, CommandError};
    use serde_json::json;

    fn event(id: &str, ts: i64) -> IndexedEvent {
        IndexedEvent {
            indexed_at: ts,
            event: GenericEventRecord {
                event_id: id.into(),
                tracker_id: "hydration".into(),
                ts,
                payload: json!({"ml":250}),
                meta: json!({"notes":null}),
            },
        }
    }
    fn seed(store: &mut EventStore, events: &[IndexedEvent]) -> String {
        store
            .migrate("test-fixture", |tx| {
                for e in events {
                    let r = &e.event;
                    tx.execute(
                        "INSERT INTO store_events(event_id,tracker_id,ts,payload,meta,indexed_at) VALUES(?1,?2,?3,?4,?5,?6)",
                        params![
                            r.event_id,
                            r.tracker_id,
                            r.ts,
                            r.payload.to_string(),
                            r.meta.to_string(),
                            e.indexed_at
                        ],
                    )
                    .map_err(error)?;
                }
                Ok(())
            })
            .unwrap();
        store.revision().unwrap().unwrap()
    }
    struct Settings;
    impl CommandAdapter for Settings {
        fn plan(
            &self,
            _: &rusqlite::Transaction<'_>,
            _: &Command,
        ) -> Result<CommandChange, CommandError> {
            Ok(CommandChange {
                metadata: BTreeMap::from([("settings".into(), json!({"unit":"oz"}))]),
                ..Default::default()
            })
        }
        fn normalize(
            &self,
            _: &rusqlite::Transaction<'_>,
            _: &BTreeMap<String, Value>,
            _: &IndexedEvent,
        ) -> Result<IndexedEvent, CommandError> {
            unreachable!()
        }
        fn update_projections(
            &self,
            _: &rusqlite::Transaction<'_>,
            _: &CommandChange,
        ) -> Result<(), CommandError> {
            Ok(())
        }
    }
    #[test]
    fn pages_preserve_ties_ranges_and_reject_revision_changes() {
        let mut store = EventStore::open(Path::new(":memory:")).unwrap();
        let revision = seed(
            &mut store,
            &[
                event("c", 10),
                event("a", 10),
                event("b", 10),
                event("old", 0),
            ],
        );
        let first = store.page("hydration", 10, 20, 2, None, &revision).unwrap();
        assert_eq!(
            first
                .events
                .iter()
                .map(|e| e.event_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        let second = store
            .page("hydration", 10, 20, 2, first.next.as_ref(), &revision)
            .unwrap();
        assert_eq!(second.events[0].event_id, "c");
        assert!(second.next.is_none());
        assert!(store
            .page("hydration", 0, 20, 2, first.next.as_ref(), &revision)
            .is_err());
        store
            .migrate("delete-fixture-row", |tx| {
                tx.execute("DELETE FROM store_events WHERE event_id='a'", [])
                    .map_err(error)?;
                Ok(())
            })
            .unwrap();
        assert!(store
            .page("hydration", 10, 20, 2, first.next.as_ref(), &revision)
            .is_err());
        let current = store.revision().unwrap().unwrap();
        assert_eq!(
            store
                .page("hydration", 10, 20, 2, None, &current)
                .unwrap()
                .events
                .len(),
            2
        );
        assert!(store
            .page("hydration", 10, 20, 513, None, &current)
            .is_err());
    }
    #[test]
    fn auxiliary_command_does_not_invalidate_event_cursor() {
        let mut store = EventStore::open(Path::new(":memory:")).unwrap();
        let revision = seed(&mut store, &[event("a", 1), event("b", 2)]);
        let first = store.page("hydration", 0, 10, 1, None, &revision).unwrap();
        let receipt = store
            .execute_command(
                &Command {
                    tracker_id: "hydration".into(),
                    operation_id: "settings".into(),
                    intent: json!({}),
                    expected_revision: Some(revision.clone()),
                },
                &Settings,
            )
            .unwrap();
        assert!(receipt.accepted);
        assert_eq!(receipt.events_revision.as_deref(), Some(revision.as_str()));
        assert_ne!(receipt.current_revision, receipt.events_revision);
        assert_eq!(
            store
                .page("hydration", 0, 10, 1, first.next.as_ref(), &revision)
                .unwrap()
                .events[0]
                .event_id,
            "b"
        );
    }
    #[test]
    fn host_time_key_preserves_original_timestamp_and_cursor_order() {
        let mut store = EventStore::open(Path::new(":memory:")).unwrap();
        let mut a = event("a", 9000);
        a.indexed_at = 10;
        let mut b = event("b", 8000);
        b.indexed_at = 10;
        let revision = seed(&mut store, &[a, b]);
        let first = store.page("hydration", 10, 10, 1, None, &revision).unwrap();
        assert_eq!(first.events[0].ts, 9000);
        assert_eq!(first.next.as_ref().unwrap().ts, 10);
        let next = store
            .page("hydration", 10, 10, 1, first.next.as_ref(), &revision)
            .unwrap();
        assert_eq!(next.events[0].ts, 8000);
        assert!(next.next.is_none());
        let plan:String=store.0.query_row("EXPLAIN QUERY PLAN SELECT event_id FROM store_events WHERE tracker_id='hydration' AND indexed_at>=0 AND indexed_at<=10 AND (indexed_at,event_id)>(1,'a') ORDER BY indexed_at,event_id LIMIT 10",[],|r|r.get(3)).unwrap();
        assert!(
            plan.contains("SEARCH") && plan.contains("store_events_by_time"),
            "{plan}"
        );
    }
}
