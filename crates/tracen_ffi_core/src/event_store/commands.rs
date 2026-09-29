//! Durable commands shared by native tracker adapters. All effects and projections
//! use the same transaction. Adapters are trusted native code, never JS callbacks.
use super::{EventStore, GenericEventRecord, IndexedEvent};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Command {
    pub tracker_id: String,
    pub operation_id: String,
    pub intent: Value,
    pub expected_revision: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum CommandError {
    Rejected(String),
    Conflict(String),
    Storage(String),
}
impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(s) | Self::Conflict(s) | Self::Storage(s) => f.write_str(s),
        }
    }
}
impl std::error::Error for CommandError {}
impl From<rusqlite::Error> for CommandError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Storage(e.to_string())
    }
}
impl From<serde_json::Error> for CommandError {
    fn from(e: serde_json::Error) -> Self {
        Self::Storage(e.to_string())
    }
}

#[derive(Default)]
pub struct CommandChange {
    pub events: Vec<IndexedEvent>,
    pub deleted: Vec<String>,
    pub metadata: BTreeMap<String, Value>,
    pub result: Value,
}

/// Trusted native integration seam. Planning must not mutate the transaction. Projection
/// writes are restricted by convention to the adapter's own tables. Neither hook
/// may commit, roll back, or recursively invoke the command coordinator.
pub trait CommandAdapter {
    fn plan(&self, tx: &Transaction<'_>, command: &Command) -> Result<CommandChange, CommandError>;
    /// Validate against prospective metadata/catalog, not a render-captured copy.
    fn normalize(
        &self,
        tx: &Transaction<'_>,
        metadata: &BTreeMap<String, Value>,
        event: &IndexedEvent,
    ) -> Result<IndexedEvent, CommandError>;
    fn update_projections(
        &self,
        tx: &Transaction<'_>,
        change: &CommandChange,
    ) -> Result<(), CommandError>;
    /// Refresh projections that require the newly accepted metadata and rows.
    /// Runs before the receipt commits; failure rolls back all command effects.
    fn finalize_projections(
        &self,
        _tx: &Transaction<'_>,
        _change: &CommandChange,
    ) -> Result<(), CommandError> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub accepted: bool,
    pub replayed: bool,
    pub store_id: String,
    pub operation_id: String,
    pub operation_revision: Option<String>,
    pub current_revision: Option<String>,
    pub events_revision: Option<String>,
    pub result: Value,
}

#[derive(Debug, Serialize)]
pub struct StoredOperation {
    pub command: Command,
    pub receipt: Option<CommandReceipt>,
}

pub fn metadata_value(connection: &Connection, key: &str) -> Result<Option<Value>, CommandError> {
    let raw: Option<String> = connection
        .query_row("SELECT value FROM store_state WHERE key=?1", [key], |r| {
            r.get(0)
        })
        .optional()?;
    raw.map(|raw| serde_json::from_str(&raw).map_err(Into::into))
        .transpose()
}

pub fn event_by_id(
    connection: &Connection,
    id: &str,
) -> Result<Option<GenericEventRecord>, CommandError> {
    let row: Option<(String, i64, String, String)> = connection
        .query_row(
            "SELECT tracker_id,ts,payload,meta FROM store_events WHERE event_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    row.map(|(tracker_id, ts, payload, meta)| {
        Ok(GenericEventRecord {
            event_id: id.into(),
            tracker_id,
            ts,
            payload: serde_json::from_str(&payload)?,
            meta: serde_json::from_str(&meta)?,
        })
    })
    .transpose()
}

fn revision(connection: &Connection) -> Result<Option<String>, CommandError> {
    Ok(connection
        .query_row(
            "SELECT value FROM store_state WHERE key='revision'",
            [],
            |r| r.get(0),
        )
        .optional()?)
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), canonical(v)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        value => value.clone(),
    }
}

fn reserved(key: &str) -> bool {
    key == "revision" || key.starts_with('_')
}

fn read_only<T>(
    tx: &Transaction<'_>,
    read: impl FnOnce() -> Result<T, CommandError>,
) -> Result<T, CommandError> {
    tx.pragma_update(None, "query_only", true)?;
    let result = read();
    tx.pragma_update(None, "query_only", false)?;
    result
}

pub(super) fn next_revision(tx: &Transaction<'_>) -> Result<String, CommandError> {
    let counter: i64 = tx.query_row(
        "SELECT CAST(value AS INTEGER) FROM store_state WHERE key='_revision_counter'",
        [],
        |r| r.get(0),
    )?;
    let next = counter
        .checked_add(1)
        .ok_or_else(|| CommandError::Storage("revision counter exhausted".into()))?;
    tx.execute(
        "UPDATE store_state SET value=?1 WHERE key='_revision_counter'",
        [next.to_string()],
    )?;
    let id = metadata_value(tx, "_store_id")?
        .and_then(|v| v.as_str().map(str::to_string))
        .ok_or_else(|| CommandError::Storage("missing database identity".into()))?;
    Ok(format!("{id}:{next}"))
}

fn receipt(
    tx: &Transaction<'_>,
    command: &Command,
    replayed: bool,
) -> Result<CommandReceipt, CommandError> {
    let (status,result,operation_revision): (String,String,Option<String>) = tx.query_row(
        "SELECT status,result,revision FROM store_operations WHERE tracker_id=?1 AND operation_id=?2 AND status!='pending'",
        params![command.tracker_id,command.operation_id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    Ok(CommandReceipt {
        accepted: status == "accepted",
        replayed,
        store_id: metadata_value(tx, "_store_id")?
            .and_then(|v| v.as_str().map(str::to_string))
            .ok_or_else(|| CommandError::Storage("missing database identity".into()))?,
        operation_id: command.operation_id.clone(),
        operation_revision,
        current_revision: revision(tx)?,
        events_revision: metadata_value(tx, "_events_revision")?
            .and_then(|v| v.as_str().map(str::to_string)),
        result: serde_json::from_str(&result)?,
    })
}

impl EventStore {
    /// Trusted native integration boundary: rebuild an adapter-owned projection
    /// atomically when its version changes.
    /// Source events are untouched; ordinary commands maintain the projection in
    /// their own transaction. The callback must not commit or modify source rows.
    pub fn prepare_projection(
        &mut self,
        name: &str,
        version: &str,
        build: impl FnOnce(&Transaction<'_>) -> Result<(), CommandError>,
    ) -> Result<(), CommandError> {
        if name.is_empty() || version.is_empty() {
            return Err(CommandError::Rejected(
                "projection identity required".into(),
            ));
        }
        let key = format!("_projection:{name}");
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if metadata_value(&tx, &key)?.as_ref() != Some(&Value::String(version.into())) {
            build(&tx)?;
            tx.execute("INSERT INTO store_state(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![key,serde_json::to_string(version)?])?;
        }
        tx.commit()?;
        Ok(())
    }
    /// Intent is durable before planning. Rejected commands retain a rejection
    /// receipt; storage failures remain pending for reconciliation.
    pub fn prepare_command(&mut self, command: &Command) -> Result<(), CommandError> {
        if command.tracker_id.trim().is_empty()
            || command.operation_id.trim().is_empty()
            || !command.intent.is_object()
        {
            return Err(CommandError::Rejected(
                "invalid command identity or intent".into(),
            ));
        }
        let intent = serde_json::to_string(&canonical(&command.intent))?;
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT intent FROM store_operations WHERE tracker_id=?1 AND operation_id=?2",
                params![command.tracker_id, command.operation_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing != intent {
                return Err(CommandError::Conflict(
                    "operation identity already has different intent".into(),
                ));
            }
        } else {
            tx.execute("INSERT INTO store_operations(tracker_id,operation_id,intent,status,expected_revision) VALUES(?1,?2,?3,'pending',?4)",
                params![command.tracker_id,command.operation_id,intent,command.expected_revision])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn pending_commands(
        &self,
        tracker_id: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Command>, CommandError> {
        if !(1..=512).contains(&limit) {
            return Err(CommandError::Rejected(
                "invalid pending command limit".into(),
            ));
        }
        let mut statement = self.0.prepare("SELECT operation_id,intent,expected_revision FROM store_operations WHERE tracker_id=?1 AND status='pending' AND operation_id>?2 ORDER BY operation_id LIMIT ?3")?;
        let rows = statement.query_map(params![tracker_id, after, limit as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (operation_id, intent, expected_revision) = row?;
            Ok(Command {
                tracker_id: tracker_id.into(),
                operation_id,
                intent: serde_json::from_str(&intent)?,
                expected_revision,
            })
        })
        .collect()
    }

    /// Run a bounded, adapter-owned projection read against one accepted snapshot.
    /// The trusted native callback must not modify storage.
    pub fn read_projection<T>(
        &mut self,
        expected_events_revision: &str,
        read: impl FnOnce(&Transaction<'_>) -> Result<T, CommandError>,
    ) -> Result<T, CommandError> {
        let tx = self.0.transaction()?;
        if metadata_value(&tx, "_events_revision")?
            .and_then(|v| v.as_str().map(str::to_string))
            .as_deref()
            != Some(expected_events_revision)
        {
            return Err(CommandError::Conflict(
                "event store revision conflict".into(),
            ));
        }
        let value = read_only(&tx, || read(&tx))?;
        tx.commit()?;
        Ok(value)
    }

    pub fn operation(
        &mut self,
        tracker_id: &str,
        operation_id: &str,
    ) -> Result<Option<StoredOperation>, CommandError> {
        let tx = self.0.transaction()?;
        let row:Option<(String,Option<String>,String)>=tx.query_row("SELECT intent,expected_revision,status FROM store_operations WHERE tracker_id=?1 AND operation_id=?2",params![tracker_id,operation_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
        let result = if let Some((intent, expected_revision, status)) = row {
            let command = Command {
                tracker_id: tracker_id.into(),
                operation_id: operation_id.into(),
                intent: serde_json::from_str(&intent)?,
                expected_revision,
            };
            let receipt = if status == "pending" {
                None
            } else {
                Some(receipt(&tx, &command, true)?)
            };
            Some(StoredOperation { command, receipt })
        } else {
            None
        };
        tx.commit()?;
        Ok(result)
    }

    /// Accepted-but-unacknowledged commands remain recoverable after lost replies.
    pub fn undelivered_commands(
        &self,
        tracker_id: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<Command>, CommandError> {
        if !(1..=512).contains(&limit) {
            return Err(CommandError::Rejected("invalid command limit".into()));
        }
        let mut statement = self.0.prepare("SELECT operation_id,intent,expected_revision FROM store_operations WHERE tracker_id=?1 AND delivered=0 AND operation_id>?2 ORDER BY operation_id LIMIT ?3")?;
        let rows = statement.query_map(params![tracker_id, after, limit as i64], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (operation_id, intent, expected_revision) = row?;
            Ok(Command {
                tracker_id: tracker_id.into(),
                operation_id,
                intent: serde_json::from_str(&intent)?,
                expected_revision,
            })
        })
        .collect()
    }

    /// Delivery does not change accepted data or prune the replay receipt.
    pub fn acknowledge_command(
        &mut self,
        tracker_id: &str,
        operation_id: &str,
    ) -> Result<(), CommandError> {
        let changed=self.0.execute("UPDATE store_operations SET delivered=1 WHERE tracker_id=?1 AND operation_id=?2 AND status!='pending'", params![tracker_id,operation_id])?;
        if changed != 1 {
            return Err(CommandError::Conflict(
                "command has no completed receipt".into(),
            ));
        }
        Ok(())
    }

    pub fn execute_command(
        &mut self,
        command: &Command,
        adapter: &impl CommandAdapter,
    ) -> Result<CommandReceipt, CommandError> {
        self.prepare_command(command)?;
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let status: String = tx.query_row(
            "SELECT status FROM store_operations WHERE tracker_id=?1 AND operation_id=?2",
            params![command.tracker_id, command.operation_id],
            |r| r.get(0),
        )?;
        if status != "pending" {
            let result = receipt(&tx, command, true)?;
            tx.commit()?;
            return Ok(result);
        }
        let expected: Option<String> = tx.query_row(
            "SELECT expected_revision FROM store_operations WHERE tracker_id=?1 AND operation_id=?2",
            params![command.tracker_id, command.operation_id], |r| r.get(0))?;
        if let Some(expected) = &expected {
            if revision(&tx)?.as_ref() != Some(expected) {
                tx.execute("UPDATE store_operations SET status='rejected',result=?3 WHERE tracker_id=?1 AND operation_id=?2",
                    params![command.tracker_id, command.operation_id, json!({"kind":"conflict","error":"accepted revision changed"}).to_string()])?;
                let result = receipt(&tx, command, false)?;
                tx.commit()?;
                return Ok(result);
            }
        }
        let prepared = read_only(&tx, || {
            let mut change = adapter.plan(&tx, command)?;
            if change.metadata.keys().any(|key| reserved(key)) {
                return Err(CommandError::Rejected("reserved metadata key".into()));
            }
            let mut seen = HashSet::new();
            for event in &mut change.events {
                let identity = (event.event.event_id.clone(), event.event.tracker_id.clone());
                *event = adapter.normalize(&tx, &change.metadata, event)?;
                if identity != (event.event.event_id.clone(), event.event.tracker_id.clone()) {
                    return Err(CommandError::Rejected(
                        "normalization changed event identity".into(),
                    ));
                }
                let raw = &event.event;
                if raw.event_id.trim().is_empty()
                    || raw.tracker_id != command.tracker_id
                    || !raw.payload.is_object()
                    || !(raw.meta.is_object() || raw.meta.is_null())
                    || !seen.insert(raw.event_id.clone())
                {
                    return Err(CommandError::Rejected(
                        "invalid or duplicate event envelope".into(),
                    ));
                }
            }
            for id in &change.deleted {
                if id.trim().is_empty() || !seen.insert(id.clone()) {
                    return Err(CommandError::Rejected(
                        "invalid, duplicate or conflicting deletion".into(),
                    ));
                }
            }
            for id in &seen {
                let owner: Option<String> = tx
                    .query_row(
                        "SELECT tracker_id FROM store_events WHERE event_id=?1",
                        [id],
                        |r| r.get(0),
                    )
                    .optional()?;
                if owner
                    .as_ref()
                    .is_some_and(|owner| owner != &command.tracker_id)
                {
                    return Err(CommandError::Rejected(
                        "event identity belongs to another tracker".into(),
                    ));
                }
            }
            Ok(change)
        });
        let change = match prepared {
            Ok(change) => change,
            Err(CommandError::Rejected(message)) => {
                tx.execute("UPDATE store_operations SET status='rejected',result=?3 WHERE tracker_id=?1 AND operation_id=?2",
                    params![command.tracker_id,command.operation_id,json!({"error":message}).to_string()])?;
                let result = receipt(&tx, command, false)?;
                tx.commit()?;
                return Ok(result);
            }
            Err(error) => return Err(error),
        };
        // Adapters observe old events while updating their own projection tables.
        // A later event/receipt failure rolls every projection back with the write.
        adapter.update_projections(&tx, &change)?;
        for id in &change.deleted {
            tx.execute("DELETE FROM store_events WHERE event_id=?1", [id])?;
        }
        for indexed in &change.events {
            let e = &indexed.event;
            tx.execute("INSERT INTO store_events(event_id,tracker_id,ts,payload,meta,indexed_at) VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(event_id) DO UPDATE SET ts=excluded.ts,payload=excluded.payload,meta=excluded.meta,indexed_at=excluded.indexed_at",
                params![e.event_id,e.tracker_id,e.ts,serde_json::to_string(&e.payload)?,serde_json::to_string(&e.meta)?,indexed.indexed_at])?;
        }
        for (key, value) in &change.metadata {
            tx.execute("INSERT INTO store_state(key,value) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value=excluded.value",params![key,serde_json::to_string(value)?])?;
        }
        adapter.finalize_projections(&tx, &change)?;
        let revision = next_revision(&tx)?;
        if !change.events.is_empty() || !change.deleted.is_empty() {
            tx.execute("INSERT INTO store_state(key,value) VALUES('_events_revision',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [serde_json::to_string(&revision)?])?;
        }
        tx.execute("INSERT INTO store_state(key,value) VALUES('revision',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value", [&revision])?;
        tx.execute("UPDATE store_operations SET status='accepted',result=?3,revision=?4 WHERE tracker_id=?1 AND operation_id=?2",params![command.tracker_id,command.operation_id,serde_json::to_string(&change.result)?,revision])?;
        let result = receipt(&tx, command, false)?;
        tx.commit()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    struct Database(PathBuf);
    impl Database {
        fn new() -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            Self(std::env::temp_dir().join(format!(
                    "tracen-command-{}-{}-{}.sqlite",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos(),
                    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                )))
        }
        fn open(&self) -> EventStore {
            EventStore::open(&self.0).unwrap()
        }
    }
    impl Drop for Database {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0.display()));
            }
        }
    }
    struct Fixture {
        fail_projection: bool,
    }
    impl CommandAdapter for Fixture {
        fn plan(&self, _: &Transaction<'_>, c: &Command) -> Result<CommandChange, CommandError> {
            let id = c.intent["id"].as_str().unwrap_or("one");
            Ok(CommandChange {
                events: if c.intent["delete"] == true {
                    vec![]
                } else {
                    vec![IndexedEvent {
                        indexed_at: 0,
                        event: GenericEventRecord {
                            event_id: id.into(),
                            tracker_id: c.tracker_id.clone(),
                            ts: 0,
                            payload: c.intent["payload"].clone(),
                            meta: json!({"source":"fixture"}),
                        },
                    }]
                },
                deleted: if c.intent["delete"] == true {
                    vec![id.into()]
                } else {
                    vec![]
                },
                metadata: BTreeMap::from([("accepted-auxiliary".into(), c.intent["aux"].clone())]),
                result: json!({"id":id}),
            })
        }
        fn normalize(
            &self,
            _: &Transaction<'_>,
            _: &BTreeMap<String, Value>,
            event: &IndexedEvent,
        ) -> Result<IndexedEvent, CommandError> {
            if event.event.payload["amount"]
                .as_i64()
                .is_none_or(|amount| amount <= 0)
            {
                return Err(CommandError::Rejected("positive amount required".into()));
            }
            Ok(event.clone())
        }
        fn update_projections(
            &self,
            tx: &Transaction<'_>,
            _: &CommandChange,
        ) -> Result<(), CommandError> {
            tx.execute_batch("CREATE TABLE IF NOT EXISTS fixture_projection (value INTEGER); INSERT INTO fixture_projection VALUES(1);")?;
            if self.fail_projection {
                return Err(CommandError::Storage("injected projection failure".into()));
            }
            Ok(())
        }
        fn finalize_projections(
            &self,
            tx: &Transaction<'_>,
            _: &CommandChange,
        ) -> Result<(), CommandError> {
            if metadata_value(tx, "accepted-auxiliary")?.is_some_and(|v| v["fail_finalize"] == true)
            {
                assert!(event_by_id(tx, "one")?.is_some());
                return Err(CommandError::Storage(
                    "injected finalization failure".into(),
                ));
            }
            Ok(())
        }
    }
    fn command(id: &str) -> Command {
        Command {
            tracker_id: "hydration".into(),
            operation_id: id.into(),
            intent: json!({"payload":{"amount":250},"aux":{"unit":"ml"}}),
            expected_revision: None,
        }
    }

    #[test]
    fn finalization_failure_rolls_back_rows_metadata_and_receipt() {
        let db = Database::new();
        let mut store = db.open();
        let mut c = command("finalization");
        c.intent["aux"] = json!({"fail_finalize":true});
        assert!(store
            .execute_command(
                &c,
                &Fixture {
                    fail_projection: false
                }
            )
            .is_err());
        drop(store);
        let mut store = db.open();
        assert!(event_by_id(&store.0, "one").unwrap().is_none());
        assert!(metadata_value(&store.0, "accepted-auxiliary")
            .unwrap()
            .is_none());
        assert!(store.revision().unwrap().is_none());
        let operation = store
            .operation("hydration", "finalization")
            .unwrap()
            .unwrap();
        assert!(operation.receipt.is_none());
        assert_eq!(operation.command.intent, c.intent);
    }

    #[test]
    fn disk_reopen_replay_keeps_current_state_after_edit_and_delete() {
        let db = Database::new();
        let adapter = Fixture {
            fail_projection: false,
        };
        let first = {
            let mut store = db.open();
            store.execute_command(&command("first"), &adapter).unwrap()
        };
        let mut store = db.open();
        assert_eq!(
            store
                .undelivered_commands("hydration", "", 512)
                .unwrap()
                .len(),
            1
        );
        store.acknowledge_command("hydration", "first").unwrap();
        assert!(store
            .undelivered_commands("hydration", "", 512)
            .unwrap()
            .is_empty());
        let mut edit = command("edit");
        edit.intent["payload"]["amount"] = json!(500);
        let edited = store.execute_command(&edit, &adapter).unwrap();
        assert_ne!(edited.current_revision, first.current_revision);
        let replay = store.execute_command(&command("first"), &adapter).unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.operation_revision, first.operation_revision);
        assert_eq!(replay.current_revision, edited.current_revision);
        assert_eq!(
            event_by_id(&store.0, "one").unwrap().unwrap().payload["amount"],
            500
        );
        let mut delete = command("delete");
        delete.intent["delete"] = json!(true);
        let deleted = store.execute_command(&delete, &adapter).unwrap();
        drop(store);
        let mut store = db.open();
        let replay = store.execute_command(&command("first"), &adapter).unwrap();
        assert_eq!(replay.current_revision, deleted.current_revision);
        assert!(event_by_id(&store.0, "one").unwrap().is_none());
        assert!(store
            .pending_commands("hydration", "", 512)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn failures_roll_back_effects_and_recovery_uses_durable_intent() {
        let db = Database::new();
        let mut store = db.open();
        let c = command("pending");
        store.prepare_command(&c).unwrap();
        drop(store);
        let mut store = db.open();
        let pending = store.pending_commands("hydration", "", 10).unwrap();
        assert_eq!(pending.len(), 1);
        assert!(store
            .execute_command(
                &pending[0],
                &Fixture {
                    fail_projection: true
                }
            )
            .is_err());
        assert!(store.revision().unwrap().is_none());
        assert!(event_by_id(&store.0, "one").unwrap().is_none());
        assert!(metadata_value(&store.0, "accepted-auxiliary")
            .unwrap()
            .is_none());
        let exists: i64 = store
            .0
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name='fixture_projection'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(exists, 0);
        let accepted = store
            .execute_command(
                &pending[0],
                &Fixture {
                    fail_projection: false,
                },
            )
            .unwrap();
        assert!(accepted.accepted);
        let mut invalid = command("invalid");
        invalid.intent["payload"]["amount"] = json!(-1);
        let rejected = store
            .execute_command(
                &invalid,
                &Fixture {
                    fail_projection: false,
                },
            )
            .unwrap();
        assert!(!rejected.accepted);
        assert_eq!(rejected.current_revision, accepted.current_revision);
        assert!(store
            .pending_commands("hydration", "", 512)
            .unwrap()
            .is_empty());
        assert_eq!(
            event_by_id(&store.0, "one").unwrap().unwrap().payload["amount"],
            250
        );
    }

    #[test]
    fn planning_and_projection_reads_cannot_mutate_sources() {
        struct BadPlanner;
        impl CommandAdapter for BadPlanner {
            fn plan(
                &self,
                tx: &Transaction<'_>,
                _: &Command,
            ) -> Result<CommandChange, CommandError> {
                tx.execute(
                    "INSERT INTO store_state(key,value) VALUES('oops','true')",
                    [],
                )?;
                Ok(CommandChange::default())
            }
            fn normalize(
                &self,
                _: &Transaction<'_>,
                _: &BTreeMap<String, Value>,
                event: &IndexedEvent,
            ) -> Result<IndexedEvent, CommandError> {
                Ok(event.clone())
            }
            fn update_projections(
                &self,
                _: &Transaction<'_>,
                _: &CommandChange,
            ) -> Result<(), CommandError> {
                Ok(())
            }
        }
        let db = Database::new();
        let mut store = db.open();
        assert!(store.execute_command(&command("bad"), &BadPlanner).is_err());
        assert!(metadata_value(&store.0, "oops").unwrap().is_none());
        assert!(store.revision().unwrap().is_none());
        let accepted = store
            .execute_command(
                &command("good"),
                &Fixture {
                    fail_projection: false,
                },
            )
            .unwrap();
        assert!(store
            .read_projection(accepted.events_revision.as_deref().unwrap(), |tx| {
                tx.execute("DELETE FROM store_events", [])?;
                Ok(())
            })
            .is_err());
        assert!(event_by_id(&store.0, "one").unwrap().is_some());
    }

    #[test]
    fn separate_connections_conflict_identity_and_tracker_checks() {
        let db = Database::new();
        let mut a = db.open();
        let mut b = db.open();
        let adapter = Fixture {
            fail_projection: false,
        };
        let first = a.execute_command(&command("one"), &adapter).unwrap();
        let mut changed_intent = command("one");
        changed_intent.intent["payload"]["amount"] = json!(1);
        assert!(matches!(
            b.execute_command(&changed_intent, &adapter),
            Err(CommandError::Conflict(_))
        ));
        let mut foreign = command("two");
        foreign.tracker_id = "sleep".into();
        assert!(!b.execute_command(&foreign, &adapter).unwrap().accepted);
        let mut edit = command("edit");
        edit.intent["payload"]["amount"] = json!(300);
        b.execute_command(&edit, &adapter).unwrap();
        let mut stale = command("stale");
        stale.expected_revision = first.current_revision;
        let before = a.revision().unwrap();
        let rejected = a.execute_command(&stale, &adapter).unwrap();
        assert!(!rejected.accepted);
        assert_eq!(rejected.result["kind"], "conflict");
        assert_eq!(a.revision().unwrap(), before);
        assert!(rejected.operation_revision.is_none());
        drop(a);
        let mut a = db.open();
        // Clearing a retry's expectation must not bypass its frozen precondition.
        stale.expected_revision = None;
        let replay = a.execute_command(&stale, &adapter).unwrap();
        assert!(!replay.accepted && replay.replayed);
        assert_eq!(replay.result, rejected.result);
        a.acknowledge_command("hydration", "stale").unwrap();
        assert!(!a
            .undelivered_commands("hydration", "", 512)
            .unwrap()
            .iter()
            .any(|c| c.operation_id == "stale"));
        assert!(a
            .operation("hydration", "stale")
            .unwrap()
            .unwrap()
            .receipt
            .is_some());
        let mut corrected = stale.clone();
        corrected.operation_id = "corrected".into();
        corrected.intent["payload"]["amount"] = json!(300);
        assert!(a.execute_command(&corrected, &adapter).unwrap().accepted);
        assert_eq!(
            event_by_id(&a.0, "one").unwrap().unwrap().payload["amount"],
            300
        );
    }
    #[test]
    fn numeric_guard_rejects_zero_and_publishes_equal_integer_float_values() {
        struct Numeric;
        impl CommandAdapter for Numeric {
            fn plan(
                &self,
                tx: &Transaction<'_>,
                c: &Command,
            ) -> Result<CommandChange, CommandError> {
                Fixture {
                    fail_projection: false,
                }
                .plan(tx, c)
            }
            fn normalize(
                &self,
                _: &Transaction<'_>,
                _: &BTreeMap<String, Value>,
                event: &IndexedEvent,
            ) -> Result<IndexedEvent, CommandError> {
                let definition=tracen_engine::compile_tracker("tracker \"numeric\" v1 { fields { amount: float } validations {\n positive = amount != 0\n expected = amount == 500.0\n arithmetic = amount * 2 == 1000\n } }").unwrap();
                tracen_engine::validate_event(&definition,&json!({"event_id":event.event.event_id,"ts":event.event.ts,"payload":event.event.payload}).to_string()).map_err(|e|CommandError::Rejected(e.to_string()))?;
                Ok(event.clone())
            }
            fn update_projections(
                &self,
                _: &Transaction<'_>,
                _: &CommandChange,
            ) -> Result<(), CommandError> {
                Ok(())
            }
        }
        let disk = Database::new();
        let mut store = disk.open();
        for (id, amount) in [("zero-int", json!(0)), ("zero-float", json!(0.0))] {
            let mut c = command(id);
            c.intent["payload"]["amount"] = amount;
            assert!(!store.execute_command(&c, &Numeric).unwrap().accepted);
            assert!(event_by_id(&store.0, "one").unwrap().is_none());
            assert!(store.revision().unwrap().is_none());
        }
        let mut c = command("equal");
        c.intent["payload"]["amount"] = json!(500);
        assert!(store.execute_command(&c, &Numeric).unwrap().accepted);
        drop(store);
        assert_eq!(
            event_by_id(&disk.open().0, "one").unwrap().unwrap().payload["amount"],
            500
        );
    }
}
