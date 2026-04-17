use rusqlite::{params, types::Type, Connection};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericTrackerRecord {
    pub tracker_id: String,
    pub dsl: String,
    #[serde(default)]
    pub version: Option<i64>,
    #[serde(default)]
    pub meta: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericEventRecord {
    pub event_id: String,
    pub tracker_id: String,
    pub ts: i64,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub meta: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GenericExportPayload {
    #[serde(default)]
    pub trackers: Vec<GenericTrackerRecord>,
    #[serde(default)]
    pub events: Vec<GenericEventRecord>,
    #[serde(default)]
    pub kv_meta: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericExportSummary {
    pub output_path: String,
    pub trackers: usize,
    pub events: usize,
    pub kv_meta: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericImportBundle {
    pub payload: GenericExportPayload,
    pub summary: GenericExportSummary,
}

fn ensure_parent(path: &Path) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
    }
    Ok(())
}

fn default_export_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "wrkt-generic-export-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

pub fn resolve_output_path(output_path: Option<&str>) -> PathBuf {
    match output_path {
        Some(value) if !value.trim().is_empty() => PathBuf::from(value),
        _ => default_export_path(),
    }
}

fn parse_required_json(
    value: &str,
    column_index: usize,
    column_name: &str,
) -> rusqlite::Result<Value> {
    serde_json::from_str(value).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(
            column_index,
            Type::Text,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{column_name} contains invalid JSON: {err}"),
            )),
        )
    })
}

fn parse_optional_json(
    value: Option<&str>,
    column_index: usize,
    column_name: &str,
) -> rusqlite::Result<Value> {
    match value {
        Some(value) => parse_required_json(value, column_index, column_name),
        None => Ok(Value::Null),
    }
}

pub fn export_generic_sqlite(
    payload: &GenericExportPayload,
    output_path: Option<&str>,
) -> Result<GenericExportSummary, String> {
    let path = resolve_output_path(output_path);
    ensure_parent(&path)?;

    if path.exists() {
        std::fs::remove_file(&path).map_err(|err| err.to_string())?;
    }

    let mut connection = Connection::open(&path).map_err(|err| err.to_string())?;
    let transaction = connection.transaction().map_err(|err| err.to_string())?;

    transaction
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS trackers (
                tracker_id TEXT PRIMARY KEY,
                dsl TEXT NOT NULL,
                version INTEGER,
                meta_json TEXT
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                tracker_id TEXT NOT NULL,
                ts INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                meta_json TEXT
            );
            CREATE TABLE IF NOT EXISTS kv_meta (
                key TEXT PRIMARY KEY,
                value_json TEXT NOT NULL
            );
            DELETE FROM trackers;
            DELETE FROM events;
            DELETE FROM kv_meta;
            ",
        )
        .map_err(|err| err.to_string())?;

    for tracker in &payload.trackers {
        let meta_json = serde_json::to_string(&tracker.meta).map_err(|err| err.to_string())?;
        transaction
            .execute(
                "INSERT INTO trackers (tracker_id, dsl, version, meta_json) VALUES (?1, ?2, ?3, ?4)",
                params![tracker.tracker_id, tracker.dsl, tracker.version, meta_json],
            )
            .map_err(|err| err.to_string())?;
    }

    for event in &payload.events {
        let payload_json = serde_json::to_string(&event.payload).map_err(|err| err.to_string())?;
        let meta_json = serde_json::to_string(&event.meta).map_err(|err| err.to_string())?;
        transaction
            .execute(
                "INSERT INTO events (event_id, tracker_id, ts, payload_json, meta_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![event.event_id, event.tracker_id, event.ts, payload_json, meta_json],
            )
            .map_err(|err| err.to_string())?;
    }

    for (key, value) in &payload.kv_meta {
        let value_json = serde_json::to_string(value).map_err(|err| err.to_string())?;
        transaction
            .execute(
                "INSERT INTO kv_meta (key, value_json) VALUES (?1, ?2)",
                params![key, value_json],
            )
            .map_err(|err| err.to_string())?;
    }

    transaction.commit().map_err(|err| err.to_string())?;

    Ok(GenericExportSummary {
        output_path: path.to_string_lossy().to_string(),
        trackers: payload.trackers.len(),
        events: payload.events.len(),
        kv_meta: payload.kv_meta.len(),
    })
}

pub fn import_generic_sqlite(input_path: &str) -> Result<GenericImportBundle, String> {
    if input_path.trim().is_empty() {
        Err("input_path is required".to_string())?;
    }

    let connection = Connection::open(input_path).map_err(|err| err.to_string())?;

    let mut trackers_stmt = connection
        .prepare("SELECT tracker_id, dsl, version, meta_json FROM trackers")
        .map_err(|err| err.to_string())?;
    let trackers = trackers_stmt
        .query_map([], |row| {
            let tracker_id: String = row.get(0)?;
            let dsl: String = row.get(1)?;
            let version: Option<i64> = row.get(2)?;
            let meta_json: Option<String> = row.get(3)?;
            let meta = parse_optional_json(meta_json.as_deref(), 3, "trackers.meta_json")?;
            Ok(GenericTrackerRecord {
                tracker_id,
                dsl,
                version,
                meta,
            })
        })
        .map_err(|err| err.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;

    let mut events_stmt = connection
        .prepare("SELECT event_id, tracker_id, ts, payload_json, meta_json FROM events")
        .map_err(|err| err.to_string())?;
    let events = events_stmt
        .query_map([], |row| {
            let event_id: String = row.get(0)?;
            let tracker_id: String = row.get(1)?;
            let ts: i64 = row.get(2)?;
            let payload_json: String = row.get(3)?;
            let meta_json: Option<String> = row.get(4)?;
            let payload = parse_required_json(&payload_json, 3, "events.payload_json")?;
            let meta = parse_optional_json(meta_json.as_deref(), 4, "events.meta_json")?;
            Ok(GenericEventRecord {
                event_id,
                tracker_id,
                ts,
                payload,
                meta,
            })
        })
        .map_err(|err| err.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;

    let mut kv_meta_stmt = connection
        .prepare("SELECT key, value_json FROM kv_meta")
        .map_err(|err| err.to_string())?;
    let kv_rows = kv_meta_stmt
        .query_map([], |row| {
            let key: String = row.get(0)?;
            let value_json: String = row.get(1)?;
            let value = parse_required_json(&value_json, 1, "kv_meta.value_json")?;
            Ok((key, value))
        })
        .map_err(|err| err.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;

    let kv_meta = kv_rows.into_iter().collect::<HashMap<_, _>>();

    let payload = GenericExportPayload {
        trackers,
        events,
        kv_meta,
    };

    let summary = GenericExportSummary {
        output_path: input_path.to_string(),
        trackers: payload.trackers.len(),
        events: payload.events.len(),
        kv_meta: payload.kv_meta.len(),
    };

    Ok(GenericImportBundle { payload, summary })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn generic_sqlite_round_trip_preserves_payload() {
        let payload = GenericExportPayload {
            trackers: vec![GenericTrackerRecord {
                tracker_id: "tracker.sample".to_string(),
                dsl: "tracker \"sample\" {}".to_string(),
                version: Some(1),
                meta: json!({ "scope": "test" }),
            }],
            events: vec![GenericEventRecord {
                event_id: "evt-1".to_string(),
                tracker_id: "tracker.sample".to_string(),
                ts: 1_704_067_200_000,
                payload: json!({ "group_key": "segment_a", "value_a": 8 }),
                meta: json!({ "source": "unit_test" }),
            }],
            kv_meta: HashMap::from([(String::from("key"), json!({ "ok": true }))]),
        };

        let output_path = resolve_output_path(None);
        let path_string = output_path.to_string_lossy().to_string();

        let summary = export_generic_sqlite(&payload, Some(path_string.as_str()))
            .expect("export should succeed");
        assert_eq!(summary.trackers, 1);
        assert_eq!(summary.events, 1);

        let imported =
            import_generic_sqlite(summary.output_path.as_str()).expect("import should succeed");

        assert_eq!(imported.payload.trackers.len(), 1);
        assert_eq!(imported.payload.events.len(), 1);
        assert_eq!(imported.payload.trackers[0].tracker_id, "tracker.sample");
        assert_eq!(imported.payload.events[0].event_id, "evt-1");
        assert_eq!(
            imported.payload.kv_meta.get("key"),
            Some(&json!({ "ok": true }))
        );

        let _ = std::fs::remove_file(summary.output_path);
    }

    #[test]
    fn import_generic_sqlite_rejects_malformed_json() {
        let output_path = resolve_output_path(None);
        let path_string = output_path.to_string_lossy().to_string();
        let connection = Connection::open(path_string.as_str()).expect("open sqlite");

        connection
            .execute_batch(
                "
                CREATE TABLE trackers (tracker_id TEXT PRIMARY KEY, dsl TEXT NOT NULL, version INTEGER, meta_json TEXT);
                CREATE TABLE events (event_id TEXT PRIMARY KEY, tracker_id TEXT NOT NULL, ts INTEGER NOT NULL, payload_json TEXT NOT NULL, meta_json TEXT);
                CREATE TABLE kv_meta (key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
                ",
            )
            .expect("create schema");
        connection
            .execute(
                "INSERT INTO trackers (tracker_id, dsl, version, meta_json) VALUES (?1, ?2, ?3, ?4)",
                params!["tracker.sample", "tracker \"sample\" {}", 1_i64, "{}"],
            )
            .expect("insert tracker");
        connection
            .execute(
                "INSERT INTO events (event_id, tracker_id, ts, payload_json, meta_json) VALUES (?1, ?2, ?3, ?4, ?5)",
                params!["evt-1", "tracker.sample", 1_704_067_200_000_i64, "{bad", "{}"],
            )
            .expect("insert malformed event");
        connection
            .execute(
                "INSERT INTO kv_meta (key, value_json) VALUES (?1, ?2)",
                params!["ok", "{}"],
            )
            .expect("insert kv");

        let err = import_generic_sqlite(path_string.as_str())
            .expect_err("malformed payload_json must fail import");
        assert!(err.contains("events.payload_json"));

        let _ = std::fs::remove_file(path_string);
    }
}
