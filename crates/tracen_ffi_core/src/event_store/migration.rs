//! Startup upgrades use SQLite's transaction as staging state. Readers observe
//! the previous state until publication; interruption rolls back the whole stage.
use super::{commands::next_revision, error, EventStore};
use rusqlite::{
    backup::{Backup, StepResult},
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior,
};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Preserve committed WAL state using SQLite's online backup API. Existing
/// recoverable sources are never replaced. Call on the host's serialized writer
/// before opening/upgrading the source, with ordinary app writes disabled.
pub fn preserve_source(source: &Path, destination: &Path) -> Result<(), String> {
    if !source.is_file() {
        return Ok(());
    }
    if destination.exists() {
        let mut aliases = fs::canonicalize(source).map_err(error)?
            == fs::canonicalize(destination).map_err(error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let a = fs::metadata(source).map_err(error)?;
            let b = fs::metadata(destination).map_err(error)?;
            aliases |= a.dev() == b.dev() && a.ino() == b.ino();
        }
        if aliases {
            return Err("Recovery source must use a separate path".into());
        }
        return check_database(destination);
    }
    let temporary = new_backup_file(destination)?;
    // Drop only cleans up this call's exclusively created file. It cannot mask
    // the original backup error, and never deletes a pre-existing partial file.
    let _cleanup = BackupFile(temporary.clone());
    let input =
        Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(error)?;
    input.busy_timeout(Duration::from_secs(5)).map_err(error)?;
    {
        let mut output = Connection::open(&temporary).map_err(error)?;
        output
            .pragma_update(None, "synchronous", "FULL")
            .map_err(error)?;
        let backup = Backup::new(&input, &mut output).map_err(error)?;
        let mut blocked_since = None;
        loop {
            match backup.step(128).map_err(error)? {
                StepResult::Done => break,
                StepResult::More => blocked_since = None,
                StepResult::Busy | StepResult::Locked => {
                    let started = blocked_since.get_or_insert_with(Instant::now);
                    if started.elapsed() >= Duration::from_secs(5) {
                        return Err("Recovery backup is busy; close other writers and retry".into());
                    }
                }
                _ => return Err("Unexpected recovery backup state".into()),
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    check_database(&temporary)?;
    fs::File::open(&temporary)
        .and_then(|file| file.sync_all())
        .map_err(error)?;
    // Android app storage forbids hard links. The serialized host writer owns
    // these paths; publish the checked backup with a same-directory rename.
    // Never replace an existing recovery source, including a completed retry.
    if destination.exists() {
        check_database(destination)?;
        fs::remove_file(&temporary).map_err(error)?;
    } else {
        fs::rename(&temporary, destination).map_err(error)?;
    }
    if let Some(parent) = destination.parent() {
        fs::File::open(parent)
            .and_then(|file| file.sync_all())
            .map_err(error)?;
    }
    Ok(())
}

struct BackupFile(PathBuf);
impl Drop for BackupFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
fn new_backup_file(destination: &Path) -> Result<PathBuf, String> {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    for _ in 0..128 {
        let id = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = parent.join(format!(".tracen-backup-{}-{id}.sqlite", std::process::id()));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(_) => return Ok(path),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(error(e)),
        }
    }
    Err("Could not reserve a recovery backup file".into())
}

fn check_database(path: &Path) -> Result<(), String> {
    let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(error)?;
    let result: String = db
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .map_err(error)?;
    if result != "ok" {
        return Err(format!("Recovery source failed integrity check: {result}"));
    }
    Ok(())
}

impl EventStore {
    /// A persisted pending checkpoint is retryable. Data, projection changes,
    /// versions and completion are published in one FULL-durability transaction.
    /// The callback is trusted storage/migration machinery, not a producer API.
    pub fn migrate(
        &mut self,
        identity: &str,
        apply: impl FnOnce(&Transaction<'_>) -> Result<(), String>,
    ) -> Result<bool, String> {
        if identity.is_empty() {
            return Err("Migration identity is required".into());
        }
        let key = format!("_migration:{identity}");
        let complete: Option<String> = self
            .0
            .query_row("SELECT value FROM store_state WHERE key=?1", [&key], |r| {
                r.get(0)
            })
            .optional()
            .map_err(error)?;
        if complete.as_deref() == Some("\"complete\"") {
            return Ok(false);
        }
        self.0
            .execute(
                "INSERT OR IGNORE INTO store_state(key,value) VALUES(?1,'\"pending\"')",
                [&key],
            )
            .map_err(error)?;
        let tx = self
            .0
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(error)?;
        let complete: String = tx
            .query_row("SELECT value FROM store_state WHERE key=?1", [&key], |r| {
                r.get(0)
            })
            .map_err(error)?;
        if complete == "\"complete\"" {
            return Ok(false);
        }
        apply(&tx)?;
        let revision = next_revision(&tx).map_err(error)?;
        tx.execute("INSERT INTO store_state(key,value) VALUES('revision',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[&revision]).map_err(error)?;
        tx.execute("INSERT INTO store_state(key,value) VALUES('_events_revision',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",[serde_json::to_string(&revision).map_err(error)?]).map_err(error)?;
        tx.execute(
            "UPDATE store_state SET value='\"complete\"' WHERE key=?1",
            [key],
        )
        .map_err(error)?;
        tx.commit().map_err(error)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backup_never_deletes_a_colliding_source_or_existing_partial() {
        let dir = std::env::temp_dir().join(format!(
            "tracen-backup-collision-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&dir).unwrap();
        let source = dir.join("data.partial");
        let destination = dir.join("data.sqlite");
        {
            let db = Connection::open(&source).unwrap();
            db.execute_batch("CREATE TABLE kept(value); INSERT INTO kept VALUES(42);")
                .unwrap();
        }
        let original = fs::read(&source).unwrap();
        // A killed backup may leave an incomplete sibling. Retry must neither
        // reuse its bytes nor delete a file owned by that earlier operation.
        let interrupted = new_backup_file(&destination).unwrap();
        fs::write(&interrupted, b"incomplete SQLite backup").unwrap();

        preserve_source(&source, &destination).unwrap();
        assert_eq!(fs::read(&source).unwrap(), original);
        check_database(&destination).unwrap();
        assert_eq!(fs::read(&interrupted).unwrap(), b"incomplete SQLite backup");
        preserve_source(&source, &destination).unwrap();
        assert_eq!(fs::read(&source).unwrap(), original);
        assert!(preserve_source(&source, &dir.join("./data.partial")).is_err());
        #[cfg(unix)]
        {
            let alias = dir.join("alias.sqlite");
            std::os::unix::fs::symlink(&source, &alias).unwrap();
            assert!(preserve_source(&source, &alias).is_err());
            fs::remove_file(&alias).unwrap();
            fs::hard_link(&source, &alias).unwrap();
            assert!(preserve_source(&source, &alias).is_err());
        }
        let unrelated = dir.join("other.partial");
        fs::write(&unrelated, b"keep me").unwrap();
        preserve_source(&source, &dir.join("other.sqlite")).unwrap();
        assert_eq!(fs::read(&unrelated).unwrap(), b"keep me");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn wal_backup_and_interrupted_upgrade_preserve_source_and_retry_once() {
        if let Some(path) = std::env::var_os("TRACEN_TEST_MIGRATION_CRASH_PATH") {
            let mut store = EventStore::open(Path::new(&path)).unwrap();
            store
                .migrate("v2", |tx| {
                    tx.execute("UPDATE store_state SET value='43' WHERE key='kept'", [])
                        .map_err(error)?;
                    // Exit without destructors: SQLite must recover an uncommitted stage.
                    std::process::exit(89);
                })
                .unwrap();
            std::process::exit(90);
        }
        let directory = std::env::temp_dir().join(format!(
            "tracen-migration-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir(&directory).unwrap();
        let source = directory.join("source.sqlite");
        let backup = directory.join("before.sqlite");
        let mut store = EventStore::open(&source).unwrap();
        store
            .0
            .execute_batch(
                "PRAGMA wal_autocheckpoint=0; INSERT INTO store_state VALUES('kept','42');",
            )
            .unwrap();
        assert!(preserve_source(&source, &source).is_err());
        preserve_source(&source, &backup).unwrap();
        let preserved = Connection::open(&backup).unwrap();
        assert_eq!(
            preserved
                .query_row("SELECT value FROM store_state WHERE key='kept'", [], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
            "42"
        );
        let interrupted = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "event_store::migration::tests::wal_backup_and_interrupted_upgrade_preserve_source_and_retry_once", "--nocapture"])
            .env("TRACEN_TEST_MIGRATION_CRASH_PATH", &source)
            .output().unwrap();
        assert_eq!(
            interrupted.status.code(),
            Some(89),
            "{}",
            String::from_utf8_lossy(&interrupted.stderr)
        );
        assert_eq!(store.metadata(&["kept".into()]).unwrap().1["kept"], 42);
        assert!(store
            .migrate("v2", |tx| {
                tx.execute("UPDATE store_state SET value='43' WHERE key='kept'", [])
                    .map_err(error)?;
                Err("interrupted".into())
            })
            .is_err());
        drop(store);
        let mut store = EventStore::open(&source).unwrap();
        assert_eq!(store.metadata(&["kept".into()]).unwrap().1["kept"], 42);
        assert!(store
            .migrate("v2", |tx| {
                tx.execute("UPDATE store_state SET value='43' WHERE key='kept'", [])
                    .map_err(error)?;
                Ok(())
            })
            .unwrap());
        let revision = store.revision().unwrap();
        drop(store);
        let mut store = EventStore::open(&source).unwrap();
        assert!(!store
            .migrate("v2", |_| panic!("accepted upgrade replayed"))
            .unwrap());
        assert_eq!(store.revision().unwrap(), revision);
        assert_eq!(store.metadata(&["kept".into()]).unwrap().1["kept"], 43);
        preserve_source(&source, &backup).unwrap();
        assert_eq!(
            preserved
                .query_row("SELECT value FROM store_state WHERE key='kept'", [], |r| {
                    r.get::<_, String>(0)
                })
                .unwrap(),
            "42"
        );
        drop(preserved);
        drop(store);
        fs::remove_dir_all(directory).unwrap();
    }
}
