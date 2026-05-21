use std::io::Write;
use std::{fs, path::PathBuf};

fn snapshot_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
        .join(file)
}

fn fixture_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file)
}

fn assert_snapshot(file: &str, actual: &str) {
    let path = snapshot_path(file);

    if std::env::var("TRACEN_UPDATE_SNAPSHOTS")
        .ok()
        .is_some_and(|value| value == "1")
    {
        let mut out = fs::File::create(&path).expect("write snapshot");
        out.write_all(actual.as_bytes())
            .expect("write snapshot bytes");
        return;
    }

    assert_eq!(fs::read_to_string(path).expect("snapshot file"), actual);
}

#[test]
fn dsl_to_ir_snapshot() {
    // Characterizes 0.1.x behavior: parser accepts `derives` block syntax today,
    // but derives are not materialized into IR.
    let dsl = fs::read_to_string(fixture_path("workout_tracker.tracker")).expect("dsl fixture");
    let actual = serde_json::to_string_pretty(&tracen_dsl::compile(&dsl).expect("compile fixture"))
        .expect("serialize ir");

    assert_snapshot("workout_tracker.ir.json", &actual);
}

#[test]
fn diagnostics_snapshot() {
    // Characterizes current 0.1.x behavior: malformed object-like view configs
    // currently fall back to strings before view config decoding fails.
    let dsl =
        fs::read_to_string(fixture_path("invalid_view_syntax.tracker")).expect("invalid fixture");
    let err = tracen_dsl::compile(&dsl).expect_err("invalid dsl must fail");
    let actual = serde_json::to_string_pretty(&err.to_json_value()).expect("serialize error");

    assert_snapshot("invalid_view_syntax.diagnostics.json", &actual);
}
