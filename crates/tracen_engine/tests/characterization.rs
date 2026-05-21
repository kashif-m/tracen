use std::io::Write;
use std::{fs, path::PathBuf};

use tracen_ir::{NormalizedEvent, Query, TrackerDefinition};

fn fixture_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file)
}

fn snapshot_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
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

fn load_definition() -> TrackerDefinition {
    let dsl =
        fs::read_to_string(fixture_path("compute_and_simulate_dsl.tracker")).expect("dsl fixture");
    tracen_dsl::compile(&dsl).expect("compile fixture")
}

fn load_events(def: &TrackerDefinition) -> Vec<NormalizedEvent> {
    let events: Vec<serde_json::Value> = serde_json::from_str(
        &fs::read_to_string(fixture_path("compute_events.json")).expect("events fixture"),
    )
    .expect("parse events fixture");

    events
        .iter()
        .map(|raw| tracen_engine::validate_event(def, &raw.to_string()).expect("validate event"))
        .collect()
}

#[test]
fn compute_output_snapshot() {
    let def = load_definition();
    let events = load_events(&def);
    let query = Query::default();
    let output = tracen_engine::compute(&def, &events, query).expect("compute");
    let actual = serde_json::to_string_pretty(&output).expect("serialize output");

    assert_snapshot("compute_output.json", &actual);
}

#[test]
fn simulation_output_snapshot() {
    let def = load_definition();
    let events = load_events(&def);
    let query = Query::default();
    let output =
        tracen_engine::simulate(&def, &events[0..2], &events[2..], query).expect("simulate");

    let actual = serde_json::to_string_pretty(&output).expect("serialize output");
    assert_snapshot("simulation_output.json", &actual);
}
