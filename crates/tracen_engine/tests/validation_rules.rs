use serde_json::json;
use tracen_engine::{compile_tracker, validate_event};

#[test]
fn hydration_rules_reject_out_of_range_values() {
    let def = compile_tracker(
        r#"
tracker "hydration" v1 {
  fields { amount_ml: float }
  validations {
    amount_range = amount_ml > 0 && amount_ml <= 10000
  }
}
"#,
    )
    .unwrap();
    for amount in [-1.0, 0.0, 10001.0] {
        let event = json!({"event_id":"h1","ts":1000,"payload":{"amount_ml":amount}});
        assert!(
            validate_event(&def, &event.to_string()).is_err(),
            "accepted {amount}"
        );
    }
    let valid = json!({"event_id":"h1","ts":1000,"payload":{"amount_ml":250.0}});
    validate_event(&def, &valid.to_string()).unwrap();
}

#[test]
fn sleep_rules_validate_relationships_and_optional_values() {
    let def = compile_tracker(
        r#"
tracker "sleep" v1 {
  fields {
    start: timestamp
    end: timestamp
    quality: int optional
  }
  validations {
    chronological = end > start
    quality_range = quality == null || (quality >= 1 && quality <= 5)
  }
}
"#,
    )
    .unwrap();
    for payload in [
        json!({"start":100,"end":100}),
        json!({"start":200,"end":100}),
        json!({"start":100,"end":200,"quality":6}),
    ] {
        assert!(validate_event(
            &def,
            &json!({"event_id":"s1","ts":1000,"payload":payload}).to_string()
        )
        .is_err());
    }
    for quality in [None, Some(json!(null)), Some(json!(5))] {
        let mut payload = json!({"start":100,"end":200});
        if let Some(value) = quality {
            payload["quality"] = value;
        }
        validate_event(
            &def,
            &json!({"event_id":"s1","ts":1000,"payload":payload}).to_string(),
        )
        .unwrap();
    }
}

#[test]
fn invalid_validation_definitions_fail_at_compile_time() {
    for rule in ["missing > 0", "amount > \"wrong\"", "unknown(amount) > 0"] {
        let dsl = format!("tracker \"hydration\" v1 {{ fields {{ amount: float }} validations {{ positive = {rule} }} }}");
        assert!(compile_tracker(&dsl).is_err(), "compiled {rule}");
    }
}

#[test]
fn misspelled_or_duplicate_sections_cannot_silently_drop_rules() {
    for sections in [
        "validation { positive = amount > 0 }",
        "validations { positive = amount > 0 } validations { ceiling = amount < 10 }",
    ] {
        let dsl = format!("tracker \"hydration\" v1 {{ fields {{ amount: float }} {sections} }}");
        assert!(compile_tracker(&dsl).is_err(), "compiled {sections}");
    }
}

#[test]
fn state_application_revalidates_constructed_and_mutated_events() {
    use tracen_engine::EngineState;
    use tracen_ir::{EventId, NormalizedEvent, Timestamp};
    let def = compile_tracker(
        r#"tracker "water" v1 {
        fields { amount: float }
        validations { positive = amount > 0 }
    }"#,
    )
    .unwrap();
    let mut state = EngineState::new(&def);
    for (id, payload, meta) in [
        ("negative", json!({"amount":-1}), json!({})),
        ("wrong-type", json!({"amount":"250"}), json!({})),
        ("missing", json!({}), json!({})),
        (" ", json!({"amount":250}), json!({})),
        ("bad-meta", json!({"amount":250}), json!([])),
    ] {
        let event = NormalizedEvent::new(
            EventId::new(id),
            def.tracker_id().clone(),
            Timestamp::new(100),
            payload,
            meta,
        );
        assert!(tracen_engine::apply(&def, &mut state, event).is_err());
        assert_eq!(state.total_events(), 0);
    }
    let valid = json!({"event_id":"valid", "ts":100,"payload":{"amount":250}});
    let mut mutated = validate_event(&def, &valid.to_string()).unwrap();
    mutated.payload_mut()["amount"] = json!(-1);
    assert!(tracen_engine::apply(&def, &mut state, mutated).is_err());
    assert_eq!(state.total_events(), 0);
    tracen_engine::apply(
        &def,
        &mut state,
        validate_event(&def, &valid.to_string()).unwrap(),
    )
    .unwrap();
    assert_eq!(state.total_events(), 1);
}

#[test]
fn compute_plan_cannot_be_used_with_another_tracker() {
    let first = compile_tracker(r#"tracker "first" v1 { fields { amount: float } metrics { total = sum(amount) over all_time } }"#).unwrap();
    let second = compile_tracker(r#"tracker "second" v1 { fields { amount: float } metrics { total = count() over all_time } }"#).unwrap();
    let plan = tracen_engine::compile_compute_plan(&first).unwrap();
    let empty = tracen_engine::prepare_events_for_compute(&first, &[]).unwrap();
    assert!(tracen_engine::compute_with_plan(&second, &plan, &empty, Default::default()).is_err());
    tracen_engine::compute_with_plan(&first, &plan, &empty, Default::default()).unwrap();
}

#[test]
fn compute_plan_is_bound_to_definition_even_when_identity_is_reused() {
    let first = compile_tracker(r#"tracker "shared" v1 { fields { amount: float } metrics { total = sum(amount) over all_time } }"#).unwrap();
    let plan = tracen_engine::compile_compute_plan(&first).unwrap();
    let empty = tracen_engine::prepare_events_for_compute(&first, &[]).unwrap();
    let serialized = serde_json::to_value(&first).unwrap();
    let reloaded = serde_json::from_value(serialized.clone()).unwrap();
    tracen_engine::compute_with_plan(&reloaded, &plan, &empty, Default::default()).unwrap();
    let mut changed = serialized;
    changed["metrics"][0]["aggregation"]["func"] = json!("Count");
    let changed: tracen_ir::TrackerDefinition = serde_json::from_value(changed).unwrap();
    assert_eq!(changed.tracker_id(), first.tracker_id());
    assert_ne!(changed.metrics(), first.metrics());
    assert!(tracen_engine::compute_with_plan(&changed, &plan, &empty, Default::default()).is_err());
}

#[test]
fn state_rejects_changed_definition_with_the_same_tracker_id() {
    let first = compile_tracker(
        r#"tracker "water" v1 { fields { amount: float } validations { positive = amount > 0 } }"#,
    )
    .unwrap();
    let mut state = tracen_engine::EngineState::for_definition(&first);
    let valid = validate_event(
        &first,
        r#"{"event_id":"valid","ts":1,"payload":{"amount":250}}"#,
    )
    .unwrap();
    tracen_engine::apply(&first, &mut state, valid).unwrap();
    let mut changed = serde_json::to_value(&first).unwrap();
    changed["validations"] = json!([]);
    let changed: tracen_ir::TrackerDefinition = serde_json::from_value(changed).unwrap();
    let invalid_under_original = validate_event(
        &changed,
        r#"{"event_id":"invalid","ts":2,"payload":{"amount":0}}"#,
    )
    .unwrap();
    assert!(tracen_engine::apply(&changed, &mut state, invalid_under_original).is_err());
    assert_eq!(state.total_events(), 1);
}

#[test]
fn snapshot_restore_validates_every_event_and_recomputes_derives() {
    let def = compile_tracker(
        r#"tracker "water" v1 {
        fields { amount: float }
        validations { positive = amount > 0 }
        derive { doubled = amount * 2 }
    }"#,
    )
    .unwrap();
    let mut original = tracen_engine::EngineState::new(&def);
    let event = validate_event(
        &def,
        r#"{"event_id":"valid","ts":123,"payload":{"amount":250},"meta":{"source":"manual"}}"#,
    )
    .unwrap();
    tracen_engine::apply(&def, &mut original, event).unwrap();
    let snapshot = serde_json::to_value(&original).unwrap();
    let mut modified = snapshot.clone();
    modified["events"][0]["payload"]["doubled"] = json!(999);
    let restored = tracen_engine::restore_state(&def, &modified.to_string()).unwrap();
    assert_eq!(serde_json::to_value(&restored).unwrap(), snapshot);
    assert_eq!(restored.events()[0].payload()["doubled"], json!(500.0));
    let mut bad_event = modified["events"][0].clone();
    bad_event["event_id"] = json!("invalid");
    bad_event["payload"]["amount"] = json!(-1);
    modified["events"].as_array_mut().unwrap().push(bad_event);
    assert!(tracen_engine::restore_state(&def, &modified.to_string()).is_err());
    assert_eq!(serde_json::to_value(&original).unwrap(), snapshot);
    modified = snapshot.clone();
    modified["tracker_id"] = json!("another-tracker");
    assert!(tracen_engine::restore_state(&def, &modified.to_string()).is_err());
    modified = snapshot;
    modified["events"][0]["meta"] = json!([]);
    assert!(tracen_engine::restore_state(&def, &modified.to_string()).is_err());
}

#[test]
fn prepared_batches_reject_changed_definitions_even_when_empty() {
    let first = compile_tracker(r#"tracker "water" v1 { fields { amount: float } derive { doubled = amount * 2 } metrics { total = sum(doubled) over all_time } }"#).unwrap();
    let event = validate_event(
        &first,
        r#"{"event_id":"e","ts":1,"payload":{"amount":250}}"#,
    )
    .unwrap();
    let serialized = serde_json::to_value(&first).unwrap();
    let equivalent = serde_json::from_value(serialized.clone()).unwrap();
    let mut changed = serialized;
    changed["derives"][0]["expr"] = json!({"Int":999});
    let changed: tracen_ir::TrackerDefinition = serde_json::from_value(changed).unwrap();
    assert_eq!(changed.tracker_id(), first.tracker_id());
    for events in [vec![], vec![event]] {
        let batch = tracen_engine::prepare_events_for_compute(&first, &events).unwrap();
        tracen_engine::compute_with_prepared_events(&equivalent, &batch, Default::default())
            .unwrap();
        assert!(
            tracen_engine::compute_with_prepared_events(&changed, &batch, Default::default())
                .is_err()
        );
        assert!(tracen_engine::compute_metric_by_name_with_prepared_events(
            &changed,
            &batch,
            "total",
            Default::default()
        )
        .is_err());
        let changed_plan = tracen_engine::compile_compute_plan(&changed).unwrap();
        assert!(tracen_engine::compute_with_plan(
            &changed,
            &changed_plan,
            &batch,
            Default::default()
        )
        .is_err());
    }
}
