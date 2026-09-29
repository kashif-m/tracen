#![allow(dead_code, unused_imports, unused_variables)]
mod hydration {
    include!(concat!(env!("OUT_DIR"), "/hydration_tracker_compiled.rs"));
}
mod sleep {
    include!(concat!(env!("OUT_DIR"), "/sleep_tracker_compiled.rs"));
}
#[derive(Clone)]
struct Helpers;
impl hydration::HydrationHelpers for Helpers {
    fn read_model_unit_echo(&self, _events: &[tracen_pack::PackInputEvent], _offset_minutes: i32, _catalog_json: &serde_json::Value, unit: hydration::Unit) -> Result<hydration::UnitEchoResponse, String> {
        Ok(hydration::UnitEchoResponse { unit })
    }
}
impl sleep::SleepHelpers for Helpers {}

#[test]
fn generated_adapters_validate_and_execute_both_trackers() {
    use serde_json::json;
    fn check<A: tracen_pack::PackExecutionAdapter>(
        runtime: tracen_pack::PackRuntime<A>,
        payload: serde_json::Value,
        expected: f64,
        invalid: serde_json::Value,
    ) {
        let accepted = runtime
            .validate_pack_event(
                &json!({"event_id":"one","ts":0,"payload":payload,"meta":{"origin":"generated"}})
                    .to_string(),
            )
            .unwrap();
        let events = runtime
            .prepare_events_json(&json!([accepted]).to_string())
            .unwrap();
        assert_eq!(events[0].event_id.as_deref(), Some("one"));
        assert_eq!(events[0].meta, json!({"origin":"generated"}));
        assert!(runtime
            .validate_pack_event(
                &json!({"event_id":"bad-rule","ts":0,"payload":invalid}).to_string()
            )
            .is_err());
        let result = runtime
            .pack_query(
                &events,
                0,
                &json!([]),
                r#"{"view":"daily","metric":"total","group_by":"day"}"#,
            )
            .unwrap();
        assert_eq!(result["points"][0]["value"].as_f64(), Some(expected));
        assert!(runtime
            .validate_pack_event(r#"{"event_id":"bad","ts":0,"payload":{}}"#)
            .is_err());
    }
    check(
        hydration::hydration_pack_runtime(Helpers).unwrap(),
        json!({"amount_ml":250}),
        0.25,
        json!({"amount_ml":0}),
    );
    check(
        sleep::sleep_pack_runtime(Helpers).unwrap(),
        json!({"start":82800000,"end":111600000}),
        8.0,
        json!({"start":200,"end":100}),
    );
}

#[test]
fn enum_wire_names_follow_the_dsl_exactly() {
    let runtime = hydration::hydration_pack_runtime(Helpers).unwrap();
    for unit in ["fl-oz", "metric_ml", "MixedCase"] {
        let query = serde_json::json!({"read_model":"unit_echo","unit":unit});
        let result = runtime.pack_query(&[], 0, &serde_json::json!([]), &query.to_string()).unwrap();
        assert_eq!(result["unit"], unit);
    }
    for unit in ["fl_oz", "mixed_case", "unknown"] {
        let query = serde_json::json!({"read_model":"unit_echo","unit":unit});
        assert!(runtime.pack_query(&[], 0, &serde_json::json!([]), &query.to_string()).is_err());
    }
}
