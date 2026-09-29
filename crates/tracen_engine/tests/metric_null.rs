use serde_json::json;
use tracen_engine::{compile_tracker, compute_metric_by_name, MetricComputeOptions};
use tracen_ir::{EventId, NormalizedEvent, Timestamp};
#[test]
fn metric_null_is_missing_not_numeric_zero() {
    let def = compile_tracker(
        r#"tracker "nullable" v1 {
      fields { amount: float optional
 enabled: bool }
      metrics { selected = avg(if (enabled == true) then amount else null) over all_time
        missing = sum(if (amount == null) then 1 else 0) over all_time }
    }"#,
    )
    .unwrap();
    let events = [
        json!({"amount":0,"enabled":true}),
        json!({"amount":10,"enabled":true}),
        json!({"amount":100,"enabled":false}),
        json!({"enabled":false}),
        json!({"enabled":false}),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, payload)| {
        NormalizedEvent::new(
            EventId::new(i.to_string()),
            def.tracker_id().clone(),
            Timestamp::new(i as i64),
            payload,
            json!({}),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(
        compute_metric_by_name(&def, &events, "selected", MetricComputeOptions::default()).unwrap(),
        json!(5.0)
    );
    assert_eq!(
        compute_metric_by_name(&def, &events, "missing", MetricComputeOptions::default()).unwrap(),
        json!(2.0)
    );
}
