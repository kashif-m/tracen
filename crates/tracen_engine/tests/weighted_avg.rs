use serde_json::json;
use tracen_engine::{compile_tracker, compute_metric_by_name, MetricComputeOptions};
use tracen_ir::{EventId, NormalizedEvent, Timestamp};

#[test]
fn weighted_average_uses_eligible_values_and_rejects_invalid_weights() {
    let def = compile_tracker(
        r#"tracker "weighted" v1 {
      fields { pace: float optional
 distance: float optional }
      metrics { average = weighted_avg(pace, distance) over all_time }
    }"#,
    )
    .unwrap();
    let compute = |payloads: Vec<serde_json::Value>| {
        let events = payloads
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
        compute_metric_by_name(&def, &events, "average", MetricComputeOptions::default())
    };
    assert_eq!(
        compute(vec![
            json!({"pace":300,"distance":1000}),
            json!({"pace":450,"distance":2000}),
            json!({"distance":10000}),
            json!({"pace":1,"distance":0}),
            json!({"pace":99})
        ])
        .unwrap(),
        json!(400.0)
    );
    assert_eq!(compute(vec![]).unwrap(), json!(null));
    assert_eq!(
        compute(vec![json!({"pace":5,"distance":0})]).unwrap(),
        json!(null)
    );
    assert!(compute(vec![json!({"pace":5,"distance":-1})]).is_err());
    assert!(compute(vec![json!({"pace":1e308,"distance":1e308})]).is_err());
    for expr in [
        "weighted_avg(pace)",
        "weighted_avg(pace, distance, pace)",
        "weighted_avg(pace, event.id)",
    ] {
        assert!(compile_tracker(&format!(
            r#"tracker "invalid" v1 {{ fields {{ pace: float
 distance: float }} metrics {{ average = {expr} over all_time }} }}"#
        ))
        .is_err());
    }
}
