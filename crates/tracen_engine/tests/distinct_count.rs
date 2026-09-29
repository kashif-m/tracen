use serde_json::json;
use tracen_engine::compile_tracker;
use tracen_engine::{compute_metric_by_name, MetricComputeOptions};
use tracen_ir::{EventId, GroupByDimension, NormalizedEvent, Timestamp};

#[test]
fn distinct_count_is_typed_skips_null_and_respects_groups() {
    let def = compile_tracker(
        r#"tracker "attendance" v1 {
      fields { day: int optional
 group_key: text }
      metrics { days = distinct_count(day) over all_time
 groups = distinct_count(group_key) over all_time }
    }"#,
    )
    .unwrap();
    let events = [(1, "a"), (1, "a"), (2, "a"), (2, "b")]
        .into_iter()
        .enumerate()
        .map(|(i, (day, group))| {
            NormalizedEvent::new(
                EventId::new(i.to_string()),
                def.tracker_id().clone(),
                Timestamp::new(i as i64),
                json!({"day":day,"group_key":group}),
                json!({}),
            )
        })
        .chain(std::iter::once(NormalizedEvent::new(
            EventId::new("null"),
            def.tracker_id().clone(),
            Timestamp::new(5),
            json!({"group_key":"a"}),
            json!({}),
        )))
        .collect::<Vec<_>>();
    assert_eq!(
        compute_metric_by_name(&def, &events, "days", MetricComputeOptions::default()).unwrap(),
        json!(2)
    );
    assert_eq!(
        compute_metric_by_name(&def, &events, "groups", MetricComputeOptions::default()).unwrap(),
        json!(2)
    );
    assert_eq!(
        compute_metric_by_name(
            &def,
            &events,
            "days",
            MetricComputeOptions {
                group_by: Some(vec![GroupByDimension::Field("group_key".into())]),
                ..Default::default()
            }
        )
        .unwrap(),
        json!({"a":2,"b":1})
    );
    assert_eq!(
        compute_metric_by_name(&def, &[], "days", MetricComputeOptions::default()).unwrap(),
        json!(0)
    );
    assert!(compile_tracker(r#"tracker "bad" v1 { fields { day: int } metrics { days = distinct_count() over all_time } }"#).is_err());
}

#[test]
fn distinct_count_preserves_scalar_types_and_numeric_equality() {
    let def = compile_tracker(
        r#"tracker "types" v1 {
      fields { value_a: float optional }
      metrics { unique_values = distinct_count(meta.value) over all_time }
    }"#,
    )
    .unwrap();
    let events = [
        json!(1),
        json!(1.0),
        json!("1"),
        json!(true),
        json!(0.0),
        json!(-0.0),
        json!(null),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, value)| {
        NormalizedEvent::new(
            EventId::new(i.to_string()),
            def.tracker_id().clone(),
            Timestamp::new(i as i64),
            json!({}),
            json!({"value":value}),
        )
    })
    .collect::<Vec<_>>();
    assert_eq!(
        compute_metric_by_name(
            &def,
            &events,
            "unique_values",
            MetricComputeOptions::default()
        )
        .unwrap(),
        json!(4)
    );
}
