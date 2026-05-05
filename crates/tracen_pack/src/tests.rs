use super::{
    build, CompiledPack, PackBuildConfig, PackInputEvent, PackRuntime, PackRuntimeOptions,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use tempfile::tempdir;

struct StubAdapter;

impl super::PackExecutionAdapter for StubAdapter {
    fn base_catalog(&self) -> Result<serde_json::Value, String> {
        Ok(json!([{ "slug": "base" }]))
    }

    fn validate_catalog_entry(
        &self,
        entry_type: &str,
        entry_json: &str,
    ) -> Result<serde_json::Value, String> {
        Ok(
            json!({ "entry_type": entry_type, "entry": serde_json::from_str::<serde_json::Value>(entry_json).unwrap() }),
        )
    }

    fn execute(
        &self,
        _definition: &tracen_ir::TrackerDefinition,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &serde_json::Value,
        plan: &super::PackExecutionPlan,
    ) -> Result<serde_json::Value, String> {
        Ok(json!({
            "events": events.len(),
            "offset_minutes": offset_minutes,
            "catalog": catalog_json,
            "plan": plan,
        }))
    }

    fn execute_read_model(
        &self,
        _definition: &tracen_ir::TrackerDefinition,
        _events: &[PackInputEvent],
        _offset_minutes: i32,
        _catalog_json: &serde_json::Value,
        query: &super::ReadModelQueryPlan,
    ) -> Result<serde_json::Value, String> {
        Ok(json!({
            "read_model": query.read_model_name,
            "params": query.params,
        }))
    }
}

fn sample_dsl() -> &'static str {
    r#"
tracker "sample_pack" v1 {
  fields {
    category: text optional
    bucket: int optional
    score: int optional
  }
  metrics {
    total_sets = count() over all_time
  }
  views {
    view "metric_series" {
      config = {"query_type":"MetricSeriesQuery","response_type":"MetricSeriesResponse","result_kind":"metric_series","count_metric":"total_sets","group_by":{"bucket":{"field":"bucket"}},"filters":{"category":{"field":"category","op":"eq","type":"string","optional":true}},"metrics":{"total_sets":{"metric":"total_sets","label":"Sets"}}}
    }
    view "category_dist" {
      config = {"query_type":"CategoryDistQuery","response_type":"CategoryDistResponse","result_kind":"distribution","group_by":{"category":{"field":"category"}},"metrics":{"total_sets":{"metric":"total_sets","label":"Sets"}}}
    }
  }
  catalog {
    entry "thing" {
      fields = {"slug":{"type":"string"}}
    }
  }
  read_models {
    read_model "daily_rollup" {
      query_type = "DailyRollupQuery"
      response_type = "DailyRollupResponse"
      params = {"bucket":{"type":"number"}}
      fields = {"total":{"type":"number"}}
    }
  }
}
"#
}

fn filtered_scope_dsl() -> &'static str {
    r#"
tracker "filtered_scope_pack" v1 {
  fields {
    segment: text optional
    day_bucket: int optional
    week_bucket: int optional
    month_bucket: int optional
    amount: float optional
    units: int optional
  }
  derive {
    derived_total = if (amount > 0 && units > 0) then amount * units else 0
  }
  metrics {
    total_records = count() over all_time
    total_derived = sum(derived_total) over all_time
    max_derived = max(derived_total) over all_time
  }
  views {
    view "series" {
      config = {"query_type":"SeriesQuery","response_type":"SeriesResponse","result_kind":"metric_series","count_metric":"total_records","group_by":{"bucket":{"field":"day_bucket"}},"filters":{"segment":{"field":"segment","op":"eq","type":"string","optional":true}},"metrics":{"total_derived":{"metric":"total_derived","label":"Total"}}}
    }
    view "scoped_series" {
      config = {"query_type":"ScopedSeriesQuery","response_type":"ScopedSeriesResponse","result_kind":"metric_series","count_metric":"total_records","group_by":{"bucket":{"field":"day_bucket"}},"filters":{"segment":{"field":"segment","op":"eq","type":"string"}},"response_fields":{"segment":{"from_filter":"segment"}},"metrics":{"total_derived":{"metric":"total_derived","label":"Total"},"max_derived":{"metric":"max_derived","label":"Max"}}}
    }
  }
}
"#
}

fn referenced_field_dsl() -> &'static str {
    r#"
tracker "referenced_pack" v1 {
  fields {
    thing_slug: text ref thing.slug
    score: int optional
  }
  metrics {
    total_events = count() over all_time
  }
  catalog {
    entry "thing" {
      fields = {"slug":{"type":"string"}}
    }
  }
  event_plans {
  }
}
"#
}

fn event_plan_draft_policy_dsl() -> &'static str {
    r#"
tracker "draft_policy_pack" v1 {
  fields {
    name: text
    score: int = 10
    flag: bool optional
  }
  metrics {
    total_events = count() over all_time
  }
  event_plans {
  }
}
"#
}

fn no_event_plans_dsl() -> &'static str {
    r#"
tracker "no_event_plans" v1 {
  fields {
    name: text
  }
  metrics {
    total_events = count() over all_time
  }
}
"#
}

#[test]
fn build_generates_core_artifacts_for_fixture_integration() {
    let temp = tempdir().expect("tempdir");
    let dsl_path = temp.path().join("sample_pack.tracker");
    let out_dir = temp.path().join("out");
    let generated_ts_dir = temp.path().join("generated");
    fs::write(&dsl_path, sample_dsl()).expect("write dsl");

    let output = build(&PackBuildConfig {
        dsl_path,
        out_dir,
        generated_ts_dir,
        base_source_paths: BTreeMap::new(),
    })
    .expect("build pack");

    assert!(output.rust_artifact_path.exists());
    assert!(output.dsl_contract_path.exists());
    assert!(output.api_contract_path.exists());
    assert!(output.domain_contract_path.exists());
}

#[test]
fn validate_pack_event_with_catalog_accepts_resolved_field_reference() {
    let compiled = CompiledPack::compile(referenced_field_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let event = json!({
        "event_id": "event-1",
        "ts": 1000,
        "payload": {
            "thing_slug": "bench",
            "score": 5
        }
    });
    let catalog = json!([{ "slug": "bench" }]);

    let normalized = runtime
        .validate_pack_event_with_catalog(&event.to_string(), &catalog)
        .expect("validate event");

    assert_eq!(normalized["payload"]["thing_slug"], "bench");
}

#[test]
fn validate_pack_event_with_catalog_rejects_missing_field_reference() {
    let compiled = CompiledPack::compile(referenced_field_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let event = json!({
        "event_id": "event-1",
        "ts": 1000,
        "payload": {
            "thing_slug": "missing"
        }
    });
    let catalog = json!([{ "slug": "bench" }]);

    let error = runtime
        .validate_pack_event_with_catalog(&event.to_string(), &catalog)
        .expect_err("missing reference should fail")
        .to_string();

    assert!(error.contains("references missing catalog entry"));
}

#[test]
fn validate_pack_event_with_catalog_rejects_duplicate_reference_targets() {
    let compiled = CompiledPack::compile(referenced_field_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let event = json!({
        "event_id": "event-1",
        "ts": 1000,
        "payload": {
            "thing_slug": "bench"
        }
    });
    let catalog = json!([{ "slug": "bench" }, { "slug": "bench" }]);

    let error = runtime
        .validate_pack_event_with_catalog(&event.to_string(), &catalog)
        .expect_err("duplicate references should fail")
        .to_string();

    assert!(error.contains("references non-unique catalog entry"));
}

#[test]
fn validate_event_plan_allows_partial_drafts_with_valid_present_refs() {
    let compiled = CompiledPack::compile(referenced_field_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let plan = json!({
        "id": " plan-1 ",
        "name": " Strength ",
        "revision": 2,
        "items": [
            {
                "id": " row-1 ",
                "payload": { "thing_slug": "bench" },
                "meta": { "section": "main" }
            }
        ]
    });
    let catalog = json!([{ "slug": "bench" }]);

    let normalized = runtime
        .validate_event_plan(&plan.to_string(), &catalog)
        .expect("validate plan");

    assert_eq!(normalized["id"], "plan-1");
    assert_eq!(normalized["items"][0]["id"], "row-1");
    assert_eq!(normalized["items"][0]["payload"]["thing_slug"], "bench");
}

#[test]
fn validate_event_plan_rejects_invalid_present_draft_fields() {
    let compiled = CompiledPack::compile(referenced_field_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let plan = json!({
        "id": "plan-1",
        "name": "Strength",
        "revision": 2,
        "items": [
            {
                "id": "row-1",
                "payload": { "thing_slug": "bench", "score": "heavy" }
            }
        ]
    });
    let catalog = json!([{ "slug": "bench" }]);

    let error = runtime
        .validate_event_plan(&plan.to_string(), &catalog)
        .expect_err("invalid draft should fail")
        .to_string();

    assert!(error.contains("invalid type/value"));
}

#[test]
fn validate_event_plan_uses_partial_draft_payload_policy() {
    let compiled = CompiledPack::compile(event_plan_draft_policy_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let plan = json!({
        "id": "plan-1",
        "name": "Drafts",
        "revision": 1,
        "items": [
            { "id": "row-1", "payload": { "flag": true } }
        ]
    });

    let normalized = runtime
        .validate_event_plan(&plan.to_string(), &json!([]))
        .expect("partial draft should not require missing fields or apply defaults");

    assert_eq!(normalized["items"][0]["payload"], json!({ "flag": true }));
    assert!(normalized["items"][0]["payload"].get("score").is_none());
    assert!(normalized["items"][0]["payload"].get("name").is_none());
}

#[test]
fn validate_event_plan_rejects_unknown_draft_payload_fields() {
    let compiled = CompiledPack::compile(event_plan_draft_policy_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let plan = json!({
        "id": "plan-1",
        "name": "Drafts",
        "revision": 1,
        "items": [
            { "id": "row-1", "payload": { "unknown": true } }
        ]
    });

    let error = runtime
        .validate_event_plan(&plan.to_string(), &json!([]))
        .expect_err("unknown draft fields should fail")
        .to_string();

    assert!(error.contains("unknown field"));
}

#[test]
fn validate_event_plan_requires_enabled_capability() {
    let compiled = CompiledPack::compile(no_event_plans_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let plan = json!({
        "id": "plan-1",
        "name": "Drafts",
        "revision": 1,
        "items": []
    });

    let error = runtime
        .validate_event_plan(&plan.to_string(), &json!([]))
        .expect_err("disabled event plans should fail")
        .to_string();

    assert!(error.contains("event plans are not enabled"));
}

#[test]
fn instantiate_event_plan_returns_ordered_event_drafts_without_event_identity() {
    let compiled = CompiledPack::compile(referenced_field_dsl()).expect("compile");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let plan = json!({
        "id": "plan-1",
        "name": "Strength",
        "revision": 2,
        "items": [
            { "id": "row-1", "payload": { "thing_slug": "bench" } },
            { "id": "row-2", "payload": { "thing_slug": "squat", "score": 7 } }
        ]
    });
    let catalog = json!([{ "slug": "bench" }, { "slug": "squat" }]);

    let drafts = runtime
        .instantiate_event_plan(&plan.to_string(), &catalog)
        .expect("instantiate plan");

    assert_eq!(drafts[0]["plan_id"], "plan-1");
    assert_eq!(drafts[0]["plan_revision"], 2);
    assert_eq!(drafts[0]["plan_item_id"], "row-1");
    assert_eq!(drafts[1]["payload"]["score"], 7);
    assert!(drafts[0].get("event_id").is_none());
    assert!(drafts[0].get("ts").is_none());
}

#[test]
fn runtime_validates_queries_against_declared_views_and_read_models() {
    let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
    let runtime = PackRuntime::new(compiled, StubAdapter);

    let view_plan = runtime
            .parse_query_json(r#"{"view":"metric_series","metric":"total_sets","group_by":"bucket","category":"focus"}"#)
            .expect("parse view query");
    assert!(matches!(view_plan, super::PackExecutionPlan::View(_)));

    let read_model_plan = runtime
        .parse_query_json(r#"{"read_model":"daily_rollup","bucket":1}"#)
        .expect("parse read model query");
    assert!(matches!(
        read_model_plan,
        super::PackExecutionPlan::ReadModel(_)
    ));

    let err = runtime
        .parse_query_json(r#"{"view":"metric_series","metric":"unknown","group_by":"bucket"}"#)
        .expect_err("unknown metric should fail");
    assert!(err.to_string().contains("metric"));

    let err = runtime
        .parse_query_json(
            r#"{"view":"metric_series","metric":"total_sets","group_by":"bucket","extra":"nope"}"#,
        )
        .expect_err("undeclared filter should fail");
    assert!(err.to_string().contains("filter"));
}

#[derive(Debug, Default)]
struct PanicAdapter;

impl super::PackExecutionAdapter for PanicAdapter {
    fn base_catalog(&self) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!([]))
    }

    fn validate_catalog_entry(
        &self,
        _entry_type: &str,
        _entry_json: &str,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({}))
    }

    fn execute(
        &self,
        _definition: &tracen_ir::TrackerDefinition,
        _events: &[PackInputEvent],
        _offset_minutes: i32,
        _catalog_json: &serde_json::Value,
        _plan: &super::PackExecutionPlan,
    ) -> Result<serde_json::Value, String> {
        panic!("view execution should not delegate to the adapter");
    }

    fn execute_read_model(
        &self,
        _definition: &tracen_ir::TrackerDefinition,
        _events: &[PackInputEvent],
        _offset_minutes: i32,
        _catalog_json: &serde_json::Value,
        _query: &super::ReadModelQueryPlan,
    ) -> Result<serde_json::Value, String> {
        panic!("read-model execution should not delegate to this adapter in generic tests");
    }
}

#[derive(Debug, Default)]
struct LegacyMirrorAdapter;

impl super::PackExecutionAdapter for LegacyMirrorAdapter {
    fn base_catalog(&self) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!([]))
    }

    fn validate_catalog_entry(
        &self,
        _entry_type: &str,
        _entry_json: &str,
    ) -> Result<serde_json::Value, String> {
        Ok(serde_json::json!({}))
    }

    fn execute(
        &self,
        definition: &tracen_ir::TrackerDefinition,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &serde_json::Value,
        plan: &super::PackExecutionPlan,
    ) -> Result<serde_json::Value, String> {
        match plan {
            super::PackExecutionPlan::View(view) => {
                super::execute_view_query(definition, events, offset_minutes, catalog_json, view)
                    .map_err(|err| err.to_string())
            }
            super::PackExecutionPlan::ReadModel(_) => {
                Err("legacy adapter path no longer owns read-model execution".to_string())
            }
        }
    }

    fn execute_read_model(
        &self,
        _definition: &tracen_ir::TrackerDefinition,
        _events: &[PackInputEvent],
        _offset_minutes: i32,
        _catalog_json: &serde_json::Value,
        _query: &super::ReadModelQueryPlan,
    ) -> Result<serde_json::Value, String> {
        Err("legacy adapter path no longer owns read-model execution".to_string())
    }
}

#[test]
fn runtime_executes_new_view_without_adapter_support() {
    let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
    let runtime = PackRuntime::new(compiled, PanicAdapter);
    let events = runtime
            .prepare_events_json(
                r#"[{"ts":1,"payload":{"category":"a","bucket":1}},{"ts":2,"payload":{"category":"b","bucket":1}},{"ts":3,"payload":{"category":"a","bucket":2}}]"#,
            )
            .expect("prepare events");
    let result = runtime
        .pack_query(
            &events,
            0,
            &serde_json::json!([]),
            r#"{"view":"category_dist","metric":"total_sets","group_by":"category"}"#,
        )
        .expect("execute view");

    let items = result
        .get("items")
        .and_then(Value::as_array)
        .expect("items");
    assert_eq!(items.len(), 2);
}

#[test]
fn runtime_applies_optional_filters_during_native_view_execution() {
    let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
    let runtime = PackRuntime::new(compiled, PanicAdapter);
    let events = runtime
            .prepare_events_json(
                r#"[{"ts":1,"payload":{"category":"a","bucket":1}},{"ts":2,"payload":{"category":"b","bucket":1}},{"ts":3,"payload":{"category":"a","bucket":2}}]"#,
            )
            .expect("prepare events");

    let filtered = runtime
        .pack_query(
            &events,
            0,
            &serde_json::json!([]),
            r#"{"view":"metric_series","metric":"total_sets","group_by":"bucket","category":"a"}"#,
        )
        .expect("execute filtered view");
    let points = filtered
        .get("points")
        .and_then(Value::as_array)
        .expect("points");
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].get("count").and_then(Value::as_i64), Some(1));
    assert_eq!(points[1].get("count").and_then(Value::as_i64), Some(1));
}

#[test]
fn runtime_scopes_metrics_to_selected_filter() {
    let compiled = CompiledPack::compile(filtered_scope_dsl()).expect("compile pack");
    let runtime = PackRuntime::new(compiled, PanicAdapter);
    let events = runtime
            .prepare_events_json(
                r#"[{"ts":1710000001000,"payload":{"segment":"alpha","amount":100,"units":5}},{"ts":1710000002000,"payload":{"segment":"alpha","amount":80,"units":8}},{"ts":1710000003000,"payload":{"segment":"beta","amount":150,"units":5}},{"ts":1710086401000,"payload":{"segment":"alpha","amount":90,"units":5}},{"ts":1710086402000,"payload":{"segment":"beta","amount":160,"units":5}}]"#,
            )
            .expect("prepare events");

    let day_one_bucket = tracen_analytics::round_to_local_day(1_710_000_001_000, 0);
    let day_two_bucket = tracen_analytics::round_to_local_day(1_710_086_401_000, 0);

    let series_filtered = runtime
        .pack_query(
            &events,
            0,
            &serde_json::json!([]),
            r#"{"view":"series","metric":"total_derived","group_by":"bucket","segment":"alpha"}"#,
        )
        .expect("execute series query");
    let series_points = series_filtered
        .get("points")
        .and_then(Value::as_array)
        .expect("series points");
    assert_eq!(series_points.len(), 2);
    assert_eq!(
        series_points[0].get("bucket").and_then(Value::as_i64),
        Some(day_one_bucket)
    );
    assert_eq!(
        series_points[0].get("value").and_then(Value::as_f64),
        Some(1140.0)
    );
    assert_eq!(
        series_points[0].get("count").and_then(Value::as_i64),
        Some(2)
    );
    assert_eq!(
        series_points[1].get("bucket").and_then(Value::as_i64),
        Some(day_two_bucket)
    );
    assert_eq!(
        series_points[1].get("value").and_then(Value::as_f64),
        Some(450.0)
    );
    assert_eq!(
        series_points[1].get("count").and_then(Value::as_i64),
        Some(1)
    );

    let scoped_total = runtime
            .pack_query(
                &events,
                0,
                &serde_json::json!([]),
                r#"{"view":"scoped_series","metric":"total_derived","group_by":"bucket","segment":"alpha"}"#,
            )
            .expect("execute scoped total query");
    let total_points = scoped_total
        .get("points")
        .and_then(Value::as_array)
        .expect("total points");
    assert_eq!(
        total_points[0].get("value").and_then(Value::as_f64),
        Some(1140.0)
    );
    assert_eq!(
        total_points[0].get("count").and_then(Value::as_i64),
        Some(2)
    );
    assert_eq!(
        total_points[1].get("value").and_then(Value::as_f64),
        Some(450.0)
    );
    assert_eq!(
        total_points[1].get("count").and_then(Value::as_i64),
        Some(1)
    );

    let scoped_max = runtime
            .pack_query(
                &events,
                0,
                &serde_json::json!([]),
                r#"{"view":"scoped_series","metric":"max_derived","group_by":"bucket","segment":"alpha"}"#,
            )
            .expect("execute scoped max query");
    let max_points = scoped_max
        .get("points")
        .and_then(Value::as_array)
        .expect("max points");
    assert_eq!(
        max_points[0].get("value").and_then(Value::as_f64),
        Some(640.0)
    );
    assert_eq!(max_points[0].get("count").and_then(Value::as_i64), Some(2));
    assert_eq!(
        max_points[1].get("value").and_then(Value::as_f64),
        Some(450.0)
    );
    assert_eq!(max_points[1].get("count").and_then(Value::as_i64), Some(1));
}

#[test]
fn runtime_legacy_fallback_matches_native_for_views() {
    let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
    let runtime_native = PackRuntime::new(compiled.clone(), PanicAdapter);
    let runtime_legacy = PackRuntime::new_with_options(
        compiled,
        LegacyMirrorAdapter,
        PackRuntimeOptions {
            use_legacy_adapter_for_queries: true,
        },
    );
    let events = runtime_native
            .prepare_events_json(
                r#"[{"ts":1,"payload":{"category":"a","bucket":1}},{"ts":2,"payload":{"category":"b","bucket":1}}]"#,
            )
            .expect("prepare events");
    let query = r#"{"view":"category_dist","metric":"total_sets","group_by":"category"}"#;
    let mut native_result = runtime_native
        .pack_query(&events, 0, &serde_json::json!([]), query)
        .expect("execute native view");
    let mut legacy_result = runtime_legacy
        .pack_query(&events, 0, &serde_json::json!([]), query)
        .expect("execute legacy view");
    for result in [&mut native_result, &mut legacy_result] {
        if let Some(items) = result.get_mut("items").and_then(Value::as_array_mut) {
            items.sort_by(|lhs, rhs| {
                let left = lhs.get("label").and_then(Value::as_str).unwrap_or_default();
                let right = rhs.get("label").and_then(Value::as_str).unwrap_or_default();
                left.cmp(right)
            });
        }
    }

    assert_eq!(legacy_result, native_result);
}

#[test]
fn runtime_applies_runtime_time_semantics_to_events() {
    let events = vec![PackInputEvent {
        ts: 1_710_000_000_000,
        payload: serde_json::json!({
            "day_bucket": 1,
            "week_bucket": 2,
            "month_bucket": 3,
            "segment": "alpha",
        }),
    }];

    let normalized = super::apply_runtime_time_semantics(&events, 0);
    let payload = normalized[0].payload.as_object().expect("object payload");

    assert_ne!(payload.get("day_bucket").and_then(Value::as_i64), Some(1));
    assert_ne!(payload.get("week_bucket").and_then(Value::as_i64), Some(2));
    assert_ne!(payload.get("month_bucket").and_then(Value::as_i64), Some(3));
    assert_eq!(
        payload.get("segment").and_then(Value::as_str),
        Some("alpha")
    );
}

#[test]
fn runtime_delegates_read_models_through_adapter_contract() {
    let compiled = CompiledPack::compile(sample_dsl()).expect("compile pack");
    let runtime = PackRuntime::new(compiled, StubAdapter);
    let result = runtime
        .pack_query(
            &[],
            0,
            &serde_json::json!([]),
            r#"{"read_model":"daily_rollup","bucket":42}"#,
        )
        .expect("execute read model");

    assert_eq!(
        result.get("read_model").and_then(Value::as_str),
        Some("daily_rollup")
    );
    assert_eq!(
        result
            .get("params")
            .and_then(Value::as_object)
            .and_then(|params| params.get("bucket"))
            .and_then(Value::as_i64),
        Some(42)
    );
}
