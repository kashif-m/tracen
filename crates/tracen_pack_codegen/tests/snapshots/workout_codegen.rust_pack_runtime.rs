// Auto-generated pack runtime for workout_codegen.
// Generated from DSL version 1.0.0.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, OnceLock};

pub const WORKOUT_CODEGEN_TRACKER_DSL: &str = r#"tracker "workout_codegen" v1 {
  fields {
    exercise: text
    duration: int
    calories: float
  }

  metrics {
    total_calories = sum(calories) over all_time
    total_duration = sum(duration) over all_time
    sessions = count() over all_time
  }

  views {
    view "summary" {
      config = {"default_metric":"total_calories","query_type":"SummaryQuery","result_kind":"metric_series","response_type":"SummaryResponse","group_by":{"exercise":{"field":"exercise"}},"metrics":{"total_calories":{"metric":"total_calories","label":"Total Calories"},"total_duration":{"metric":"total_duration","label":"Total Duration"}}}
    }
  }
}
"#;
pub const WORKOUT_CODEGEN_TRACKER_JSON: &str = r#"{"tracker_id":"workout_codegen_v1_8a232c54","tracker_id_override":null,"tracker_name":"workout_codegen","version":{"major":1,"minor":0,"patch":0},"dsl":"tracker \"workout_codegen\" v1 {\n  fields {\n    exercise: text\n    duration: int\n    calories: float\n  }\n\n  metrics {\n    total_calories = sum(calories) over all_time\n    total_duration = sum(duration) over all_time\n    sessions = count() over all_time\n  }\n\n  views {\n    view \"summary\" {\n      config = {\"default_metric\":\"total_calories\",\"query_type\":\"SummaryQuery\",\"result_kind\":\"metric_series\",\"response_type\":\"SummaryResponse\",\"group_by\":{\"exercise\":{\"field\":\"exercise\"}},\"metrics\":{\"total_calories\":{\"metric\":\"total_calories\",\"label\":\"Total Calories\"},\"total_duration\":{\"metric\":\"total_duration\",\"label\":\"Total Duration\"}}}\n    }\n  }\n}\n","fields":[{"name":"exercise","field_type":"Text","optional":false,"default_value":null,"reference":null},{"name":"duration","field_type":"Int","optional":false,"default_value":null,"reference":null},{"name":"calories","field_type":"Float","optional":false,"default_value":null,"reference":null}],"derives":[],"metrics":[{"name":"total_calories","aggregation":{"func":"Sum","target":{"Field":"calories"},"group_by":[],"over":"AllTime"}},{"name":"total_duration","aggregation":{"func":"Sum","target":{"Field":"duration"},"group_by":[],"over":"AllTime"}},{"name":"sessions","aggregation":{"func":"Count","target":null,"group_by":[],"over":"AllTime"}}],"alerts":[],"planning":null,"event_plans":null,"views":[{"name":"summary","params":{"config":{"default_metric":"total_calories","group_by":{"exercise":{"field":"exercise"}},"metrics":{"total_calories":{"label":"Total Calories","metric":"total_calories"},"total_duration":{"label":"Total Duration","metric":"total_duration"}},"query_type":"SummaryQuery","response_type":"SummaryResponse","result_kind":"metric_series"}}}],"catalog":[],"read_models":[],"types":[],"helpers":[],"imports":[],"extern_ts":[],"compat":null}"#;
pub const WORKOUT_CODEGEN_VIEW_METRICS_JSON: &str = r#"{"summary":["total_calories","total_duration"]}"#;
pub const WORKOUT_CODEGEN_VIEW_DEFAULT_METRICS_JSON: &str = r#"{"summary":"total_calories"}"#;
pub const WORKOUT_CODEGEN_VIEW_METRIC_CONFIG_JSON: &str = r#"{"summary":{"total_calories":{"metric":"total_calories","label":"Total Calories","unit":null,"modes":[],"requires":[]},"total_duration":{"metric":"total_duration","label":"Total Duration","unit":null,"modes":[],"requires":[]}}}"#;
pub const WORKOUT_CODEGEN_CAPABILITIES_JSON: &str = r#"{"catalog":{},"event_plans":{"enabled":false},"read_models":{},"views":{"summary":{"default_metric":"total_calories","filters":[],"group_by":["exercise"],"metric_config":{"total_calories":{"label":"Total Calories","metric":"total_calories","modes":[],"requires":[],"unit":null},"total_duration":{"label":"Total Duration","metric":"total_duration","modes":[],"requires":[],"unit":null}},"metrics":["total_calories","total_duration"],"query_type":"SummaryQuery","response_type":"SummaryResponse","result_kind":"metric_series"}}}"#;


pub const WORKOUT_CODEGEN_METRIC_NAMES: &[&str] = &[
    "sessions",
    "total_calories",
    "total_duration",
];



pub trait WorkoutCodegenHelpers {
}

pub struct GeneratedWorkoutCodegenPackAdapter<H> {
    helpers: H,
}

impl<H> GeneratedWorkoutCodegenPackAdapter<H> {
    pub fn new(helpers: H) -> Self {
        Self { helpers }
    }

    pub fn helpers(&self) -> &H {
        &self.helpers
    }
}

impl<H> GeneratedWorkoutCodegenPackAdapter<H>
where
    H: WorkoutCodegenHelpers,
{
    pub fn import_by_name(&self, import_name: &str, args: &[Value]) -> Result<Value, String> {
        match import_name {
            other => Err(format!("unknown import helper: {other}")),
        }
    }
}

impl<H> tracen_pack::PackExecutionAdapter for GeneratedWorkoutCodegenPackAdapter<H>
where
    H: WorkoutCodegenHelpers,
{
    fn base_catalog(&self) -> Result<Value, String> {
        let catalogs: Vec<Value> = vec![
        ];
        match catalogs.as_slice() {
            [] => Ok(Value::Array(Vec::new())),
            [catalog] => Ok(catalog.clone()),
            _ => Err("generated pack error: multiple base catalog sources are not yet supported by pack_base_catalog".to_string()),
        }
    }

    fn validate_catalog_entry(&self, entry_type: &str, entry_json: &str) -> Result<Value, String> {
        match entry_type {
            other => Err(format!("unknown catalog entry type: {other}")),
        }
    }

    fn execute(
        &self,
        _definition: &tracen_ir::TrackerDefinition,
        _events: &[tracen_pack::PackInputEvent],
        _offset_minutes: i32,
        _catalog_json: &Value,
        _plan: &tracen_pack::PackExecutionPlan,
    ) -> Result<Value, String> {
        Err("adapter execution removed: tracen_pack runtime owns query execution".to_string())
    }

    fn execute_read_model(
        &self,
        _definition: &tracen_ir::TrackerDefinition,
        events: &[tracen_pack::PackInputEvent],
        offset_minutes: i32,
        catalog_json: &Value,
        query: &tracen_pack::ReadModelQueryPlan,
    ) -> Result<Value, String> {
        match query.read_model_name.as_str() {
            other => Err(format!("unknown read_model: {other}")),
        }
    }
}

fn cached_workout_codegen_compiled_pack() -> &'static Arc<tracen_pack::CompiledPack> {
    static COMPILED: OnceLock<Arc<tracen_pack::CompiledPack>> = OnceLock::new();
    COMPILED.get_or_init(|| {
        let definition: tracen_ir::TrackerDefinition = serde_json::from_str(WORKOUT_CODEGEN_TRACKER_JSON)
            .expect("generated tracker JSON must deserialize");
        let capabilities: Value = serde_json::from_str(WORKOUT_CODEGEN_CAPABILITIES_JSON)
            .expect("generated tracker capabilities JSON must deserialize");
        Arc::new(tracen_pack::CompiledPack::from_precomputed(definition, capabilities))
    })
}

pub fn compiled_workout_codegen_definition() -> tracen_ir::TrackerDefinition {
    cached_workout_codegen_compiled_pack().definition().clone()
}

pub fn compiled_workout_codegen_capabilities() -> Value {
    cached_workout_codegen_compiled_pack().capabilities().clone()
}

pub fn compiled_workout_codegen_tracker_id() -> &'static str {
    "workout_codegen_v1_8a232c54"
}

pub fn workout_codegen_pack_runtime<H>(helpers: H) -> Result<tracen_pack::PackRuntime<GeneratedWorkoutCodegenPackAdapter<H>>, tracen_pack::PackError>
where
    H: WorkoutCodegenHelpers,
{
    Ok(tracen_pack::PackRuntime::new_shared(
        Arc::clone(cached_workout_codegen_compiled_pack()),
        GeneratedWorkoutCodegenPackAdapter::new(helpers),
    ))
}
