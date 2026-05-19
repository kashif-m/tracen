//! Build-time pack integration and generic runtime execution boundaries.

mod build;
mod catalog_references;
mod event_plans;
mod query;
mod runtime_events;
mod view_runtime;

use serde_json::Value;
use std::sync::Arc;
use thiserror::Error;
use tracen_ir::TrackerDefinition;

pub use build::{build, PackBuildConfig, PackBuildOutput};
pub use query::{PackExecutionPlan, ReadModelQueryPlan, ViewQueryPlan};
pub use runtime_events::PackInputEvent;

use query::parse_query_json;
use runtime_events::{
    apply_runtime_time_semantics, prepare_pack_events, to_pack_event_error,
    validate_event_references,
};
use view_runtime::execute_view_query;

#[derive(Debug, Error)]
pub enum PackError {
    #[error("dsl compile failed: {0}")]
    Compile(String),
    #[error("build io failed: {0}")]
    Io(String),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("event preparation failed: {0}")]
    Event(String),
    #[error("adapter failed: {0}")]
    Adapter(String),
}

#[derive(Debug, Clone)]
pub struct CompiledPack {
    definition: TrackerDefinition,
    capabilities: Value,
}

impl CompiledPack {
    pub fn compile(dsl: &str) -> Result<Self, PackError> {
        let definition = tracen_dsl::compile(dsl).map_err(|err| PackError::Compile(err.message))?;
        Self::from_definition(definition)
    }

    pub fn from_definition(definition: TrackerDefinition) -> Result<Self, PackError> {
        let model = tracen_pack_codegen::PackGenModel::from_tracker(&definition)
            .map_err(PackError::Compile)?;
        let capabilities = serde_json::from_str(&model.capabilities_json)
            .map_err(|err| PackError::Compile(format!("deserialize capabilities: {err}")))?;
        Ok(Self::from_precomputed(definition, capabilities))
    }

    pub fn from_precomputed(definition: TrackerDefinition, capabilities: Value) -> Self {
        Self {
            definition,
            capabilities,
        }
    }

    pub fn definition(&self) -> &TrackerDefinition {
        &self.definition
    }

    pub fn capabilities(&self) -> &Value {
        &self.capabilities
    }
}

#[derive(Debug, Clone)]
pub struct PackRuntime<A> {
    compiled: Arc<CompiledPack>,
    adapter: A,
    options: PackRuntimeOptions,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct PackRuntimeOptions {
    pub use_legacy_adapter_for_queries: bool,
}

impl<A> PackRuntime<A>
where
    A: PackExecutionAdapter,
{
    pub fn new(compiled: CompiledPack, adapter: A) -> Self {
        Self::new_with_options(compiled, adapter, PackRuntimeOptions::default())
    }

    pub fn new_shared(compiled: Arc<CompiledPack>, adapter: A) -> Self {
        Self::new_shared_with_options(compiled, adapter, PackRuntimeOptions::default())
    }

    pub fn new_with_options(
        compiled: CompiledPack,
        adapter: A,
        options: PackRuntimeOptions,
    ) -> Self {
        Self::new_shared_with_options(Arc::new(compiled), adapter, options)
    }

    pub fn new_shared_with_options(
        compiled: Arc<CompiledPack>,
        adapter: A,
        options: PackRuntimeOptions,
    ) -> Self {
        Self {
            compiled,
            adapter,
            options,
        }
    }

    pub fn compiled(&self) -> &CompiledPack {
        self.compiled.as_ref()
    }

    pub fn pack_capabilities(&self) -> Value {
        self.compiled.capabilities().clone()
    }

    pub fn pack_base_catalog(&self) -> Result<Value, PackError> {
        self.adapter.base_catalog().map_err(PackError::Adapter)
    }

    pub fn validate_pack_event(&self, event_json: &str) -> Result<Value, PackError> {
        let normalized = tracen_engine::validate_event(self.compiled.definition(), event_json)
            .map_err(to_pack_event_error)?;
        serde_json::to_value(normalized).map_err(|err| PackError::Event(err.to_string()))
    }

    pub fn validate_pack_event_with_catalog(
        &self,
        event_json: &str,
        catalog_json: &Value,
    ) -> Result<Value, PackError> {
        let normalized = tracen_engine::validate_event(self.compiled.definition(), event_json)
            .map_err(to_pack_event_error)?;
        validate_event_references(self.compiled.definition(), &normalized, catalog_json)?;
        serde_json::to_value(normalized).map_err(|err| PackError::Event(err.to_string()))
    }

    pub fn validate_event_plan(
        &self,
        plan_json: &str,
        catalog_json: &Value,
    ) -> Result<Value, PackError> {
        let plan =
            event_plans::validate_event_plan(self.compiled.definition(), plan_json, catalog_json)?;
        serde_json::to_value(plan).map_err(|err| PackError::Event(err.to_string()))
    }

    pub fn instantiate_event_plan(
        &self,
        plan_json: &str,
        catalog_json: &Value,
    ) -> Result<Value, PackError> {
        let drafts = event_plans::instantiate_event_plan(
            self.compiled.definition(),
            plan_json,
            catalog_json,
        )?;
        serde_json::to_value(drafts).map_err(|err| PackError::Event(err.to_string()))
    }

    pub fn validate_pack_catalog_entry(
        &self,
        entry_type: &str,
        entry_json: &str,
    ) -> Result<Value, PackError> {
        self.adapter
            .validate_catalog_entry(entry_type, entry_json)
            .map_err(PackError::Adapter)
    }

    pub fn parse_query_json(&self, query_json: &str) -> Result<PackExecutionPlan, PackError> {
        parse_query_json(self.compiled.definition(), query_json)
    }

    pub fn prepare_events_json(&self, events_json: &str) -> Result<Vec<PackInputEvent>, PackError> {
        let events: Vec<PackInputEvent> = serde_json::from_str(events_json)
            .map_err(|err| PackError::Event(format!("parse pack events: {err}")))?;
        prepare_pack_events(self.compiled.definition(), &events)
    }

    pub fn pack_query(
        &self,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &Value,
        query_json: &str,
    ) -> Result<Value, PackError> {
        let plan = self.parse_query_json(query_json)?;
        let normalized_events = apply_runtime_time_semantics(events, offset_minutes);
        let result = if self.options.use_legacy_adapter_for_queries {
            self
                .adapter
                .execute(
                    self.compiled.definition(),
                    &normalized_events,
                    offset_minutes,
                    catalog_json,
                    &plan,
                )
                .map_err(PackError::Adapter)
        } else {
            match &plan {
                PackExecutionPlan::View(view) => self
                    .adapter
                    .execute_view_query(
                        self.compiled.definition(),
                        &normalized_events,
                        offset_minutes,
                        catalog_json,
                        view,
                    )
                    .map_err(PackError::Adapter),
                PackExecutionPlan::ReadModel(read_model) => self
                    .adapter
                    .execute_read_model(
                        self.compiled.definition(),
                        &normalized_events,
                        offset_minutes,
                        catalog_json,
                        read_model,
                    )
                    .map_err(PackError::Adapter),
            }
        };
        result
    }
}

pub trait PackExecutionAdapter {
    fn base_catalog(&self) -> Result<Value, String>;
    fn validate_catalog_entry(&self, entry_type: &str, entry_json: &str) -> Result<Value, String>;
    fn execute(
        &self,
        definition: &TrackerDefinition,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &Value,
        plan: &PackExecutionPlan,
    ) -> Result<Value, String>;
    fn execute_view_query(
        &self,
        definition: &TrackerDefinition,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &Value,
        query: &ViewQueryPlan,
    ) -> Result<Value, String> {
        execute_view_query(
            definition,
            events,
            offset_minutes,
            catalog_json,
            query,
        )
        .map_err(|err| err.to_string())
    }
    fn execute_read_model(
        &self,
        definition: &TrackerDefinition,
        events: &[PackInputEvent],
        offset_minutes: i32,
        catalog_json: &Value,
        query: &ReadModelQueryPlan,
    ) -> Result<Value, String>;
}

#[cfg(test)]
mod tests;
