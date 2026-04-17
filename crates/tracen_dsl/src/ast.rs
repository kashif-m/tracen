//! AST types for tracker DSL.

use serde::{Deserialize, Serialize};
use tracen_ir::{
    AlertDefinition, CatalogEntryDefinition, CompatDefinition, DeriveDefinition,
    ExternTsImportDefinition, FieldDefinition, HelperDefinition, ImportDefinition,
    MetricDefinition, PackTypeDefinition, PlanningDefinition, ReadModelDefinition, TrackerVersion,
    ViewDefinition,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerAst {
    pub name: String,
    pub version: TrackerVersion,
    pub fields: Vec<FieldDefinition>,
    pub derives: Vec<DeriveDefinition>,
    pub metrics: Vec<MetricDefinition>,
    pub alerts: Vec<AlertDefinition>,
    pub planning: Option<PlanningDefinition>,
    pub views: Vec<ViewDefinition>,
    pub catalog: Vec<CatalogEntryDefinition>,
    pub read_models: Vec<ReadModelDefinition>,
    pub types: Vec<PackTypeDefinition>,
    pub helpers: Vec<HelperDefinition>,
    pub imports: Vec<ImportDefinition>,
    pub extern_ts: Vec<ExternTsImportDefinition>,
    pub compat: Option<CompatDefinition>,
}
