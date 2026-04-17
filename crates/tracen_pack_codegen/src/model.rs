//! Model building for pack code generation.

use crate::naming::{
    dsl_ident_to_screaming_snake_case, dsl_ident_to_snake_case, dsl_ident_to_ts_type_name,
    enum_variant_to_rust_ident, render_rust_type, render_ts_contract_type,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use tracen_ir::{
    CompatDefinition, ExternTsImportDefinition, FilterOperator, ImportDefinition,
    PackTypeDefinition, PackTypeKind, SchemaFieldDefinition, TrackerDefinition, ViewDefinition,
};

/// Model used for code generation from a compiled tracker definition.
#[derive(Debug, Serialize)]
pub struct PackGenModel {
    pub tracker_name: String,
    pub tracker_id: String,
    pub tracker_type: String,
    pub tracker_const: String,
    pub tracker_fn: String,
    pub tracker_mod: String,
    pub core_api_contract_module: String,
    pub core_domain_contract_module: String,
    pub compat_api_contract_file: String,
    pub compat_domain_contract_file: String,
    pub compat_dsl_contract_file: String,
    pub compat_api_contract_module: String,
    pub compat_domain_contract_module: String,
    pub compat_dsl_contract_module: String,
    pub analytics_capabilities_type: String,
    pub version: String,
    pub dsl: String,
    pub tracker_json: String,
    pub metric_names: Vec<String>,
    pub view_metrics: BTreeMap<String, Vec<String>>,
    pub view_defaults: BTreeMap<String, String>,
    pub view_metric_config: BTreeMap<String, BTreeMap<String, ViewMetricConfig>>,
    pub view_group_by: BTreeMap<String, Vec<GroupByModel>>,
    pub view_filters: BTreeMap<String, Vec<FilterModel>>,
    pub views: Vec<ViewModel>,
    pub base_catalog_sources: Vec<BaseCatalogSourceModel>,
    pub catalog_entries: Vec<CatalogEntryModel>,
    pub read_models: Vec<ReadModelModel>,
    pub api_types: Vec<TypeModel>,
    pub domain_types: Vec<TypeModel>,
    pub rust_types: Vec<TypeModel>,
    pub extern_ts_imports: Vec<ExternTsImportModel>,
    pub helper_trait_name: String,
    pub helper_impl_type_name: String,
    pub generated_adapter_type_name: String,
    pub helpers: Vec<HelperModel>,
    pub imports: Vec<HelperModel>,
    pub capabilities_json: String,
    pub view_metrics_json: String,
    pub view_default_metrics_json: String,
    pub view_metric_config_json: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct ViewMetricConfig {
    pub metric: String,
    pub label: String,
    pub unit: Option<String>,
    pub modes: Vec<String>,
    pub requires: Vec<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct GroupByModel {
    pub key: String,
    pub field: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct FilterModel {
    pub key: String,
    pub field: String,
    pub op: String,
    pub ts_type: String,
    pub rendered_ts_type: String,
    pub rust_type: String,
    pub optional: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct ResponseFieldModel {
    pub name: String,
    pub source: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct ViewModel {
    pub name: String,
    pub name_pascal: String,
    pub metric_type_name: String,
    pub group_by_type_name: String,
    pub query_type: String,
    pub pack_query_type: String,
    pub response_type: String,
    pub result_kind: String,
    pub is_metric_series: bool,
    pub is_distribution: bool,
    pub group_by_keys: Vec<String>,
    pub filters: Vec<FilterModel>,
    pub response_fields: Vec<ResponseFieldModel>,
    pub point_type: Option<String>,
    pub totals_type: Option<String>,
    pub totals_fields: Vec<ViewTotalFieldModel>,
    pub query_filter_field: Option<String>,
    pub query_filter_type: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ViewTotalFieldModel {
    pub name: String,
    pub rendered_ts_type: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct BaseCatalogSourceModel {
    pub name: String,
    pub json_const: String,
    pub json: String,
    pub has_json: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct CatalogEntryModel {
    pub name: String,
    pub name_pascal: String,
    pub base_source: Option<String>,
    pub compat_base_type: String,
    pub compat_overlay_type: Option<String>,
    pub compat_overlay_source_type: String,
    pub validate_helper: Option<String>,
    pub validate_helper_method: Option<String>,
    pub fields: Vec<TypeFieldModel>,
}

#[derive(Debug, Serialize, Clone)]
pub struct TypeFieldModel {
    pub name: String,
    pub ts_type: String,
    pub rendered_ts_type: String,
    pub rust_type: String,
    pub optional: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct TypeVariantModel {
    pub value: String,
    pub rust_ident: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct TypeModel {
    pub name: String,
    pub kind: String,
    pub is_object: bool,
    pub is_enum: bool,
    pub is_alias: bool,
    pub emit_ts: bool,
    pub emit_rust: bool,
    pub contract: Option<String>,
    pub fields: Vec<TypeFieldModel>,
    pub variants: Vec<TypeVariantModel>,
    pub target: Option<String>,
    pub rust_target: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ReadModelModel {
    pub name: String,
    pub name_pascal: String,
    pub rust_method_name: String,
    pub query_type: String,
    pub pack_query_type: String,
    pub response_type: String,
    pub response_rust_type: String,
    pub emit_rust_response_struct: bool,
    pub params: Vec<TypeFieldModel>,
    pub filters: Vec<FilterModel>,
    pub fields: Vec<TypeFieldModel>,
}

#[derive(Debug, Serialize, Clone)]
pub struct HelperParamModel {
    pub name: String,
    pub ts_type: String,
    pub rendered_ts_type: String,
    pub rust_type: String,
    pub optional: bool,
}

#[derive(Debug, Serialize, Clone)]
pub struct HelperModel {
    pub name: String,
    pub rust_method_name: String,
    pub ts_method_name: Option<String>,
    pub native_export_name: Option<String>,
    pub fallible: bool,
    pub params: Vec<HelperParamModel>,
    pub return_type: String,
    pub rendered_return_ts_type: String,
    pub rust_return_type: String,
    pub param_count: usize,
}

#[derive(Debug, Serialize, Clone)]
pub struct ExternTsImportModel {
    pub module: String,
    pub names: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct ViewConfig {
    #[serde(default)]
    default_metric: Option<String>,
    #[serde(default)]
    query_type: Option<String>,
    #[serde(default)]
    response_type: Option<String>,
    #[serde(default)]
    result_kind: Option<String>,
    #[serde(default)]
    metrics: BTreeMap<String, MetricConfig>,
    #[serde(default)]
    group_by: BTreeMap<String, GroupByConfig>,
    #[serde(default)]
    filters: BTreeMap<String, FilterConfig>,
    #[serde(default)]
    response_fields: BTreeMap<String, ResponseFieldConfig>,
    #[serde(default)]
    totals: BTreeMap<String, TotalFieldConfig>,
}

#[derive(Debug, serde::Deserialize)]
struct MetricConfig {
    metric: String,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    unit: Option<String>,
    #[serde(default)]
    modes: Vec<String>,
    #[serde(default)]
    requires: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
struct GroupByConfig {
    field: String,
}

#[derive(Debug, serde::Deserialize)]
struct FilterConfig {
    field: String,
    #[serde(default = "default_filter_op")]
    op: String,
    #[serde(rename = "type")]
    type_ref: String,
    #[serde(default)]
    optional: bool,
}

#[derive(Debug, serde::Deserialize)]
struct ResponseFieldConfig {
    #[serde(default)]
    from_filter: Option<String>,
    #[serde(default)]
    from_param: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct TotalFieldConfig {
    #[serde(default)]
    coerce: Option<String>,
}

impl PackGenModel {
    /// Build a pack generation model from a compiled tracker definition.
    pub fn from_tracker(def: &TrackerDefinition) -> Result<Self, String> {
        Self::from_tracker_internal(def, &BTreeMap::new(), false)
    }

    pub fn from_tracker_with_base_sources(
        def: &TrackerDefinition,
        base_sources: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        Self::from_tracker_internal(def, base_sources, true)
    }

    fn from_tracker_internal(
        def: &TrackerDefinition,
        base_sources: &BTreeMap<String, String>,
        validate_base_sources: bool,
    ) -> Result<Self, String> {
        let tracker_name = def.tracker_name().to_string();
        let tracker_id = def.tracker_id().as_str().to_string();
        let tracker_type = to_pascal_case(&tracker_name);
        let tracker_const = to_screaming_snake_case(&tracker_name);
        let tracker_fn = to_snake_case(&tracker_name);
        let tracker_mod = to_snake_case(&tracker_name);
        let core_api_contract_module = format!("{tracker_fn}PackCoreApiContract");
        let core_domain_contract_module = format!("{tracker_fn}PackCoreDomainContract");
        let compat = def.compat().cloned().unwrap_or_default();
        let compat_dsl_contract_file = compat
            .ts_dsl_contract
            .clone()
            .unwrap_or_else(|| format!("{tracker_fn}DslContract.ts"));
        let compat_api_contract_file = compat
            .ts_api_contract
            .clone()
            .unwrap_or_else(|| format!("{tracker_fn}ApiContract.ts"));
        let compat_domain_contract_file = compat
            .ts_domain_contract
            .clone()
            .unwrap_or_else(|| format!("{tracker_fn}DomainContract.ts"));
        let compat_dsl_contract_module = strip_ts_extension(&compat_dsl_contract_file);
        let compat_api_contract_module = strip_ts_extension(&compat_api_contract_file);
        let compat_domain_contract_module = strip_ts_extension(&compat_domain_contract_file);
        let analytics_capabilities_type = compat
            .analytics_capabilities_type
            .clone()
            .unwrap_or_else(|| format!("{tracker_type}AnalyticsCapabilities"));
        let version = format!(
            "{}.{}.{}",
            def.version().major,
            def.version().minor,
            def.version().patch
        );

        let mut metric_names: Vec<String> = def.metrics().iter().map(|m| m.name.clone()).collect();
        metric_names.sort();

        let extern_ts_map = build_extern_ts_rust_map(def.extern_ts());
        let view_data = extract_view_configs(def.views(), &compat, &extern_ts_map)?;
        let base_catalog_sources = collect_base_catalog_sources(
            &tracker_name,
            &tracker_const,
            def.catalog(),
            base_sources,
            validate_base_sources,
        )?;
        let catalog_entries = def
            .catalog()
            .iter()
            .map(|entry| {
                let compat_base_type = entry
                    .compat_base_type
                    .clone()
                    .unwrap_or_else(|| format!("{}CatalogEntry", to_pascal_case(&entry.name)));
                CatalogEntryModel {
                    name: entry.name.clone(),
                    name_pascal: to_pascal_case(&entry.name),
                    base_source: entry.base_source.clone(),
                    compat_base_type,
                    compat_overlay_type: entry.compat_overlay_type.clone(),
                    compat_overlay_source_type: entry
                        .compat_overlay_source_type
                        .clone()
                        .unwrap_or_else(|| "BrandedString".to_string()),
                    validate_helper: entry.validate_helper.clone(),
                    validate_helper_method: entry
                        .validate_helper
                        .as_ref()
                        .map(|name| dsl_ident_to_snake_case(name)),
                    fields: entry
                        .fields
                        .iter()
                        .map(|field| build_type_field_model(field, &extern_ts_map))
                        .collect(),
                }
            })
            .collect::<Vec<_>>();

        let type_models = def
            .types()
            .iter()
            .map(|type_def| build_type_model(type_def, &extern_ts_map))
            .collect::<Vec<_>>();
        let required_rust_types = collect_required_rust_type_names(def);
        let rust_types = type_models
            .iter()
            .filter(|type_def| type_def.emit_rust || required_rust_types.contains(&type_def.name))
            .map(|type_def| {
                let mut type_def = type_def.clone();
                type_def.emit_rust = true;
                type_def
            })
            .collect::<Vec<_>>();
        let rust_type_names = rust_types
            .iter()
            .map(|type_def| type_def.name.clone())
            .collect::<BTreeSet<_>>();

        let read_models = def
            .read_models()
            .iter()
            .map(|model| {
                let response_type = model
                    .response_type
                    .clone()
                    .unwrap_or_else(|| format!("{}Response", to_pascal_case(&model.name)));
                ReadModelModel {
                    name: model.name.clone(),
                    name_pascal: to_pascal_case(&model.name),
                    rust_method_name: format!(
                        "read_model_{}",
                        dsl_ident_to_snake_case(&model.name)
                    ),
                    query_type: model
                        .query_type
                        .clone()
                        .unwrap_or_else(|| format!("{}Query", to_pascal_case(&model.name))),
                    pack_query_type: format!("{}PackQuery", to_pascal_case(&model.name)),
                    response_rust_type: resolve_rust_type(&response_type, &extern_ts_map),
                    emit_rust_response_struct: !rust_type_names.contains(&response_type),
                    response_type,
                    params: model
                        .params
                        .iter()
                        .map(|field| build_type_field_model(field, &extern_ts_map))
                        .collect(),
                    filters: model
                        .filters
                        .iter()
                        .map(|filter| {
                            build_filter_model(
                                filter.key.clone(),
                                filter.field.clone(),
                                filter.op,
                                filter.type_ref.clone(),
                                filter.optional,
                                &extern_ts_map,
                            )
                        })
                        .collect(),
                    fields: model
                        .fields
                        .iter()
                        .map(|field| build_type_field_model(field, &extern_ts_map))
                        .collect(),
                }
            })
            .collect::<Vec<_>>();

        let api_types = type_models
            .iter()
            .filter(|type_def| {
                type_def.emit_ts && matches!(type_def.contract.as_deref(), Some("api"))
            })
            .cloned()
            .collect::<Vec<_>>();
        let domain_types = type_models
            .iter()
            .filter(|type_def| {
                type_def.emit_ts && matches!(type_def.contract.as_deref(), Some("domain"))
            })
            .cloned()
            .collect::<Vec<_>>();
        let helper_trait_name = format!("{}Helpers", tracker_type);
        let helper_impl_type_name = format!("{}HelperImpl", tracker_type);
        let generated_adapter_type_name = format!("Generated{}PackAdapter", tracker_type);
        let helpers = def
            .helpers()
            .iter()
            .map(|helper| build_helper_model(helper, &extern_ts_map))
            .collect::<Vec<_>>();
        let imports = def
            .imports()
            .iter()
            .map(|import| build_import_model(import, &extern_ts_map))
            .collect::<Vec<_>>();
        let extern_ts_imports = def
            .extern_ts()
            .iter()
            .map(|import| ExternTsImportModel {
                module: import.module.clone(),
                names: import.items.iter().map(|item| item.name.clone()).collect(),
            })
            .collect::<Vec<_>>();

        let capabilities_json = serde_json::to_string(&build_capabilities(
            &view_data,
            &catalog_entries,
            &read_models,
        ))
        .map_err(|e| format!("failed to serialize capabilities: {e}"))?;

        let tracker_json =
            serde_json::to_string(def).map_err(|e| format!("failed to serialize tracker: {e}"))?;

        let view_metrics_json = serde_json::to_string(&view_data.view_metrics)
            .map_err(|e| format!("failed to serialize view metrics: {e}"))?;
        let view_default_metrics_json = serde_json::to_string(&view_data.view_defaults)
            .map_err(|e| format!("failed to serialize default metrics: {e}"))?;
        let view_metric_config_json = serde_json::to_string(&view_data.view_metric_config)
            .map_err(|e| format!("failed to serialize view metric config: {e}"))?;

        Ok(Self {
            tracker_name,
            tracker_id,
            tracker_type,
            tracker_const,
            tracker_fn,
            tracker_mod,
            core_api_contract_module,
            core_domain_contract_module,
            compat_api_contract_file,
            compat_domain_contract_file,
            compat_dsl_contract_file,
            compat_api_contract_module,
            compat_domain_contract_module,
            compat_dsl_contract_module,
            analytics_capabilities_type,
            version,
            dsl: def.dsl().to_string(),
            tracker_json,
            metric_names,
            view_metrics: view_data.view_metrics,
            view_defaults: view_data.view_defaults,
            view_metric_config: view_data.view_metric_config,
            view_group_by: view_data.view_group_by,
            view_filters: view_data.view_filters,
            views: view_data.views,
            base_catalog_sources,
            catalog_entries,
            read_models,
            api_types,
            domain_types,
            rust_types,
            extern_ts_imports,
            helper_trait_name,
            helper_impl_type_name,
            generated_adapter_type_name,
            helpers,
            imports,
            capabilities_json,
            view_metrics_json,
            view_default_metrics_json,
            view_metric_config_json,
        })
    }
}

struct ViewData {
    view_metrics: BTreeMap<String, Vec<String>>,
    view_defaults: BTreeMap<String, String>,
    view_metric_config: BTreeMap<String, BTreeMap<String, ViewMetricConfig>>,
    view_group_by: BTreeMap<String, Vec<GroupByModel>>,
    view_filters: BTreeMap<String, Vec<FilterModel>>,
    views: Vec<ViewModel>,
}

fn extract_view_configs(
    views: &[ViewDefinition],
    compat: &CompatDefinition,
    extern_ts_map: &BTreeMap<String, String>,
) -> Result<ViewData, String> {
    let compat_map = compat
        .view_aliases
        .iter()
        .map(|alias| (alias.view.as_str(), alias))
        .collect::<BTreeMap<_, _>>();
    let mut view_metrics = BTreeMap::new();
    let mut view_defaults = BTreeMap::new();
    let mut view_metric_config = BTreeMap::new();
    let mut view_group_by = BTreeMap::new();
    let mut view_filters = BTreeMap::new();
    let mut view_models = Vec::new();

    for view in views {
        let Some(config_value) = view.params.get("config") else {
            continue;
        };
        let config: ViewConfig = serde_json::from_value(config_value.clone())
            .map_err(|e| format!("invalid view config for '{}': {e}", view.name))?;
        let compat_view = compat_map.get(view.name.as_str()).copied();

        let metrics = config
            .metrics
            .values()
            .map(|m| m.metric.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        view_metrics.insert(view.name.clone(), metrics);

        if let Some(default_metric) = config.default_metric.clone() {
            view_defaults.insert(view.name.clone(), default_metric);
        }

        let mut metric_configs = BTreeMap::new();
        for (key, value) in config.metrics {
            metric_configs.insert(
                key,
                ViewMetricConfig {
                    metric: value.metric,
                    label: value.label.unwrap_or_default(),
                    unit: value.unit,
                    modes: value.modes,
                    requires: value.requires,
                },
            );
        }
        view_metric_config.insert(view.name.clone(), metric_configs);

        let mut group_by_items = config
            .group_by
            .into_iter()
            .map(|(key, value)| GroupByModel {
                key,
                field: value.field,
            })
            .collect::<Vec<_>>();
        group_by_items.sort_by(|lhs, rhs| lhs.key.cmp(&rhs.key));
        view_group_by.insert(view.name.clone(), group_by_items.clone());

        let mut filters = config
            .filters
            .into_iter()
            .map(|(key, value)| {
                build_filter_model(
                    key,
                    value.field,
                    parse_filter_op(&value.op),
                    value.type_ref,
                    value.optional,
                    extern_ts_map,
                )
            })
            .collect::<Vec<_>>();
        filters.sort_by(|lhs, rhs| lhs.key.cmp(&rhs.key));
        view_filters.insert(view.name.clone(), filters.clone());

        let mut response_fields = config
            .response_fields
            .into_iter()
            .filter_map(|(name, cfg)| {
                cfg.from_filter
                    .or(cfg.from_param)
                    .map(|source| ResponseFieldModel { name, source })
            })
            .collect::<Vec<_>>();
        response_fields.sort_by(|lhs, rhs| lhs.name.cmp(&rhs.name));

        let mut totals_fields = config
            .totals
            .into_iter()
            .map(|(name, cfg)| ViewTotalFieldModel {
                name,
                rendered_ts_type: match cfg.coerce.as_deref() {
                    Some("integer") | Some("float") | None => "number".to_string(),
                    Some(_) => "number".to_string(),
                },
            })
            .collect::<Vec<_>>();
        totals_fields.sort_by(|lhs, rhs| lhs.name.cmp(&rhs.name));

        let query_type = config
            .query_type
            .unwrap_or_else(|| format!("{}Query", to_pascal_case(&view.name)));
        let response_type = config
            .response_type
            .unwrap_or_else(|| format!("{}Response", to_pascal_case(&view.name)));
        let result_kind = config.result_kind.unwrap_or_else(|| "result".to_string());
        let is_metric_series = result_kind == "metric_series";
        let is_distribution = result_kind == "distribution";

        view_models.push(ViewModel {
            name: view.name.clone(),
            name_pascal: to_pascal_case(&view.name),
            metric_type_name: compat_view
                .and_then(|alias| alias.metric_alias_type.clone())
                .unwrap_or_else(|| metric_type_name(&view.name)),
            group_by_type_name: compat_view
                .and_then(|alias| alias.group_by_alias_type.clone())
                .unwrap_or_else(|| group_by_type_name(&view.name)),
            query_type,
            pack_query_type: compat_view
                .and_then(|alias| alias.pack_query_type.clone())
                .unwrap_or_else(|| format!("{}PackQuery", to_pascal_case(&view.name))),
            response_type,
            result_kind,
            is_metric_series,
            is_distribution,
            group_by_keys: group_by_items.into_iter().map(|item| item.key).collect(),
            filters,
            response_fields,
            point_type: compat_view.and_then(|alias| alias.point_type.clone()),
            totals_type: compat_view.and_then(|alias| alias.totals_type.clone()),
            totals_fields,
            query_filter_field: compat_view.and_then(|alias| alias.query_filter_field.clone()),
            query_filter_type: compat_view.and_then(|alias| alias.query_filter_type.clone()),
        });
    }

    Ok(ViewData {
        view_metrics,
        view_defaults,
        view_metric_config,
        view_group_by,
        view_filters,
        views: view_models,
    })
}

fn build_capabilities(
    view_data: &ViewData,
    catalog_entries: &[CatalogEntryModel],
    read_models: &[ReadModelModel],
) -> serde_json::Value {
    let view_map = view_data
        .views
        .iter()
        .map(|view| {
            (
                view.name.clone(),
                serde_json::json!({
                    "metrics": view_data.view_metrics.get(&view.name).cloned().unwrap_or_default(),
                    "default_metric": view_data.view_defaults.get(&view.name).cloned(),
                    "metric_config": view_data.view_metric_config.get(&view.name).cloned().unwrap_or_default(),
                    "query_type": view.query_type,
                    "response_type": view.response_type,
                    "result_kind": view.result_kind,
                    "group_by": view.group_by_keys,
                    "filters": view.filters.iter().map(|filter| serde_json::json!({
                        "key": filter.key,
                        "field": filter.field,
                        "op": filter.op,
                        "type": filter.ts_type,
                        "optional": filter.optional,
                    })).collect::<Vec<_>>(),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();

    let catalog_map = catalog_entries
        .iter()
        .map(|entry| {
            (
                entry.name.clone(),
                serde_json::json!({
                    "base_source": entry.base_source,
                    "fields": entry.fields.iter().map(|field| serde_json::json!({
                        "name": field.name,
                        "type": field.ts_type,
                        "optional": field.optional,
                    })).collect::<Vec<_>>(),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();

    let read_model_map = read_models
        .iter()
        .map(|model| {
            (
                model.name.clone(),
                serde_json::json!({
                    "query_type": model.query_type,
                    "response_type": model.response_type,
                    "params": model.params.iter().map(|field| serde_json::json!({
                        "name": field.name,
                        "type": field.ts_type,
                        "optional": field.optional,
                    })).collect::<Vec<_>>(),
                }),
            )
        })
        .collect::<serde_json::Map<_, _>>();

    serde_json::json!({
        "views": view_map,
        "catalog": catalog_map,
        "read_models": read_model_map,
    })
}

fn collect_base_catalog_sources(
    tracker_name: &str,
    tracker_const: &str,
    catalog_entries: &[tracen_ir::CatalogEntryDefinition],
    base_sources: &BTreeMap<String, String>,
    validate_missing: bool,
) -> Result<Vec<BaseCatalogSourceModel>, String> {
    let unique_sources = catalog_entries
        .iter()
        .filter_map(|entry| entry.base_source.as_ref().cloned())
        .collect::<BTreeSet<_>>();

    if unique_sources.len() > 1 {
        let sources = unique_sources
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(format!(
            "tracker '{tracker_name}' declares multiple base catalog sources ({sources}), but pack_base_catalog currently supports exactly one payload"
        ));
    }

    unique_sources
        .into_iter()
        .map(|source| {
            let payload = base_sources.get(&source).cloned();
            let has_json = payload.is_some();
            if validate_missing && payload.is_none() {
                return Err(format!(
                    "tracker '{tracker_name}' declares base catalog source '{source}', but no payload was provided for it"
                ));
            }
            Ok(BaseCatalogSourceModel {
                json_const: format!(
                    "{}_{}_BASE_CATALOG_JSON",
                    tracker_const,
                    dsl_ident_to_screaming_snake_case(&source)
                ),
                name: source,
                json: payload.unwrap_or_default(),
                has_json,
            })
        })
        .collect()
}

fn collect_required_rust_type_names(def: &TrackerDefinition) -> BTreeSet<String> {
    let type_map = def
        .types()
        .iter()
        .map(|type_def| (type_def.name.as_str(), type_def))
        .collect::<BTreeMap<_, _>>();
    let mut required = BTreeSet::new();

    for entry in def.catalog() {
        for field in &entry.fields {
            collect_type_dependencies(&field.type_ref, &type_map, &mut required);
        }
    }

    for helper in def.helpers() {
        for param in &helper.params {
            collect_type_dependencies(&param.type_ref, &type_map, &mut required);
        }
        collect_type_dependencies(&helper.return_type, &type_map, &mut required);
    }

    for import in def.imports() {
        for param in &import.params {
            collect_type_dependencies(&param.type_ref, &type_map, &mut required);
        }
        collect_type_dependencies(&import.return_type, &type_map, &mut required);
    }

    for read_model in def.read_models() {
        if let Some(response_type) = &read_model.response_type {
            collect_type_dependencies(response_type, &type_map, &mut required);
        }
        for field in &read_model.params {
            collect_type_dependencies(&field.type_ref, &type_map, &mut required);
        }
        for field in &read_model.fields {
            collect_type_dependencies(&field.type_ref, &type_map, &mut required);
        }
    }

    required
}

fn collect_type_dependencies(
    type_ref: &str,
    type_map: &BTreeMap<&str, &PackTypeDefinition>,
    required: &mut BTreeSet<String>,
) {
    let trimmed = type_ref.trim();
    if let Some(inner) = trimmed.strip_suffix("[]") {
        collect_type_dependencies(inner, type_map, required);
        return;
    }

    let Some(type_def) = type_map.get(trimmed).copied() else {
        return;
    };
    if !required.insert(type_def.name.clone()) {
        return;
    }

    match type_def.kind {
        PackTypeKind::Object => {
            for field in &type_def.fields {
                collect_type_dependencies(&field.type_ref, type_map, required);
            }
        }
        PackTypeKind::Alias => {
            if let Some(target) = &type_def.target {
                collect_type_dependencies(target, type_map, required);
            }
        }
        PackTypeKind::Enum => {}
    }
}

fn build_extern_ts_rust_map(imports: &[ExternTsImportDefinition]) -> BTreeMap<String, String> {
    imports
        .iter()
        .flat_map(|import| {
            import
                .items
                .iter()
                .map(|item| (item.name.clone(), item.rust_type.clone()))
        })
        .collect()
}

fn build_type_field_model(
    field: &SchemaFieldDefinition,
    extern_ts_map: &BTreeMap<String, String>,
) -> TypeFieldModel {
    TypeFieldModel {
        name: field.name.clone(),
        ts_type: field.type_ref.clone(),
        rendered_ts_type: render_ts_contract_type(&field.type_ref),
        rust_type: resolve_rust_type(&field.type_ref, extern_ts_map),
        optional: field.optional,
    }
}

fn build_type_model(
    type_def: &PackTypeDefinition,
    extern_ts_map: &BTreeMap<String, String>,
) -> TypeModel {
    let variants = type_def
        .variants
        .iter()
        .map(|value| TypeVariantModel {
            value: value.clone(),
            rust_ident: enum_variant_to_rust_ident(value),
        })
        .collect::<Vec<_>>();
    let rust_target = type_def
        .target
        .as_ref()
        .map(|target| resolve_rust_type(target, extern_ts_map));
    TypeModel {
        name: type_def.name.clone(),
        kind: match type_def.kind {
            PackTypeKind::Object => "object",
            PackTypeKind::Enum => "enum",
            PackTypeKind::Alias => "alias",
        }
        .to_string(),
        is_object: matches!(type_def.kind, PackTypeKind::Object),
        is_enum: matches!(type_def.kind, PackTypeKind::Enum),
        is_alias: matches!(type_def.kind, PackTypeKind::Alias),
        emit_ts: type_def.emit_ts,
        emit_rust: type_def.emit_rust,
        contract: type_def.contract.clone(),
        fields: type_def
            .fields
            .iter()
            .map(|field| build_type_field_model(field, extern_ts_map))
            .collect(),
        variants,
        target: type_def
            .target
            .as_ref()
            .map(|target| render_ts_contract_type(target)),
        rust_target,
    }
}

fn build_helper_model(
    helper: &tracen_ir::HelperDefinition,
    extern_ts_map: &BTreeMap<String, String>,
) -> HelperModel {
    let params = helper
        .params
        .iter()
        .map(|param| HelperParamModel {
            name: param.name.clone(),
            ts_type: param.type_ref.clone(),
            rendered_ts_type: render_ts_contract_type(&param.type_ref),
            rust_type: resolve_rust_type(&param.type_ref, extern_ts_map),
            optional: param.optional,
        })
        .collect::<Vec<_>>();
    HelperModel {
        name: helper.name.clone(),
        rust_method_name: dsl_ident_to_snake_case(&helper.name),
        ts_method_name: helper.compat_ts_name.clone(),
        native_export_name: helper.compat_native_name.clone(),
        fallible: helper.fallible,
        param_count: params.len(),
        params,
        return_type: helper.return_type.clone(),
        rendered_return_ts_type: render_ts_contract_type(&helper.return_type),
        rust_return_type: resolve_rust_type(&helper.return_type, extern_ts_map),
    }
}

fn build_import_model(
    import: &ImportDefinition,
    extern_ts_map: &BTreeMap<String, String>,
) -> HelperModel {
    let params = import
        .params
        .iter()
        .map(|param| HelperParamModel {
            name: param.name.clone(),
            ts_type: param.type_ref.clone(),
            rendered_ts_type: render_ts_contract_type(&param.type_ref),
            rust_type: resolve_rust_type(&param.type_ref, extern_ts_map),
            optional: param.optional,
        })
        .collect::<Vec<_>>();
    HelperModel {
        name: import.name.clone(),
        rust_method_name: dsl_ident_to_snake_case(&import.name),
        ts_method_name: import.compat_ts_name.clone(),
        native_export_name: import.compat_native_name.clone(),
        fallible: import.fallible,
        param_count: params.len(),
        params,
        return_type: import.return_type.clone(),
        rendered_return_ts_type: render_ts_contract_type(&import.return_type),
        rust_return_type: resolve_rust_type(&import.return_type, extern_ts_map),
    }
}

fn build_filter_model(
    key: String,
    field: String,
    op: FilterOperator,
    type_ref: String,
    optional: bool,
    extern_ts_map: &BTreeMap<String, String>,
) -> FilterModel {
    FilterModel {
        key,
        field,
        op: filter_op_to_str(op),
        rendered_ts_type: render_ts_contract_type(&type_ref),
        rust_type: resolve_rust_type(&type_ref, extern_ts_map),
        ts_type: type_ref,
        optional,
    }
}

fn resolve_rust_type(type_ref: &str, extern_ts_map: &BTreeMap<String, String>) -> String {
    if let Some(inner) = type_ref.trim().strip_suffix("[]") {
        return format!("Vec<{}>", resolve_rust_type(inner, extern_ts_map));
    }
    if let Some(mapped) = extern_ts_map.get(type_ref.trim()) {
        return mapped.clone();
    }
    render_rust_type(type_ref)
}

fn strip_ts_extension(file_name: &str) -> String {
    file_name.trim_end_matches(".ts").to_string()
}

fn default_filter_op() -> String {
    "eq".to_string()
}

fn parse_filter_op(raw: &str) -> FilterOperator {
    match raw {
        "neq" => FilterOperator::Neq,
        "gt" => FilterOperator::Gt,
        "gte" => FilterOperator::Gte,
        "lt" => FilterOperator::Lt,
        "lte" => FilterOperator::Lte,
        _ => FilterOperator::Eq,
    }
}

fn filter_op_to_str(op: FilterOperator) -> String {
    match op {
        FilterOperator::Eq => "eq".to_string(),
        FilterOperator::Neq => "neq".to_string(),
        FilterOperator::Gt => "gt".to_string(),
        FilterOperator::Gte => "gte".to_string(),
        FilterOperator::Lt => "lt".to_string(),
        FilterOperator::Lte => "lte".to_string(),
    }
}

fn to_pascal_case(s: &str) -> String {
    dsl_ident_to_ts_type_name(s)
}

fn to_snake_case(s: &str) -> String {
    dsl_ident_to_snake_case(s)
}

fn to_screaming_snake_case(s: &str) -> String {
    dsl_ident_to_screaming_snake_case(s)
}

fn metric_type_name(view_name: &str) -> String {
    format!("{}MetricKey", to_pascal_case(view_name))
}

fn group_by_type_name(view_name: &str) -> String {
    format!("{}GroupByKey", to_pascal_case(view_name))
}

#[cfg(test)]
mod tests {
    use super::PackGenModel;
    use std::collections::BTreeMap;

    fn compile_tracker(dsl: &str) -> tracen_ir::TrackerDefinition {
        tracen_dsl::compile(dsl).expect("compile tracker")
    }

    #[test]
    fn base_catalog_sources_are_empty_when_tracker_declares_none() {
        let definition = compile_tracker(
            r#"
tracker "catalog_pack" v1 {
  fields {
    slug: text optional
  }
}
"#,
        );

        let model = PackGenModel::from_tracker(&definition).expect("build model");
        assert!(model.base_catalog_sources.is_empty());
    }

    #[test]
    fn requires_declared_base_catalog_source_payloads_when_build_inputs_are_supplied() {
        let definition = compile_tracker(
            r#"
tracker "catalog_pack" v1 {
  fields {
    slug: text optional
  }
  catalog {
    entry "primary_entry" {
      base_source = "primary_source"
      fields = {"slug":{"type":"string"}}
    }
  }
}
"#,
        );

        let error = PackGenModel::from_tracker_with_base_sources(&definition, &BTreeMap::new())
            .expect_err("missing base source should fail");
        assert!(error.contains("primary_source"));
        assert!(error.contains("no payload was provided"));
    }

    #[test]
    fn includes_single_declared_base_catalog_source_once() {
        let definition = compile_tracker(
            r#"
tracker "catalog_pack" v1 {
  fields {
    slug: text optional
  }
  catalog {
    entry "primary_entry" {
      base_source = "primary_source"
      fields = {"slug":{"type":"string"}}
    }
  }
}
"#,
        );
        let mut base_sources = BTreeMap::new();
        base_sources.insert(
            "primary_source".to_string(),
            r#"[{"slug":"bench_press"}]"#.to_string(),
        );

        let model = PackGenModel::from_tracker_with_base_sources(&definition, &base_sources)
            .expect("single base source should succeed");
        assert_eq!(model.base_catalog_sources.len(), 1);
        assert_eq!(model.base_catalog_sources[0].name, "primary_source");
        assert!(model.base_catalog_sources[0].has_json);
        assert!(model.base_catalog_sources[0].json.contains("bench_press"));
    }

    #[test]
    fn deduplicates_duplicate_catalog_entries_that_share_one_base_source() {
        let definition = compile_tracker(
            r#"
tracker "catalog_pack" v1 {
  fields {
    slug: text optional
  }
  catalog {
    entry "primary_entry" {
      base_source = "shared_source"
      fields = {"slug":{"type":"string"}}
    }
    entry "secondary_entry" {
      base_source = "shared_source"
      fields = {"slug":{"type":"string"}}
    }
  }
}
"#,
        );
        let mut base_sources = BTreeMap::new();
        base_sources.insert(
            "shared_source".to_string(),
            r#"[{"slug":"bench_press"}]"#.to_string(),
        );

        let model = PackGenModel::from_tracker_with_base_sources(&definition, &base_sources)
            .expect("shared source should succeed");
        assert_eq!(model.base_catalog_sources.len(), 1);
        assert_eq!(model.base_catalog_sources[0].name, "shared_source");
    }

    #[test]
    fn rejects_multiple_base_catalog_sources() {
        let definition = compile_tracker(
            r#"
tracker "catalog_pack" v1 {
  fields {
    slug: text optional
  }
  catalog {
    entry "primary_entry" {
      base_source = "primary_source"
      fields = {"slug":{"type":"string"}}
    }
    entry "secondary_entry" {
      base_source = "secondary_source"
      fields = {"slug":{"type":"string"}}
    }
  }
}
"#,
        );

        let error = PackGenModel::from_tracker(&definition).expect_err("multiple base sources");
        assert!(error.contains("pack_base_catalog"));
        assert!(error.contains("primary_source"));
        assert!(error.contains("secondary_source"));
    }

    #[test]
    fn includes_read_model_types_in_rust_output_even_when_emit_rust_is_false() {
        let definition = compile_tracker(
            r#"
tracker "helper_pack" v1 {
  fields {
    inner: int optional
  }
  types {
    type "InnerStat" {
      contract = "api"
      emit_rust = false
      fields = {"value":{"type":"int"}}
    }

    type "RollupPayload" {
      contract = "api"
      emit_rust = false
      fields = {"inner":{"type":"InnerStat"}}
    }
  }

  read_models {
    read_model "rollup" {
      response_type = "RollupPayload"
      fields = {"inner":{"type":"InnerStat"}}
    }
  }
}
"#,
        );

        let model = PackGenModel::from_tracker(&definition).expect("build model");
        assert!(model
            .rust_types
            .iter()
            .any(|type_def| type_def.name == "RollupPayload"));
        assert!(model
            .rust_types
            .iter()
            .any(|type_def| type_def.name == "InnerStat"));

        let read_model = model
            .read_models
            .iter()
            .find(|read_model| read_model.name == "rollup")
            .expect("rollup read model");
        assert_eq!(read_model.response_rust_type, "RollupPayload");
        assert!(!read_model.emit_rust_response_struct);
    }

    #[test]
    fn includes_helper_and_import_types_in_rust_output_even_when_emit_rust_is_false() {
        let definition = compile_tracker(
            r#"
tracker "helper_pack" v1 {
  fields {
    inner: int optional
  }
  types {
    type "InnerValue" {
      contract = "domain"
      emit_rust = false
      fields = {"count":{"type":"int"}}
    }

    type "HelperInput" {
      contract = "domain"
      emit_rust = false
      fields = {"inner":{"type":"InnerValue"}}
    }

    type "HelperOutput" {
      contract = "domain"
      emit_rust = false
      fields = {"inner":{"type":"InnerValue"}}
    }
  }

  helpers {
    helper "enrich_payload" {
      fallible = true
      params = {"payload":{"type":"HelperInput"}}
      return_type = "HelperOutput"
    }
  }

  imports {
    import "load_payload" {
      fallible = true
      params = {"seed":{"type":"HelperInput"}}
      return_type = "HelperOutput"
    }
  }
}
"#,
        );

        let model = PackGenModel::from_tracker(&definition).expect("build model");
        assert!(model
            .rust_types
            .iter()
            .any(|type_def| type_def.name == "InnerValue"));
        assert!(model
            .rust_types
            .iter()
            .any(|type_def| type_def.name == "HelperInput"));
        assert!(model
            .rust_types
            .iter()
            .any(|type_def| type_def.name == "HelperOutput"));

        let generator = crate::with_builtin_templates().expect("generator");
        let runtime = generator
            .generate_all_from_model(&model)
            .expect("generate artifacts")
            .rust_pack_runtime;
        assert!(runtime.contains("fn enrich_payload"));
        assert!(runtime.contains("payload: HelperInput"));
        assert!(runtime.contains("Result<HelperOutput, String>"));
        assert!(runtime.contains("fn load_payload"));
    }
}
