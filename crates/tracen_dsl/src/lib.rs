//! tracen_dsl - parser and semantic validator for tracker DSL.

use serde::Deserialize;
use tracen_ir::error::{ErrorCode, TrackerError, TrackerResult};
use tracen_ir::{TrackerDefinition, TrackerDefinitionInput};

pub mod ast;
pub mod parser;

pub use ast::*;

/// Parse DSL string and compile to validated TrackerDefinition.
pub fn compile(input: &str) -> TrackerResult<TrackerDefinition> {
    let ast = parser::parse_tracker(input)?;
    validate_semantics(&ast)?;

    Ok(TrackerDefinition::new(TrackerDefinitionInput {
        tracker_id_override: ast
            .compat
            .as_ref()
            .and_then(|compat| compat.tracker_id_override.clone()),
        tracker_name: ast.name,
        version: ast.version,
        dsl: input.to_string(),
        fields: ast.fields,
        derives: ast.derives,
        metrics: ast.metrics,
        alerts: ast.alerts,
        planning: ast.planning,
        views: ast.views,
        catalog: ast.catalog,
        read_models: ast.read_models,
        types: ast.types,
        helpers: ast.helpers,
        imports: ast.imports,
        extern_ts: ast.extern_ts,
        compat: ast.compat,
    }))
}

/// Parse DSL into AST (without full semantic validation).
pub fn parse(input: &str) -> TrackerResult<TrackerAst> {
    parser::parse_tracker(input)
}

fn validate_semantics(ast: &TrackerAst) -> TrackerResult<()> {
    let mut field_names = std::collections::BTreeSet::new();
    for field in &ast.fields {
        if !field_names.insert(field.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate field definition: {}", field.name),
            ))?;
        }
    }

    let mut derive_names = std::collections::BTreeSet::new();
    for derive in &ast.derives {
        if !derive_names.insert(derive.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate derive definition: {}", derive.name),
            ))?;
        }
        if references_ident(&derive.expr, &derive.name) {
            Err(TrackerError::new_simple(
                ErrorCode::CircularDependency,
                format!("derive '{}' references itself", derive.name),
            ))?;
        }
    }

    let mut metric_names = std::collections::BTreeSet::new();
    for metric in &ast.metrics {
        if !metric_names.insert(metric.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate metric definition: {}", metric.name),
            ))?;
        }
    }

    let mut view_names = std::collections::BTreeSet::new();
    for view in &ast.views {
        if !view_names.insert(view.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate view definition: {}", view.name),
            ))?;
        }
    }

    let mut catalog_names = std::collections::BTreeSet::new();
    for entry in &ast.catalog {
        if !catalog_names.insert(entry.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate catalog entry definition: {}", entry.name),
            ))?;
        }

        let mut field_names = std::collections::BTreeSet::new();
        for field in &entry.fields {
            if !field_names.insert(field.name.clone()) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!(
                        "catalog entry '{}' defines duplicate field '{}'",
                        entry.name, field.name
                    ),
                ))?;
            }
        }
    }

    let mut read_model_names = std::collections::BTreeSet::new();
    for model in &ast.read_models {
        if !read_model_names.insert(model.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate read model definition: {}", model.name),
            ))?;
        }

        let mut param_names = std::collections::BTreeSet::new();
        for field in &model.params {
            if !param_names.insert(field.name.clone()) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!(
                        "read model '{}' defines duplicate param '{}'",
                        model.name, field.name
                    ),
                ))?;
            }
        }

        let mut field_names = std::collections::BTreeSet::new();
        for field in &model.fields {
            if !field_names.insert(field.name.clone()) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!(
                        "read model '{}' defines duplicate field '{}'",
                        model.name, field.name
                    ),
                ))?;
            }
        }
    }

    let mut type_names = std::collections::BTreeSet::new();
    for type_def in &ast.types {
        if !type_names.insert(type_def.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate type definition: {}", type_def.name),
            ))?;
        }
    }

    let mut helper_names = std::collections::BTreeSet::new();
    for helper in &ast.helpers {
        if !helper_names.insert(helper.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate helper definition: {}", helper.name),
            ))?;
        }
    }

    let mut import_names = std::collections::BTreeSet::new();
    for import in &ast.imports {
        if !import_names.insert(import.name.clone()) {
            Err(TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("duplicate import definition: {}", import.name),
            ))?;
        }
    }

    #[derive(Debug, Deserialize)]
    struct ViewConfig {
        #[serde(default)]
        group_by: std::collections::BTreeMap<String, ViewGroupByConfig>,
        #[serde(default)]
        filters: std::collections::BTreeMap<String, ViewFilterConfig>,
        #[serde(default)]
        metrics: std::collections::BTreeMap<String, ViewMetricConfig>,
    }

    #[derive(Debug, Deserialize)]
    struct ViewMetricConfig {
        metric: String,
    }

    #[derive(Debug, Deserialize)]
    struct ViewGroupByConfig {
        field: String,
    }

    #[derive(Debug, Deserialize)]
    struct ViewFilterConfig {
        field: String,
    }

    let metric_names = ast
        .metrics
        .iter()
        .map(|metric| metric.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let extern_ts_names = ast
        .extern_ts
        .iter()
        .flat_map(|import| import.items.iter().map(|item| item.name.as_str()))
        .collect::<std::collections::BTreeSet<_>>();
    let known_fields = ast
        .fields
        .iter()
        .map(|field| field.name.as_str())
        .chain(
            ast.derives
                .iter()
                .map(|derive_def| derive_def.name.as_str()),
        )
        .collect::<std::collections::BTreeSet<_>>();
    let mut known_schema_types = ast
        .types
        .iter()
        .map(|type_def| type_def.name.clone())
        .collect::<std::collections::BTreeSet<_>>();
    for model in &ast.read_models {
        known_schema_types.insert(
            model
                .response_type
                .clone()
                .unwrap_or_else(|| format!("{}Response", to_pascal_case(&model.name))),
        );
        known_schema_types.insert(
            model
                .query_type
                .clone()
                .unwrap_or_else(|| format!("{}Query", to_pascal_case(&model.name))),
        );
    }
    for entry in &ast.catalog {
        if let Some(name) = &entry.compat_base_type {
            known_schema_types.insert(name.clone());
        }
        if let Some(name) = &entry.compat_overlay_type {
            known_schema_types.insert(name.clone());
        }
    }
    for name in extern_ts_names {
        known_schema_types.insert(name.to_string());
    }

    for view in &ast.views {
        let Some(config_value) = view.params.get("config") else {
            continue;
        };
        let config: ViewConfig = serde_json::from_value(config_value.clone()).map_err(|err| {
            TrackerError::new_simple(
                ErrorCode::DslInvalidExpression,
                format!("view '{}' has invalid config payload: {}", view.name, err),
            )
        })?;
        for (key, metric) in &config.metrics {
            if !metric_names.contains(metric.metric.as_str()) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!(
                        "view '{}' metric key '{}' references unknown metric '{}'",
                        view.name, key, metric.metric
                    ),
                ))?;
            }
        }

        for (key, group_by) in &config.group_by {
            if !known_fields.contains(group_by.field.as_str()) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!(
                        "view '{}' group_by '{}' references unknown field '{}'",
                        view.name, key, group_by.field
                    ),
                ))?;
            }
        }

        for (key, filter) in &config.filters {
            if !known_fields.contains(filter.field.as_str()) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!(
                        "view '{}' filter '{}' references unknown field '{}'",
                        view.name, key, filter.field
                    ),
                ))?;
            }
        }
    }

    for model in &ast.read_models {
        for filter in &model.filters {
            if !known_fields.contains(filter.field.as_str()) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!(
                        "read model '{}' filter '{}' references unknown field '{}'",
                        model.name, filter.key, filter.field
                    ),
                ))?;
            }
        }
    }

    for type_def in &ast.types {
        match type_def.kind {
            tracen_ir::PackTypeKind::Object => {
                for field in &type_def.fields {
                    ensure_known_type_ref(&known_schema_types, &field.type_ref, &type_def.name)?;
                }
            }
            tracen_ir::PackTypeKind::Enum => {
                if type_def.variants.is_empty() {
                    Err(TrackerError::new_simple(
                        ErrorCode::DslInvalidExpression,
                        format!(
                            "enum type '{}' must define at least one variant",
                            type_def.name
                        ),
                    ))?;
                }
            }
            tracen_ir::PackTypeKind::Alias => {
                if type_def.target.is_none() {
                    Err(TrackerError::new_simple(
                        ErrorCode::DslInvalidExpression,
                        format!("alias type '{}' must define target", type_def.name),
                    ))?;
                }
            }
        }
    }

    for helper in &ast.helpers {
        for field in &helper.params {
            ensure_known_type_ref(&known_schema_types, &field.type_ref, &helper.name)?;
        }
        ensure_known_type_ref(&known_schema_types, &helper.return_type, &helper.name)?;
    }

    for import in &ast.imports {
        for field in &import.params {
            ensure_known_type_ref(&known_schema_types, &field.type_ref, &import.name)?;
        }
        ensure_known_type_ref(&known_schema_types, &import.return_type, &import.name)?;
    }

    for entry in &ast.catalog {
        for field in &entry.fields {
            ensure_known_type_ref(&known_schema_types, &field.type_ref, &entry.name)?;
        }
        if let Some(validate_helper) = &entry.validate_helper {
            if !helper_names.contains(validate_helper) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!(
                        "catalog entry '{}' validate_helper '{}' is not a declared helper",
                        entry.name, validate_helper
                    ),
                ))?;
            }
        }
    }

    for model in &ast.read_models {
        for field in &model.params {
            ensure_known_type_ref(&known_schema_types, &field.type_ref, &model.name)?;
        }
        for field in &model.fields {
            ensure_known_type_ref(&known_schema_types, &field.type_ref, &model.name)?;
        }
    }

    if let Some(compat) = &ast.compat {
        let mut view_alias_names = std::collections::BTreeSet::new();
        for alias in &compat.view_aliases {
            if !view_names.contains(&alias.view) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!("compat view alias references unknown view '{}'", alias.view),
                ))?;
            }
            if !view_alias_names.insert(alias.view.clone()) {
                Err(TrackerError::new_simple(
                    ErrorCode::DslInvalidExpression,
                    format!("duplicate compat view alias for '{}'", alias.view),
                ))?;
            }
        }
    }

    Ok(())
}

fn ensure_known_type_ref(
    known_schema_types: &std::collections::BTreeSet<String>,
    type_ref: &str,
    context: &str,
) -> TrackerResult<()> {
    let trimmed = type_ref.trim();
    if is_builtin_type_ref(trimmed) {
        return Ok(());
    }
    if let Some(inner) = trimmed.strip_suffix("[]") {
        return ensure_known_type_ref(known_schema_types, inner, context);
    }
    if trimmed.starts_with('{')
        || trimmed.starts_with("Record<")
        || trimmed.starts_with("Array<")
        || trimmed.starts_with("ReadonlyArray<")
        || trimmed.contains(" | ")
        || trimmed.contains(" & ")
        || trimmed.contains('<')
    {
        return Ok(());
    }
    if known_schema_types.contains(trimmed) {
        return Ok(());
    }
    Err(TrackerError::new_simple(
        ErrorCode::DslInvalidExpression,
        format!("'{}' references unknown type '{}'", context, type_ref),
    ))
}

fn to_pascal_case(input: &str) -> String {
    input
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .map(|segment| {
            let mut chars = segment.chars();
            match chars.next() {
                Some(first) => {
                    first.to_ascii_uppercase().to_string() + &chars.as_str().to_ascii_lowercase()
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join("")
}

fn is_builtin_type_ref(type_ref: &str) -> bool {
    matches!(
        type_ref,
        "string"
            | "text"
            | "number"
            | "float"
            | "int"
            | "boolean"
            | "bool"
            | "unknown"
            | "any"
            | "json"
            | "null"
    )
}

fn references_ident(expr: &tracen_ir::Expression, ident: &str) -> bool {
    use tracen_ir::Expression;
    match expr {
        Expression::Field(name) => name == ident,
        Expression::Binary { left, right, .. } => {
            references_ident(left, ident) || references_ident(right, ident)
        }
        Expression::Conditional {
            condition,
            then_expr,
            else_expr,
        } => {
            references_ident_in_condition(condition, ident)
                || references_ident(then_expr, ident)
                || references_ident(else_expr, ident)
        }
        Expression::Function { args, .. } => args.iter().any(|arg| references_ident(arg, ident)),
        Expression::Number(_)
        | Expression::Int(_)
        | Expression::Bool(_)
        | Expression::Text(_)
        | Expression::Null => false,
    }
}

fn references_ident_in_condition(condition: &tracen_ir::Condition, ident: &str) -> bool {
    use tracen_ir::Condition;
    match condition {
        Condition::Comparison { left, right, .. } => {
            references_ident(left, ident) || references_ident(right, ident)
        }
        Condition::And(parts) | Condition::Or(parts) => parts
            .iter()
            .any(|part| references_ident_in_condition(part, ident)),
        Condition::Not(inner) => references_ident_in_condition(inner, ident),
        Condition::True | Condition::False => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compile_sample_tracker() {
        let dsl = r#"
        tracker "sample" v1 {
          fields {
            group_key: text
            value_a: float optional
          }
          metrics {
            total_value = sum(value_a) over all_time
          }
          views {
            view "summary" {
              config = {"metrics":{"total_value":{"metric":"total_value"}}}
            }
          }
          catalog {
            entry "sample_entry" {
              fields = {"slug":{"type":"string"}}
            }
          }
          read_models {
            read_model "home" {
              fields = {"items":{"type":"unknown[]"}}
            }
          }
        }
        "#;
        let def = compile(dsl).expect("compile sample dsl");
        assert_eq!(def.tracker_name(), "sample");
        assert!(!def.fields().is_empty());
        assert!(!def.metrics().is_empty());
        assert!(!def.views().is_empty());
        assert_eq!(def.catalog().len(), 1);
        assert_eq!(def.read_models().len(), 1);
    }

    #[test]
    fn reject_duplicate_fields() {
        let dsl = r#"
        tracker "x" v1 {
          fields {
            value_a: int
            value_a: int
          }
        }
        "#;
        assert!(compile(dsl).is_err());
    }

    #[test]
    fn reject_unknown_view_metric_reference() {
        let dsl = r#"
        tracker "x" v1 {
          fields { value_a: int optional }
          metrics { total_value = sum(value_a) over all_time }
          views {
            view "summary" {
              config = {"metrics":{"foo":{"metric":"missing_metric"}}}
            }
          }
        }
        "#;
        assert!(compile(dsl).is_err());
    }

    #[test]
    fn reject_unknown_view_group_by_field_reference() {
        let dsl = r#"
        tracker "x" v1 {
          fields { value_a: int optional }
          metrics { total_value = sum(value_a) over all_time }
          views {
            view "summary" {
              config = {"metrics":{"foo":{"metric":"total_value"}},"group_by":{"week":{"field":"missing_bucket"}}}
            }
          }
        }
        "#;
        assert!(compile(dsl).is_err());
    }

    #[test]
    fn reject_unknown_read_model_filter_reference() {
        let dsl = r#"
        tracker "x" v1 {
          fields { value_a: int optional }
          read_models {
            read_model "home" {
              filters = {"week":{"field":"missing_bucket","type":"number"}}
              fields = {"items":{"type":"unknown[]"}}
            }
          }
        }
        "#;
        assert!(compile(dsl).is_err());
    }
}
