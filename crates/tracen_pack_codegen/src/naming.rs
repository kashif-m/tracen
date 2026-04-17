//! Shared naming and schema rendering helpers for pack code generation.

fn split_ident(input: &str) -> Vec<&str> {
    input
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|segment| !segment.is_empty())
        .collect()
}

pub fn dsl_ident_to_ts_type_name(input: &str) -> String {
    split_ident(input)
        .into_iter()
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

pub fn dsl_ident_to_snake_case(input: &str) -> String {
    split_ident(input)
        .into_iter()
        .map(|segment| segment.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join("_")
}

pub fn dsl_ident_to_screaming_snake_case(input: &str) -> String {
    dsl_ident_to_snake_case(input).to_ascii_uppercase()
}

pub fn wrap_pack_type_ref(input: &str) -> String {
    format!("PackTypeRef<'{}'>", input)
}

pub fn render_ts_schema_type(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }

    if let Some(inner) = trimmed.strip_suffix("[]") {
        return format!("{}[]", render_ts_schema_type(inner));
    }

    if is_passthrough_ts_type(trimmed) {
        return trimmed.to_string();
    }

    if is_identifier_like(trimmed) {
        return wrap_pack_type_ref(trimmed);
    }

    trimmed.to_string()
}

pub fn render_ts_contract_type(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return "unknown".to_string();
    }

    if let Some(inner) = trimmed.strip_suffix("[]") {
        return format!("{}[]", render_ts_contract_type(inner));
    }

    match trimmed {
        "string" | "text" => "string".to_string(),
        "number" | "float" | "int" => "number".to_string(),
        "bool" | "boolean" => "boolean".to_string(),
        "json" => "DomainJsonValue".to_string(),
        "unknown" | "any" | "null" | "undefined" | "never" | "void" => trimmed.to_string(),
        other if is_passthrough_ts_type(other) => other.to_string(),
        other => other.to_string(),
    }
}

pub fn render_rust_type(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return "serde_json::Value".to_string();
    }

    if let Some(inner) = trimmed.strip_suffix("[]") {
        return format!("Vec<{}>", render_rust_type(inner));
    }

    match trimmed {
        "string" | "text" => "String".to_string(),
        "number" | "float" => "f64".to_string(),
        "int" => "i64".to_string(),
        "bool" | "boolean" => "bool".to_string(),
        "json" | "unknown" | "any" => "serde_json::Value".to_string(),
        other
            if other.starts_with('{')
                || other.starts_with("Record<")
                || other.starts_with("Array<")
                || other.starts_with("ReadonlyArray<")
                || other.contains(" | ")
                || other.contains(" & ")
                || other.contains('<') =>
        {
            "serde_json::Value".to_string()
        }
        other => other.to_string(),
    }
}

pub fn enum_variant_to_rust_ident(input: &str) -> String {
    dsl_ident_to_ts_type_name(input)
}

fn is_identifier_like(input: &str) -> bool {
    let mut chars = input.chars();
    match chars.next() {
        Some(first) if first == '_' || first.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn is_passthrough_ts_type(input: &str) -> bool {
    matches!(
        input,
        "string"
            | "number"
            | "boolean"
            | "unknown"
            | "any"
            | "null"
            | "undefined"
            | "never"
            | "void"
    ) || input.starts_with('{')
        || input.starts_with("Record<")
        || input.starts_with("Array<")
        || input.starts_with("ReadonlyArray<")
        || input.starts_with('(')
        || input.contains(" | ")
        || input.contains(" & ")
        || input.contains('<')
        || input.contains('\'')
        || input.contains('"')
}

#[cfg(test)]
mod tests {
    use super::{dsl_ident_to_ts_type_name, render_ts_contract_type, render_ts_schema_type};

    #[test]
    fn converts_dsl_identifiers_to_ts_type_names() {
        assert_eq!(dsl_ident_to_ts_type_name("metric_series"), "MetricSeries");
        assert_eq!(dsl_ident_to_ts_type_name("daily-card"), "DailyCard");
        assert_eq!(dsl_ident_to_ts_type_name("pr"), "Pr");
        assert_eq!(dsl_ident_to_ts_type_name("monthly_rollup"), "MonthlyRollup");
    }

    #[test]
    fn renders_named_schema_types_as_pack_type_refs() {
        assert_eq!(render_ts_schema_type("string"), "string");
        assert_eq!(render_ts_schema_type("number[]"), "number[]");
        assert_eq!(
            render_ts_schema_type("SummaryCard"),
            "PackTypeRef<'SummaryCard'>"
        );
        assert_eq!(
            render_ts_schema_type("BucketPoint[]"),
            "PackTypeRef<'BucketPoint'>[]"
        );
        assert_eq!(
            render_ts_schema_type("{ min: number; max: number }"),
            "{ min: number; max: number }"
        );
    }

    #[test]
    fn renders_contract_json_types_to_domain_json_value() {
        assert_eq!(render_ts_contract_type("json"), "DomainJsonValue");
        assert_eq!(render_ts_contract_type("json[]"), "DomainJsonValue[]");
        assert_eq!(render_ts_contract_type("int"), "number");
    }
}
