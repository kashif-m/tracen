use serde_json::Value;
use std::fmt;

use crate::{FieldDefinition, FieldType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadValidationPolicy {
    /// Strict event validation used for public event ingestion.
    ///
    /// Unknown keys are rejected and default values are applied for missing
    /// nullable fields.
    Event,
    /// Backward-compatible permissive event validation that does not reject unknown fields.
    ///
    /// This is intended for internal integrations that already normalize payloads before
    /// validation and need to accept extra fields temporarily.
    EventLax,
    /// Legacy-friendly pack-query validation that skips required-field rejection for
    /// historical payloads while still applying defaults and type checks.
    PackQueryLax,
    PartialDraft,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PayloadValidationError {
    PayloadMustBeObject,
    UnknownField(String),
    RequiredFieldMissing(String),
    NonOptionalNull(String),
    InvalidFieldValue(String),
}

impl fmt::Display for PayloadValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadValidationError::PayloadMustBeObject => f.write_str("payload must be object"),
            PayloadValidationError::UnknownField(field) => {
                write!(f, "event plan item payload contains unknown field: {field}")
            }
            PayloadValidationError::RequiredFieldMissing(field) => {
                write!(f, "required field missing: {field}")
            }
            PayloadValidationError::NonOptionalNull(field) => {
                write!(f, "field '{field}' cannot be null")
            }
            PayloadValidationError::InvalidFieldValue(field) => {
                write!(f, "field '{field}' has invalid type/value")
            }
        }
    }
}

pub fn validate_payload_fields(
    fields: &[FieldDefinition],
    payload: &mut Value,
    policy: PayloadValidationPolicy,
) -> Result<(), PayloadValidationError> {
    let map = match payload.as_object_mut() {
        Some(map) => map,
        None => Err(PayloadValidationError::PayloadMustBeObject)?,
    };

    if matches!(
        policy,
        PayloadValidationPolicy::Event | PayloadValidationPolicy::PartialDraft
    ) {
        map.keys().try_for_each(|key| {
            if fields.iter().any(|field| field.name == *key) {
                Ok(())
            } else {
                Err(PayloadValidationError::UnknownField(key.clone()))
            }
        })?;
    }

    for field in fields {
        match map.get(&field.name) {
            Some(value) => validate_field_type(field, value)?,
            None if matches!(
                policy,
                PayloadValidationPolicy::Event
                    | PayloadValidationPolicy::EventLax
                    | PayloadValidationPolicy::PackQueryLax
            ) =>
            {
                if let Some(default_value) = &field.default_value {
                    map.insert(field.name.clone(), default_value.clone());
                } else if !field.optional
                    && !matches!(policy, PayloadValidationPolicy::PackQueryLax)
                {
                    Err(PayloadValidationError::RequiredFieldMissing(
                        field.name.clone(),
                    ))?;
                }
            }
            None => {}
        }
    }

    Ok(())
}

fn validate_field_type(
    field: &FieldDefinition,
    value: &Value,
) -> Result<(), PayloadValidationError> {
    if value.is_null() {
        if field.optional {
            Ok(())
        } else {
            Err(PayloadValidationError::NonOptionalNull(field.name.clone()))
        }
    } else {
        let valid = match &field.field_type {
            FieldType::Text => value.is_string(),
            FieldType::Float => value.is_number(),
            FieldType::Int => value.as_i64().is_some(),
            FieldType::Bool => value.is_boolean(),
            FieldType::Duration => value.as_i64().is_some() || value.as_f64().is_some(),
            FieldType::Timestamp => value.as_i64().is_some(),
            FieldType::Enum(values) => value
                .as_str()
                .map(|v| values.iter().any(|allowed| allowed == v))
                .unwrap_or(false),
        };

        if valid {
            Ok(())
        } else {
            Err(PayloadValidationError::InvalidFieldValue(
                field.name.clone(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        validate_payload_fields, FieldDefinition, FieldType, PayloadValidationError,
        PayloadValidationPolicy,
    };
    use serde_json::json;

    fn required_text_field(name: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            field_type: FieldType::Text,
            optional: false,
            default_value: None,
            reference: None,
        }
    }

    fn optional_text_field_with_default(name: &str, default: &str) -> FieldDefinition {
        FieldDefinition {
            name: name.to_string(),
            field_type: FieldType::Text,
            optional: true,
            default_value: Some(json!(default)),
            reference: None,
        }
    }

    #[test]
    fn event_policy_rejects_missing_required_fields() {
        let fields = vec![required_text_field("modality")];
        let mut payload = json!({});
        let error = validate_payload_fields(&fields, &mut payload, PayloadValidationPolicy::Event)
            .expect_err("strict event should reject missing required fields");

        assert!(matches!(
            error,
            PayloadValidationError::RequiredFieldMissing(field) if field == "modality"
        ));
    }

    #[test]
    fn event_lax_remains_strict_for_missing_required_fields() {
        let fields = vec![required_text_field("modality")];
        let mut payload = json!({});
        let error =
            validate_payload_fields(&fields, &mut payload, PayloadValidationPolicy::EventLax)
                .expect_err("event lax should still reject missing required fields");

        assert!(matches!(
            error,
            PayloadValidationError::RequiredFieldMissing(field) if field == "modality"
        ));
    }

    #[test]
    fn pack_query_lax_accepts_missing_required_fields() {
        let fields = vec![required_text_field("modality")];
        let mut payload = json!({});
        validate_payload_fields(&fields, &mut payload, PayloadValidationPolicy::PackQueryLax)
            .expect("pack query lax should allow missing non-optional field");

        assert!(payload.get("modality").is_none());
    }

    #[test]
    fn pack_query_lax_applies_defaults() {
        let fields = vec![optional_text_field_with_default("modality", "missing")];
        let mut payload = json!({});
        validate_payload_fields(&fields, &mut payload, PayloadValidationPolicy::PackQueryLax)
            .expect("pack query lax should apply defaults");

        assert_eq!(payload.get("modality"), Some(&json!("missing")));
    }

    #[test]
    fn pack_query_lax_rejects_invalid_present_field_values() {
        let fields = vec![required_text_field("modality")];

        let mut invalid_payload = json!({"modality": 1});
        let invalid_type = validate_payload_fields(
            &fields,
            &mut invalid_payload,
            PayloadValidationPolicy::PackQueryLax,
        )
        .expect_err("invalid type should reject");

        assert!(matches!(
            invalid_type,
            PayloadValidationError::InvalidFieldValue(field) if field == "modality"
        ));

        let mut null_payload = json!({"modality": null});
        let null_value = validate_payload_fields(
            &fields,
            &mut null_payload,
            PayloadValidationPolicy::PackQueryLax,
        )
        .expect_err("null required field should reject");

        assert!(matches!(
            null_value,
            PayloadValidationError::NonOptionalNull(field) if field == "modality"
        ));
    }
}
