use serde_json::Value;
use std::fmt;

use crate::{FieldDefinition, FieldType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadValidationPolicy {
    Event,
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
    let Some(map) = payload.as_object_mut() else {
        return Err(PayloadValidationError::PayloadMustBeObject);
    };

    if policy == PayloadValidationPolicy::PartialDraft {
        for key in map.keys() {
            if !fields.iter().any(|field| field.name == *key) {
                return Err(PayloadValidationError::UnknownField(key.clone()));
            }
        }
    }

    for field in fields {
        match map.get(&field.name) {
            Some(value) => validate_field_type(field, value)?,
            None if policy == PayloadValidationPolicy::Event => {
                if let Some(default_value) = &field.default_value {
                    map.insert(field.name.clone(), default_value.clone());
                } else if !field.optional {
                    return Err(PayloadValidationError::RequiredFieldMissing(
                        field.name.clone(),
                    ));
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
        return if field.optional {
            Ok(())
        } else {
            Err(PayloadValidationError::NonOptionalNull(field.name.clone()))
        };
    }

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
