use serde_json::Value;
use tracen_ir::TrackerDefinition;

use crate::PackError;

pub(crate) fn validate_payload_references(
    definition: &TrackerDefinition,
    payload: &Value,
    catalog_json: &Value,
) -> Result<(), PackError> {
    let referenced_fields = definition
        .fields()
        .iter()
        .filter(|field| field.reference.is_some())
        .collect::<Vec<_>>();
    if referenced_fields.is_empty() {
        return Ok(());
    }

    let catalog_entries = catalog_json
        .as_array()
        .ok_or_else(|| PackError::Event("catalog must be an array".into()))?;

    for field in referenced_fields {
        let Some(reference) = &field.reference else {
            continue;
        };
        let Some(value) = payload.get(&field.name) else {
            continue;
        };
        if value.is_null() && field.optional {
            continue;
        }

        let target = value.as_str().ok_or_else(|| {
            PackError::Event(format!(
                "field '{}' reference value must be a string",
                field.name
            ))
        })?;
        match count_flat_catalog_matches(catalog_entries, &reference.field, target) {
            1 => {}
            0 => {
                return Err(PackError::Event(format!(
                    "field '{}' references missing catalog entry '{}.{}={}'",
                    field.name, reference.catalog, reference.field, target
                )));
            }
            _ => {
                return Err(PackError::Event(format!(
                    "field '{}' references non-unique catalog entry '{}.{}={}'",
                    field.name, reference.catalog, reference.field, target
                )));
            }
        }
    }

    Ok(())
}

fn count_flat_catalog_matches(entries: &[Value], field: &str, target: &str) -> usize {
    entries
        .iter()
        .filter(|entry| {
            entry
                .get(field)
                .and_then(Value::as_str)
                .map(|candidate| candidate == target)
                .unwrap_or(false)
        })
        .count()
}
