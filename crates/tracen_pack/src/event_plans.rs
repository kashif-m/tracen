use serde_json::Value;
use std::collections::HashSet;
use tracen_ir::{
    schema_validation::{validate_payload_fields, PayloadValidationError, PayloadValidationPolicy},
    EventDraft, EventPlan, TrackerDefinition,
};

use crate::{catalog_references, PackError};

pub(crate) fn validate_event_plan(
    definition: &TrackerDefinition,
    plan_json: &str,
    catalog_json: &Value,
) -> Result<EventPlan, PackError> {
    if !definition
        .event_plans()
        .map(|event_plans| event_plans.enabled)
        .unwrap_or(false)
    {
        return Err(PackError::InvalidQuery(
            "event plans are not enabled for this tracker".into(),
        ));
    }

    let mut plan: EventPlan = serde_json::from_str(plan_json)
        .map_err(|err| PackError::Event(format!("parse event plan: {err}")))?;
    plan.id = normalize_required_id("event plan id", plan.id)?;
    plan.name = normalize_required_id("event plan name", plan.name)?;
    if let Some(description) = plan.description.take() {
        let trimmed = description.trim().to_string();
        plan.description = if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        };
    }
    if plan.meta.is_null() {
        plan.meta = serde_json::json!({});
    }
    if !plan.meta.is_object() {
        return Err(PackError::Event("event plan meta must be an object".into()));
    }

    let mut item_ids = HashSet::new();
    for item in &mut plan.items {
        item.id = normalize_required_id("event plan item id", std::mem::take(&mut item.id))?;
        if !item_ids.insert(item.id.clone()) {
            return Err(PackError::Event(format!(
                "duplicate event plan item id: {}",
                item.id
            )));
        }
        if item.payload.is_null() {
            item.payload = serde_json::json!({});
        }
        validate_draft_payload(definition, &item.payload, catalog_json)?;
        if item.meta.is_null() {
            item.meta = serde_json::json!({});
        }
        if !item.meta.is_object() {
            return Err(PackError::Event(format!(
                "event plan item '{}' meta must be an object",
                item.id
            )));
        }
    }

    Ok(plan)
}

pub(crate) fn instantiate_event_plan(
    definition: &TrackerDefinition,
    plan_json: &str,
    catalog_json: &Value,
) -> Result<Vec<EventDraft>, PackError> {
    let plan = validate_event_plan(definition, plan_json, catalog_json)?;
    Ok(plan
        .items
        .into_iter()
        .map(|item| EventDraft {
            plan_id: plan.id.clone(),
            plan_revision: plan.revision,
            plan_item_id: item.id,
            payload: item.payload,
            meta: item.meta,
        })
        .collect())
}

fn normalize_required_id(label: &str, value: String) -> Result<String, PackError> {
    let trimmed = value.trim().to_string();
    if trimmed.is_empty() {
        return Err(PackError::Event(format!("{label} cannot be empty")));
    }
    Ok(trimmed)
}

fn validate_draft_payload(
    definition: &TrackerDefinition,
    payload: &Value,
    catalog_json: &Value,
) -> Result<(), PackError> {
    if !payload.is_object() {
        return Err(PackError::Event(
            "event plan item payload must be an object".into(),
        ));
    }
    let mut draft_payload = payload.clone();
    validate_payload_fields(
        definition.fields(),
        &mut draft_payload,
        PayloadValidationPolicy::PartialDraft,
    )
    .map_err(to_pack_payload_error)?;
    catalog_references::validate_payload_references(definition, payload, catalog_json)
}

fn to_pack_payload_error(error: PayloadValidationError) -> PackError {
    PackError::Event(error.to_string())
}
