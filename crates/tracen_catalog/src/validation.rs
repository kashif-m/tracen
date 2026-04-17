//! Catalog validation

use super::types::*;
use super::TrackerError;

/// Validation rules for catalog entries
pub struct ValidationRules {
    pub required_attributes: Vec<String>,
    pub allowed_tags: Option<Vec<String>>,
    pub max_display_name_length: usize,
}

impl Default for ValidationRules {
    fn default() -> Self {
        Self {
            required_attributes: Vec::new(),
            allowed_tags: None,
            max_display_name_length: 100,
        }
    }
}

/// Validate a catalog entry
pub fn validate_entry(entry: &CatalogEntry, rules: &ValidationRules) -> Result<(), TrackerError> {
    if entry.display_name.is_empty() {
        Err(TrackerError::new_simple(
            tracen_ir::error::ErrorCode::CatalogValidationFailed,
            "Display name cannot be empty",
        ))?;
    } else if entry.display_name.len() > rules.max_display_name_length {
        Err(TrackerError::new_simple(
            tracen_ir::error::ErrorCode::CatalogValidationFailed,
            format!(
                "Display name too long (max {} characters)",
                rules.max_display_name_length
            ),
        ))?;
    } else if entry.slug.is_empty() {
        Err(TrackerError::new_simple(
            tracen_ir::error::ErrorCode::CatalogValidationFailed,
            "Slug cannot be empty",
        ))?;
    } else if !is_valid_slug(&entry.slug) {
        Err(TrackerError::new_simple(
            tracen_ir::error::ErrorCode::CatalogValidationFailed,
            "Slug contains invalid characters",
        ))?;
    }

    for attr in &rules.required_attributes {
        if entry.get_attribute(attr).is_none() {
            Err(TrackerError::new_simple(
                tracen_ir::error::ErrorCode::CatalogValidationFailed,
                format!("Missing required attribute: {}", attr),
            ))?;
        }
    }

    if let Some(ref allowed) = rules.allowed_tags {
        for tag in &entry.tags {
            if !allowed.iter().any(|a| a.eq_ignore_ascii_case(tag)) {
                Err(TrackerError::new_simple(
                    tracen_ir::error::ErrorCode::CatalogValidationFailed,
                    format!("Invalid tag: {}", tag),
                ))?;
            }
        }
    }

    Ok(())
}

/// Check if slug is valid (alphanumeric, underscores, hyphens)
fn is_valid_slug(slug: &str) -> bool {
    slug.chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
}

/// Sanitize a string to make it a valid slug
pub fn sanitize_slug(s: &str) -> String {
    s.to_lowercase()
        .replace(' ', "_")
        .replace("-", "_")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_valid_entry() {
        let entry = CatalogEntry::new(CatalogId::new("test"), "Test Entry", "test_entry");

        let rules = ValidationRules::default();
        assert!(validate_entry(&entry, &rules).is_ok());
    }

    #[test]
    fn validate_empty_display_name() {
        let entry = CatalogEntry::new(CatalogId::new("test"), "", "test");

        let rules = ValidationRules::default();
        assert!(validate_entry(&entry, &rules).is_err());
    }

    #[test]
    fn validate_invalid_slug() {
        let entry = CatalogEntry::new(
            CatalogId::new("test"),
            "Test",
            "test entry!", // Invalid: space and !
        );

        let rules = ValidationRules::default();
        assert!(validate_entry(&entry, &rules).is_err());
    }

    #[test]
    fn test_sanitize_slug() {
        assert_eq!(sanitize_slug("Test Entry"), "test_entry");
        assert_eq!(sanitize_slug("Test-Entry!"), "test_entry");
        assert_eq!(sanitize_slug("Test 123"), "test_123");
    }
}
