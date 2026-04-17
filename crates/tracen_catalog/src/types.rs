//! Catalog types - Domain-agnostic catalog data structures

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracen_ir::Timestamp;

/// Unique identifier for catalog entries
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CatalogId(String);

impl CatalogId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Generate ID from slug
    pub fn from_slug(slug: &str) -> Self {
        Self(slug.to_lowercase().replace(' ', "_"))
    }
}

impl std::fmt::Display for CatalogId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Catalog version (semver-like)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl CatalogVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Parse from string (e.g., "1.2.3")
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            None
        } else {
            Some(Self::new(
                parts[0].parse().ok()?,
                parts[1].parse().ok()?,
                parts[2].parse().ok()?,
            ))
        }
    }

    /// Check if this version is compatible with another
    /// (same major version)
    pub fn is_compatible_with(&self, other: &CatalogVersion) -> bool {
        self.major == other.major
    }
}

impl std::fmt::Display for CatalogVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// Source of catalog entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntrySource {
    /// Bundled with application
    Default,
    /// Created by user
    Custom,
    /// Modified from default
    Modified,
    /// Imported from external source
    Imported,
}

impl EntrySource {
    pub fn is_editable(&self) -> bool {
        !matches!(self, EntrySource::Default)
    }
}

/// A catalog entry (domain-agnostic)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    /// Unique identifier
    pub id: CatalogId,

    /// Human-readable display name
    pub display_name: String,

    /// URL-friendly identifier
    pub slug: String,

    /// Domain-specific attributes (JSON object)
    pub attributes: serde_json::Value,

    /// Tags for categorization
    #[serde(default)]
    pub tags: Vec<String>,

    /// Version of this entry
    pub version: CatalogVersion,

    /// Source of entry
    pub source: EntrySource,

    /// Creation timestamp
    pub created_at: Timestamp,

    /// Last update timestamp
    pub updated_at: Timestamp,

    /// Archived flag
    #[serde(default)]
    pub archived: bool,

    /// Hidden flag
    #[serde(default)]
    pub hidden: bool,
}

impl CatalogEntry {
    /// Create new entry
    pub fn new(id: CatalogId, display_name: impl Into<String>, slug: impl Into<String>) -> Self {
        let now = Timestamp::new(chrono::Utc::now().timestamp_millis());
        Self {
            id,
            display_name: display_name.into(),
            slug: slug.into(),
            attributes: serde_json::Value::Object(serde_json::Map::new()),
            tags: Vec::new(),
            version: CatalogVersion::new(1, 0, 0),
            source: EntrySource::Custom,
            created_at: now,
            updated_at: now,
            archived: false,
            hidden: false,
        }
    }

    /// Set an attribute
    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Serialize) -> Self {
        if let Ok(value) = serde_json::to_value(value) {
            if let Some(obj) = self.attributes.as_object_mut() {
                obj.insert(key.into(), value);
            }
        }
        self
    }

    /// Add tags
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    /// Set source
    pub fn with_source(mut self, source: EntrySource) -> Self {
        self.source = source;
        self
    }

    /// Get attribute value
    pub fn get_attribute(&self, key: &str) -> Option<&serde_json::Value> {
        self.attributes.get(key)
    }

    /// Check if entry has tag
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t.eq_ignore_ascii_case(tag))
    }
}

/// Complete catalog with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    /// Catalog type identifier (e.g., "domain_a", "domain_b")
    pub catalog_type: String,

    /// Catalog version
    pub version: CatalogVersion,

    /// All entries
    pub entries: HashMap<CatalogId, CatalogEntry>,
}

impl Catalog {
    /// Create empty catalog
    pub fn new(catalog_type: impl Into<String>) -> Self {
        Self {
            catalog_type: catalog_type.into(),
            version: CatalogVersion::new(1, 0, 0),
            entries: HashMap::new(),
        }
    }

    /// Add entry
    pub fn add_entry(&mut self, entry: CatalogEntry) {
        self.entries.insert(entry.id.clone(), entry);
    }

    /// Get entry by ID
    pub fn get(&self, id: &CatalogId) -> Option<&CatalogEntry> {
        self.entries.get(id)
    }

    /// Get mutable entry
    pub fn get_mut(&mut self, id: &CatalogId) -> Option<&mut CatalogEntry> {
        self.entries.get_mut(id)
    }

    /// Remove entry
    pub fn remove(&mut self, id: &CatalogId) -> Option<CatalogEntry> {
        self.entries.remove(id)
    }

    /// Find entries by tag
    pub fn find_by_tag(&self, tag: &str) -> Vec<&CatalogEntry> {
        self.entries
            .values()
            .filter(|e| e.has_tag(tag) && !e.archived && !e.hidden)
            .collect()
    }

    /// Find entries by attribute
    pub fn find_by_attribute(&self, key: &str, value: &serde_json::Value) -> Vec<&CatalogEntry> {
        self.entries
            .values()
            .filter(|e| e.get_attribute(key).map(|v| v == value).unwrap_or(false))
            .collect()
    }

    /// Get all visible (non-archived, non-hidden) entries
    pub fn visible_entries(&self) -> Vec<&CatalogEntry> {
        self.entries
            .values()
            .filter(|e| !e.archived && !e.hidden)
            .collect()
    }

    /// Count entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_id_creation() {
        let id = CatalogId::new("test_entry");
        assert_eq!(id.as_str(), "test_entry");
    }

    #[test]
    fn catalog_id_from_slug() {
        let id = CatalogId::from_slug("Test Entry");
        assert_eq!(id.as_str(), "test_entry");
    }

    #[test]
    fn version_parsing() {
        let v = CatalogVersion::parse("1.2.3").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);
    }

    #[test]
    fn version_compatibility() {
        let v1 = CatalogVersion::new(1, 0, 0);
        let v2 = CatalogVersion::new(1, 5, 0);
        let v3 = CatalogVersion::new(2, 0, 0);

        assert!(v1.is_compatible_with(&v2));
        assert!(v2.is_compatible_with(&v1));
        assert!(!v1.is_compatible_with(&v3));
    }

    #[test]
    fn catalog_entry_builder() {
        let entry = CatalogEntry::new(CatalogId::new("item_alpha"), "Item Alpha", "item_alpha")
            .with_attribute("group_key", "segment_a")
            .with_tags(vec!["tag_a".to_string(), "tag_b".to_string()]);

        assert_eq!(entry.display_name, "Item Alpha");
        assert_eq!(entry.get_attribute("group_key").unwrap(), "segment_a");
        assert!(entry.has_tag("tag_a"));
    }

    #[test]
    fn catalog_operations() {
        let mut catalog = Catalog::new("domain_a");

        let entry1 = CatalogEntry::new(CatalogId::new("item1"), "Item 1", "item1");

        let entry2 = CatalogEntry::new(CatalogId::new("item2"), "Item 2", "item2")
            .with_tags(vec!["tag1".to_string()]);

        catalog.add_entry(entry1);
        catalog.add_entry(entry2);

        assert_eq!(catalog.len(), 2);
        assert!(catalog.get(&CatalogId::new("item1")).is_some());

        let tagged = catalog.find_by_tag("tag1");
        assert_eq!(tagged.len(), 1);
    }
}
