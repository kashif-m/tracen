//! Storage backends for catalog

use super::types::*;
use super::TrackerError;
use std::path::Path;

/// Trait for catalog storage backends
pub trait CatalogStorage: Send + Sync {
    /// Load default entries from bundled source
    fn load_defaults(&self) -> Result<Vec<CatalogEntry>, TrackerError>;

    /// Load user-created/modified entries
    fn load_custom(&self) -> Result<Vec<CatalogEntry>, TrackerError>;

    /// Save entry
    fn save(&mut self, entry: &CatalogEntry) -> Result<(), TrackerError>;

    /// Delete entry
    fn delete(&mut self, id: &CatalogId) -> Result<(), TrackerError>;

    /// Check if entry exists
    fn exists(&self, id: &CatalogId) -> bool;

    /// Get catalog version
    fn version(&self) -> CatalogVersion;
}

/// JSON file storage for default/bundled catalogs
pub struct JsonFileStorage {
    path: std::path::PathBuf,
}

impl JsonFileStorage {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn from_json(json: &str) -> Result<Vec<CatalogEntry>, TrackerError> {
        serde_json::from_str(json).map_err(|e| {
            TrackerError::new_simple(
                tracen_ir::error::ErrorCode::DeserializationFailed,
                format!("Failed to parse catalog JSON: {}", e),
            )
        })
    }
}

impl CatalogStorage for JsonFileStorage {
    fn load_defaults(&self) -> Result<Vec<CatalogEntry>, TrackerError> {
        if !self.path.exists() {
            Ok(Vec::new())
        } else {
            let content = std::fs::read_to_string(&self.path).map_err(|e| {
                TrackerError::new_simple(
                    tracen_ir::error::ErrorCode::FileIoError,
                    format!("Failed to read catalog file: {}", e),
                )
            })?;

            Self::from_json(&content)
        }
    }

    fn load_custom(&self) -> Result<Vec<CatalogEntry>, TrackerError> {
        // JSON file storage only supports defaults
        Ok(Vec::new())
    }

    fn save(&mut self, _entry: &CatalogEntry) -> Result<(), TrackerError> {
        Err(TrackerError::new_simple(
            tracen_ir::error::ErrorCode::StorageError,
            "JSON file storage is read-only",
        ))
    }

    fn delete(&mut self, _id: &CatalogId) -> Result<(), TrackerError> {
        Err(TrackerError::new_simple(
            tracen_ir::error::ErrorCode::StorageError,
            "JSON file storage is read-only",
        ))
    }

    fn exists(&self, _id: &CatalogId) -> bool {
        false
    }

    fn version(&self) -> CatalogVersion {
        CatalogVersion::new(1, 0, 0)
    }
}

/// In-memory storage for testing
pub struct MemoryStorage {
    entries: std::collections::HashMap<CatalogId, CatalogEntry>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
        }
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl CatalogStorage for MemoryStorage {
    fn load_defaults(&self) -> Result<Vec<CatalogEntry>, TrackerError> {
        Ok(self.entries.values().cloned().collect())
    }

    fn load_custom(&self) -> Result<Vec<CatalogEntry>, TrackerError> {
        Ok(self.entries.values().cloned().collect())
    }

    fn save(&mut self, entry: &CatalogEntry) -> Result<(), TrackerError> {
        self.entries.insert(entry.id.clone(), entry.clone());
        Ok(())
    }

    fn delete(&mut self, id: &CatalogId) -> Result<(), TrackerError> {
        self.entries.remove(id);
        Ok(())
    }

    fn exists(&self, id: &CatalogId) -> bool {
        self.entries.contains_key(id)
    }

    fn version(&self) -> CatalogVersion {
        CatalogVersion::new(1, 0, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_storage_basic() {
        let mut storage = MemoryStorage::new();

        let entry = CatalogEntry::new(CatalogId::new("test"), "Test", "test");

        storage.save(&entry).unwrap();
        assert!(storage.exists(&CatalogId::new("test")));

        let loaded = storage.load_custom().unwrap();
        assert_eq!(loaded.len(), 1);

        storage.delete(&CatalogId::new("test")).unwrap();
        assert!(!storage.exists(&CatalogId::new("test")));
    }
}
