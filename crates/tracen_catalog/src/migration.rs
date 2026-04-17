//! Catalog migration system

use super::types::*;
use super::TrackerError;

/// Migration trait for catalog upgrades
pub trait Migration {
    /// Source version
    fn source_version(&self) -> CatalogVersion;

    /// Target version
    fn target_version(&self) -> CatalogVersion;

    /// Apply migration to an entry
    fn migrate_entry(&self, entry: CatalogEntry) -> Result<CatalogEntry, TrackerError>;
}

/// Migration registry
pub struct MigrationRegistry {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationRegistry {
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }

    pub fn register(&mut self, migration: Box<dyn Migration>) {
        self.migrations.push(migration);
    }

    /// Find migration path from one version to another
    pub fn find_path(&self, from: CatalogVersion, to: CatalogVersion) -> Vec<&dyn Migration> {
        // Simple linear migration (assumes migrations are registered in order)
        self.migrations
            .iter()
            .filter(|m| {
                m.source_version().major >= from.major && m.target_version().major <= to.major
            })
            .map(|m| m.as_ref())
            .collect()
    }
}

impl Default for MigrationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// No-op migration for same version
pub struct NoOpMigration;

impl Migration for NoOpMigration {
    fn source_version(&self) -> CatalogVersion {
        CatalogVersion::new(1, 0, 0)
    }

    fn target_version(&self) -> CatalogVersion {
        CatalogVersion::new(1, 0, 0)
    }

    fn migrate_entry(&self, entry: CatalogEntry) -> Result<CatalogEntry, TrackerError> {
        Ok(entry)
    }
}

/// Example: Rename attribute migration
pub struct RenameAttributeMigration {
    from_attr: String,
    to_attr: String,
}

impl RenameAttributeMigration {
    pub fn new(from: impl Into<String>, to: impl Into<String>) -> Self {
        Self {
            from_attr: from.into(),
            to_attr: to.into(),
        }
    }
}

impl Migration for RenameAttributeMigration {
    fn source_version(&self) -> CatalogVersion {
        CatalogVersion::new(1, 0, 0)
    }

    fn target_version(&self) -> CatalogVersion {
        CatalogVersion::new(1, 1, 0)
    }

    fn migrate_entry(&self, mut entry: CatalogEntry) -> Result<CatalogEntry, TrackerError> {
        if let Some(value) = entry.get_attribute(&self.from_attr).cloned() {
            if let Some(obj) = entry.attributes.as_object_mut() {
                obj.remove(&self.from_attr);
                obj.insert(self.to_attr.clone(), value);
            }
        }
        entry.version = self.target_version();
        Ok(entry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_op_migration() {
        let migration = NoOpMigration;
        let entry = CatalogEntry::new(CatalogId::new("test"), "Test", "test");

        let result = migration.migrate_entry(entry.clone()).unwrap();
        assert_eq!(result.id, entry.id);
    }

    #[test]
    fn rename_attribute_migration() {
        let migration = RenameAttributeMigration::new("old_name", "new_name");
        let entry = CatalogEntry::new(CatalogId::new("test"), "Test", "test")
            .with_attribute("old_name", "value");

        let result = migration.migrate_entry(entry).unwrap();
        assert!(result.get_attribute("new_name").is_some());
        assert!(result.get_attribute("old_name").is_none());
    }
}
