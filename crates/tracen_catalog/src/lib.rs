//! tracen_catalog - Generic catalog management system
//!
//! Provides domain-agnostic catalog storage with hybrid JSON/SQLite backend.
//! Used for categories, labels, tags, and other catalog data.

pub mod migration;
pub mod storage;
pub mod types;
pub mod validation;

pub use migration::*;
pub use storage::*;
pub use types::*;
pub use validation::*;

// Re-export error types for convenience
pub use tracen_ir::error::{ErrorCode, TrackerError, TrackerResult};

/// Current catalog version
pub const CATALOG_VERSION_MAJOR: u32 = 1;
pub const CATALOG_VERSION_MINOR: u32 = 0;
pub const CATALOG_VERSION_PATCH: u32 = 0;

/// Get current catalog version
pub fn current_version() -> CatalogVersion {
    CatalogVersion::new(
        CATALOG_VERSION_MAJOR,
        CATALOG_VERSION_MINOR,
        CATALOG_VERSION_PATCH,
    )
}
