//! Stable error codes for FFI boundaries
//!
//! Codes are STABLE across releases. New codes are ADDED only, never changed.
//! Each code is a u16 for efficient FFI serialization and machine parsing.

use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use std::fmt;

/// Stable numeric error codes (u16 for FFI compatibility)
///
/// # Stability Guarantee
/// These codes will NEVER change between releases. New codes are added with
/// higher numbers. Deprecated codes remain but may be unused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize_repr, Deserialize_repr)]
#[repr(u16)]
#[non_exhaustive]
pub enum ErrorCode {
    // Success (0)
    /// Operation completed successfully
    Success = 0,

    // Parsing errors (1000-1099)
    /// General DSL parsing error
    DslParseError = 1000,
    /// DSL syntax violation
    DslSyntaxError = 1001,
    /// Invalid tracker version specified
    DslInvalidVersion = 1002,
    /// Unknown type in field definition
    DslUnknownType = 1003,
    /// Invalid expression in derive/metric/alert
    DslInvalidExpression = 1004,
    /// Unclosed brace or parenthesis
    DslUnclosedDelimiter = 1005,
    /// Unexpected token
    DslUnexpectedToken = 1006,
    /// Reserved for future parsing errors
    _ParsingReservedStart = 1007,
    _ParsingReservedEnd = 1099,

    // Validation errors (1100-1199)
    /// Event validation failed
    EventValidationFailed = 1100,
    /// Tracker ID mismatch between definition and event
    TrackerMismatch = 1101,
    /// State tracker ID doesn't match definition
    StateMismatch = 1102,
    /// Referenced field not found in schema
    FieldNotFound = 1103,
    /// Type mismatch in expression or field
    TypeMismatch = 1104,
    /// Required field missing from event
    RequiredFieldMissing = 1105,
    /// Field value is invalid
    InvalidFieldValue = 1106,
    /// Schema validation failed
    SchemaValidationFailed = 1107,
    /// Invalid timestamp format
    InvalidTimestamp = 1108,
    /// Event ID is empty or invalid
    InvalidEventId = 1109,
    /// Reserved for future validation errors
    _ValidationReservedStart = 1110,
    _ValidationReservedEnd = 1199,

    // Evaluation errors (1200-1299)
    /// Metric evaluation failed
    MetricEvaluationFailed = 1200,
    /// Division by zero in expression
    DivisionByZero = 1201,
    /// Aggregation computation error
    AggregationError = 1202,
    /// Expression evaluation error
    ExpressionError = 1203,
    /// Circular dependency detected in derived fields
    CircularDependency = 1204,
    /// Invalid aggregation function
    InvalidAggregation = 1205,
    /// Time window is invalid
    InvalidTimeWindow = 1206,
    /// Reserved for future evaluation errors
    _EvaluationReservedStart = 1207,
    _EvaluationReservedEnd = 1299,

    // Catalog errors (1300-1399)
    /// Catalog entry not found
    CatalogEntryNotFound = 1300,
    /// Catalog entry already exists
    CatalogEntryExists = 1301,
    /// Catalog validation failed
    CatalogValidationFailed = 1302,
    /// Catalog migration failed
    CatalogMigrationFailed = 1303,
    /// Invalid catalog version
    CatalogVersionMismatch = 1304,
    /// Reserved for future catalog errors
    _CatalogReservedStart = 1305,
    _CatalogReservedEnd = 1399,

    // Planning errors (1400-1499)
    /// No baseline events for planning
    PlanningNoBaseline = 1400,
    /// Invalid planning strategy
    PlanningInvalidStrategy = 1401,
    /// Planning simulation failed
    PlanningSimulationFailed = 1402,
    /// No candidates generated
    PlanningNoCandidates = 1403,
    /// Reserved for future planning errors
    _PlanningReservedStart = 1404,
    _PlanningReservedEnd = 1499,

    // Storage/IO errors (1500-1599)
    /// JSON serialization failed
    SerializationFailed = 1500,
    /// JSON deserialization failed
    DeserializationFailed = 1501,
    /// Storage operation failed
    StorageError = 1502,
    /// FFI binding error
    FfiBindingError = 1503,
    /// SQLite operation failed
    SqliteError = 1504,
    /// File I/O error
    FileIoError = 1505,
    /// Reserved for future storage errors
    _StorageReservedStart = 1506,
    _StorageReservedEnd = 1599,

    // Unknown (65535)
    /// Unknown or unclassified error
    Unknown = 65535,
}

impl ErrorCode {
    /// Get the error category name
    pub fn category(&self) -> &'static str {
        let code = *self as u16;
        match code {
            0 => "success",
            1000..=1099 => "parsing",
            1100..=1199 => "validation",
            1200..=1299 => "evaluation",
            1300..=1399 => "catalog",
            1400..=1499 => "planning",
            1500..=1599 => "storage",
            _ => "unknown",
        }
    }

    /// Get the severity level for UI handling
    pub fn severity(&self) -> ErrorSeverity {
        match *self {
            Self::Success => ErrorSeverity::Success,
            Self::FieldNotFound | Self::CatalogEntryNotFound | Self::PlanningNoCandidates => {
                ErrorSeverity::Warning
            }
            Self::DivisionByZero
            | Self::CircularDependency
            | Self::StorageError
            | Self::SqliteError
            | Self::FileIoError => ErrorSeverity::Fatal,
            _ => ErrorSeverity::Error,
        }
    }

    /// Check if this is a reserved code
    pub fn is_reserved(&self) -> bool {
        matches!(
            self,
            Self::_ParsingReservedStart
                | Self::_ParsingReservedEnd
                | Self::_ValidationReservedStart
                | Self::_ValidationReservedEnd
                | Self::_EvaluationReservedStart
                | Self::_EvaluationReservedEnd
                | Self::_CatalogReservedStart
                | Self::_CatalogReservedEnd
                | Self::_PlanningReservedStart
                | Self::_PlanningReservedEnd
                | Self::_StorageReservedStart
                | Self::_StorageReservedEnd
        )
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_reserved() {
            write!(f, "RESERVED({})", *self as u16)
        } else {
            write!(f, "{}", *self as u16)
        }
    }
}

/// Error severity levels for UI handling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorSeverity {
    /// Operation succeeded
    Success,
    /// Informational message
    Info,
    /// Warning - operation succeeded but with issues
    Warning,
    /// Error - operation failed but app can continue
    Error,
    /// Fatal error - app may need to restart
    Fatal,
}

impl ErrorSeverity {
    /// HTTP-like status code mapping for reference
    pub fn http_equivalent(&self) -> u16 {
        match self {
            Self::Success => 200,
            Self::Info => 100,
            Self::Warning => 299,
            Self::Error => 400,
            Self::Fatal => 500,
        }
    }
}

/// Structured error with stable code and machine-readable context
///
/// This is the primary error type for FFI boundaries. It serializes to JSON
/// for cross-language communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackerError {
    /// Stable numeric error code
    pub code: ErrorCode,

    /// Human-readable error message
    pub message: String,

    /// Machine-readable context (field values, line numbers, etc.)
    /// Use serde_json::json!({...}) to construct
    #[serde(default = "default_context")]
    pub context: serde_json::Value,

    /// Error severity (derived from code by default)
    #[serde(default = "default_severity")]
    pub severity: ErrorSeverity,

    /// Source location where error occurred (file:line)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_location: Option<String>,

    /// Timestamp when error occurred (milliseconds since epoch)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<i64>,
}

fn default_context() -> serde_json::Value {
    serde_json::Value::Null
}

fn default_severity() -> ErrorSeverity {
    ErrorSeverity::Error
}

impl TrackerError {
    /// Create a new error with just code and message
    #[track_caller]
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        let location = std::panic::Location::caller();
        Self {
            code,
            message: message.into(),
            context: serde_json::Value::Null,
            severity: code.severity(),
            source_location: Some(format!("{}:{}", location.file(), location.line())),
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
        }
    }

    /// Create a simple error without source tracking (for tests)
    pub fn new_simple(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            context: serde_json::Value::Null,
            severity: code.severity(),
            source_location: None,
            timestamp_ms: None,
        }
    }

    /// Add context to the error (fluent API)
    pub fn with_context(mut self, context: serde_json::Value) -> Self {
        self.context = context;
        self
    }

    /// Override severity (fluent API)
    pub fn with_severity(mut self, severity: ErrorSeverity) -> Self {
        self.severity = severity;
        self
    }

    /// Set source location manually (fluent API)
    pub fn at_location(mut self, file: &str, line: u32) -> Self {
        self.source_location = Some(format!("{}:{}", file, line));
        self
    }

    /// Serialize to JSON string for FFI boundary
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| {
            format!(
                r#"{{"code":{},"message":"Internal serialization error","severity":"fatal"}}"#,
                ErrorCode::SerializationFailed as u16
            )
        })
    }

    /// Serialize to JSON Value
    pub fn to_json_value(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_else(|_| {
            serde_json::json!({
                "code": ErrorCode::SerializationFailed as u16,
                "message": "Internal serialization error",
                "severity": "fatal"
            })
        })
    }

    /// Deserialize from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Check if this is a success code
    pub fn is_success(&self) -> bool {
        matches!(self.code, ErrorCode::Success)
    }

    /// Check if this is an error (non-success)
    pub fn is_error(&self) -> bool {
        !self.is_success()
    }

    /// Check if this is a fatal error
    pub fn is_fatal(&self) -> bool {
        matches!(self.severity, ErrorSeverity::Fatal)
    }

    /// Convert to a Result
    pub fn into_result<T>(self) -> Result<T, Self> {
        if self.is_success() {
            panic!("Cannot convert success error into Err")
        } else {
            Err(self)
        }
    }
}

impl fmt::Display for TrackerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.code as u16, self.message)?;
        if let Some(ref loc) = self.source_location {
            write!(f, " at {}", loc)?;
        }
        Ok(())
    }
}

impl std::error::Error for TrackerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// Convenience type alias
pub type TrackerResult<T> = Result<T, TrackerError>;

/// Helper to create a success result
pub fn success<T>(value: T) -> TrackerResult<T> {
    Ok(value)
}

/// Helper to create an error result
#[track_caller]
pub fn error<T>(code: ErrorCode, message: impl Into<String>) -> TrackerResult<T> {
    Err(TrackerError::new(code, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_are_unique() {
        // Ensure no duplicate codes
        let codes = vec![
            ErrorCode::Success,
            ErrorCode::DslParseError,
            ErrorCode::EventValidationFailed,
            ErrorCode::MetricEvaluationFailed,
            ErrorCode::CatalogEntryNotFound,
            ErrorCode::PlanningNoBaseline,
            ErrorCode::SerializationFailed,
            ErrorCode::Unknown,
        ];

        let mut seen = std::collections::HashSet::new();
        for code in codes {
            let code_num = code as u16;
            assert!(
                seen.insert(code_num),
                "Duplicate error code: {:?} = {}",
                code,
                code_num
            );
        }
    }

    #[test]
    fn error_code_categories() {
        assert_eq!(ErrorCode::Success.category(), "success");
        assert_eq!(ErrorCode::DslParseError.category(), "parsing");
        assert_eq!(ErrorCode::EventValidationFailed.category(), "validation");
        assert_eq!(ErrorCode::MetricEvaluationFailed.category(), "evaluation");
        assert_eq!(ErrorCode::CatalogEntryNotFound.category(), "catalog");
        assert_eq!(ErrorCode::PlanningNoBaseline.category(), "planning");
        assert_eq!(ErrorCode::SerializationFailed.category(), "storage");
        assert_eq!(ErrorCode::Unknown.category(), "unknown");
    }

    #[test]
    fn error_severity_levels() {
        assert!(matches!(
            ErrorCode::Success.severity(),
            ErrorSeverity::Success
        ));
        assert!(matches!(
            ErrorCode::FieldNotFound.severity(),
            ErrorSeverity::Warning
        ));
        assert!(matches!(
            ErrorCode::DslParseError.severity(),
            ErrorSeverity::Error
        ));
        assert!(matches!(
            ErrorCode::DivisionByZero.severity(),
            ErrorSeverity::Fatal
        ));
    }

    #[test]
    fn tracker_error_creation() {
        let err = TrackerError::new_simple(ErrorCode::DslParseError, "test error");
        assert_eq!(err.code, ErrorCode::DslParseError);
        assert_eq!(err.message, "test error");
        assert!(matches!(err.severity, ErrorSeverity::Error));
    }

    #[test]
    fn tracker_error_with_context() {
        let err = TrackerError::new_simple(ErrorCode::EventValidationFailed, "validation failed")
            .with_context(serde_json::json!({"field": "value_a", "value": -5}));

        assert_eq!(err.context["field"], "value_a");
        assert_eq!(err.context["value"], -5);
    }

    #[test]
    fn tracker_error_json_roundtrip() {
        let original = TrackerError::new_simple(ErrorCode::TrackerMismatch, "tracker mismatch")
            .with_context(serde_json::json!({"expected": "domain_a", "actual": "domain_b"}))
            .at_location("test.rs", 42);

        let json = original.to_json();
        let restored = TrackerError::from_json(&json).expect("deserialization failed");

        assert_eq!(restored.code, original.code);
        assert_eq!(restored.message, original.message);
        assert_eq!(restored.context, original.context);
        assert_eq!(restored.source_location, original.source_location);
    }

    #[test]
    fn display_format() {
        let err = TrackerError::new_simple(ErrorCode::DivisionByZero, "division by zero")
            .at_location("math.rs", 10);

        let display = format!("{}", err);
        assert!(display.contains("1201")); // Error code
        assert!(display.contains("division by zero"));
        assert!(display.contains("math.rs:10"));
    }

    #[test]
    fn is_success_and_is_error() {
        let success = TrackerError::new_simple(ErrorCode::Success, "ok");
        let error = TrackerError::new_simple(ErrorCode::Unknown, "fail");

        assert!(success.is_success());
        assert!(!success.is_error());

        assert!(!error.is_success());
        assert!(error.is_error());
    }

    #[test]
    fn error_code_display() {
        assert_eq!(format!("{}", ErrorCode::DslParseError), "1000");
        assert_eq!(format!("{}", ErrorCode::Success), "0");
    }

    #[test]
    fn severity_http_equivalent() {
        assert_eq!(ErrorSeverity::Success.http_equivalent(), 200);
        assert_eq!(ErrorSeverity::Info.http_equivalent(), 100);
        assert_eq!(ErrorSeverity::Warning.http_equivalent(), 299);
        assert_eq!(ErrorSeverity::Error.http_equivalent(), 400);
        assert_eq!(ErrorSeverity::Fatal.http_equivalent(), 500);
    }
}
