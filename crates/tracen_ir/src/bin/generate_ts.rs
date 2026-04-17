//! TypeScript Type Generator
//!
//! Generates TypeScript type definitions from Rust error code definitions.
//! Run with: cargo run -p tracen_ir --bin generate_ts

use std::fmt::Write;
use std::path::PathBuf;

fn main() {
    let ts_code = generate_typescript_types();

    // Find output path
    let output_path = find_output_path();
    println!("Generating TypeScript types at: {}", output_path.display());

    // Write file
    std::fs::write(&output_path, ts_code).expect("Failed to write TypeScript file");
    println!("✓ Generated TypeScript types successfully");
}

fn find_output_path() -> PathBuf {
    // Prefer repository-level app view directory when present.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .expect("No parent")
        .parent()
        .expect("No workspace root");

    let repo_root = workspace_root.parent().unwrap_or(workspace_root);
    let app_view_domain = repo_root.join("view").join("src").join("domain");
    let view_domain = if app_view_domain.parent().is_some() {
        app_view_domain
    } else {
        workspace_root.join("view").join("src").join("domain")
    };

    // Ensure directory exists
    std::fs::create_dir_all(&view_domain).expect("Failed to create domain directory");

    view_domain.join("errors.ts")
}

fn generate_typescript_types() -> String {
    let mut output = String::new();

    // Header
    writeln!(&mut output, "// Auto-generated from Rust error definitions").unwrap();
    writeln!(&mut output, "// Do not edit manually").unwrap();
    writeln!(
        &mut output,
        "// Regenerate with: cargo run -p tracen_ir --bin generate_ts"
    )
    .unwrap();
    writeln!(&mut output).unwrap();

    // ErrorCode enum
    generate_error_code_enum(&mut output);

    // ErrorSeverity enum
    generate_error_severity_enum(&mut output);

    // TrackerError interface
    generate_tracker_error_interface(&mut output);

    // Helper types
    generate_helper_types(&mut output);

    // Utility functions
    generate_utility_functions(&mut output);

    output
}

fn generate_error_code_enum(output: &mut String) {
    writeln!(output, "/**").unwrap();
    writeln!(output, " * Stable error codes for FFI boundaries.").unwrap();
    writeln!(
        output,
        " * These codes are guaranteed to never change between releases."
    )
    .unwrap();
    writeln!(output, " */").unwrap();
    writeln!(output, "export enum ErrorCode {{").unwrap();

    // Generate all error codes using the macro
    generate_error_code_variants(output);

    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();
}

macro_rules! define_error_codes {
    ($($name:ident = $value:expr, $doc:expr);* $(;)?) => {
        fn generate_error_code_variants(output: &mut String) {
            $(
                writeln!(output, "  /** {} */", $doc).unwrap();
                writeln!(output, "  {} = {},", stringify!($name), $value).unwrap();
            )*
        }
    };
}

define_error_codes! {
    Success = 0, "Operation completed successfully";
    DslParseError = 1000, "General DSL parsing error";
    DslSyntaxError = 1001, "DSL syntax violation";
    DslInvalidVersion = 1002, "Invalid tracker version specified";
    DslUnknownType = 1003, "Unknown type in field definition";
    DslInvalidExpression = 1004, "Invalid expression in derive/metric/alert";
    DslUnclosedDelimiter = 1005, "Unclosed brace or parenthesis";
    DslUnexpectedToken = 1006, "Unexpected token";
    EventValidationFailed = 1100, "Event validation failed";
    TrackerMismatch = 1101, "Tracker ID mismatch between definition and event";
    StateMismatch = 1102, "State tracker ID doesn't match definition";
    FieldNotFound = 1103, "Referenced field not found in schema";
    TypeMismatch = 1104, "Type mismatch in expression or field";
    RequiredFieldMissing = 1105, "Required field missing from event";
    InvalidFieldValue = 1106, "Field value is invalid";
    SchemaValidationFailed = 1107, "Schema validation failed";
    InvalidTimestamp = 1108, "Invalid timestamp format";
    InvalidEventId = 1109, "Event ID is empty or invalid";
    MetricEvaluationFailed = 1200, "Metric evaluation failed";
    DivisionByZero = 1201, "Division by zero in expression";
    AggregationError = 1202, "Aggregation computation error";
    ExpressionError = 1203, "Expression evaluation error";
    CircularDependency = 1204, "Circular dependency detected in derived fields";
    InvalidAggregation = 1205, "Invalid aggregation function";
    InvalidTimeWindow = 1206, "Time window is invalid";
    CatalogEntryNotFound = 1300, "Catalog entry not found";
    CatalogEntryExists = 1301, "Catalog entry already exists";
    CatalogValidationFailed = 1302, "Catalog validation failed";
    CatalogMigrationFailed = 1303, "Catalog migration failed";
    CatalogVersionMismatch = 1304, "Invalid catalog version";
    PlanningNoBaseline = 1400, "No baseline events for planning";
    PlanningInvalidStrategy = 1401, "Invalid planning strategy";
    PlanningSimulationFailed = 1402, "Planning simulation failed";
    PlanningNoCandidates = 1403, "No candidates generated";
    SerializationFailed = 1500, "JSON serialization failed";
    DeserializationFailed = 1501, "JSON deserialization failed";
    StorageError = 1502, "Storage operation failed";
    FfiBindingError = 1503, "FFI binding error";
    SqliteError = 1504, "SQLite operation failed";
    FileIoError = 1505, "File I/O error";
    Unknown = 65535, "Unknown or unclassified error";
}

fn generate_error_severity_enum(output: &mut String) {
    writeln!(output, "/**").unwrap();
    writeln!(output, " * Error severity levels").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(output, "export enum ErrorSeverity {{").unwrap();
    writeln!(output, "  Success = 'success',").unwrap();
    writeln!(output, "  Info = 'info',").unwrap();
    writeln!(output, "  Warning = 'warning',").unwrap();
    writeln!(output, "  Error = 'error',").unwrap();
    writeln!(output, "  Fatal = 'fatal',").unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();
}

fn generate_tracker_error_interface(output: &mut String) {
    writeln!(output, "/**").unwrap();
    writeln!(output, " * Structured error type from Rust").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(output, "export interface TrackerError {{").unwrap();
    writeln!(output, "  /** Stable numeric error code */").unwrap();
    writeln!(output, "  code: ErrorCode;").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "  /** Human-readable error message */").unwrap();
    writeln!(output, "  message: string;").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "  /** Machine-readable context */").unwrap();
    writeln!(output, "  context: Record<string, unknown>;").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "  /** Error severity */").unwrap();
    writeln!(output, "  severity: ErrorSeverity;").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "  /** Source location (file:line) */").unwrap();
    writeln!(output, "  sourceLocation?: string;").unwrap();
    writeln!(output).unwrap();
    writeln!(output, "  /** Timestamp when error occurred */").unwrap();
    writeln!(output, "  timestampMs?: number;").unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();
}

fn generate_helper_types(output: &mut String) {
    writeln!(output, "/**").unwrap();
    writeln!(output, " * Error categories").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export type ErrorCategory = 'success' | 'parsing' | 'validation' | 'evaluation' | 'catalog' | 'planning' | 'storage' | 'unknown';"
    )
    .unwrap();
    writeln!(output).unwrap();
}

fn generate_utility_functions(output: &mut String) {
    writeln!(output, "/**").unwrap();
    writeln!(output, " * Get category for an error code").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export function getErrorCategory(code: ErrorCode): ErrorCategory {{"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code === ErrorCode.Success) return 'success';"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code >= 1000 && code <= 1099) return 'parsing';"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code >= 1100 && code <= 1199) return 'validation';"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code >= 1200 && code <= 1299) return 'evaluation';"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code >= 1300 && code <= 1399) return 'catalog';"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code >= 1400 && code <= 1499) return 'planning';"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code >= 1500 && code <= 1599) return 'storage';"
    )
    .unwrap();
    writeln!(output, "  return 'unknown';").unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();

    writeln!(output, "/**").unwrap();
    writeln!(output, " * Get severity for an error code").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export function getErrorSeverity(code: ErrorCode): ErrorSeverity {{"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code === ErrorCode.Success) return ErrorSeverity.Success;"
    )
    .unwrap();
    writeln!(
        output,
        "  if (code === ErrorCode.FieldNotFound || code === ErrorCode.CatalogEntryNotFound) {{"
    )
    .unwrap();
    writeln!(output, "    return ErrorSeverity.Warning;").unwrap();
    writeln!(output, "  }}").unwrap();
    writeln!(
        output,
        "  if (code === ErrorCode.DivisionByZero || code === ErrorCode.CircularDependency || code === ErrorCode.SqliteError || code === ErrorCode.FileIoError) {{"
    )
    .unwrap();
    writeln!(output, "    return ErrorSeverity.Fatal;").unwrap();
    writeln!(output, "  }}").unwrap();
    writeln!(output, "  return ErrorSeverity.Error;").unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();

    writeln!(output, "/**").unwrap();
    writeln!(output, " * Check if error code represents success").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export function isSuccess(code: ErrorCode): boolean {{"
    )
    .unwrap();
    writeln!(output, "  return code === ErrorCode.Success;").unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();

    writeln!(output, "/**").unwrap();
    writeln!(output, " * Check if error code represents an error").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export function isError(code: ErrorCode): boolean {{"
    )
    .unwrap();
    writeln!(output, "  return code !== ErrorCode.Success;").unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();

    writeln!(output, "/**").unwrap();
    writeln!(output, " * Format error for display").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export function formatError(error: TrackerError): string {{"
    )
    .unwrap();
    writeln!(
        output,
        "  let result = `[${{error.code}}] ${{error.message}}`;"
    )
    .unwrap();
    writeln!(output, "  if (error.sourceLocation) {{").unwrap();
    writeln!(output, "    result += ` at ${{error.sourceLocation}}`;").unwrap();
    writeln!(output, "  }}").unwrap();
    writeln!(output, "  return result;").unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();

    writeln!(output, "/**").unwrap();
    writeln!(output, " * Parse error from JSON string (from Rust FFI)").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export function parseError(json: string): TrackerError {{"
    )
    .unwrap();
    writeln!(output, "  const parsed = JSON.parse(json);").unwrap();
    writeln!(output, "  return {{").unwrap();
    writeln!(output, "    code: parsed.code as ErrorCode,").unwrap();
    writeln!(output, "    message: parsed.message,").unwrap();
    writeln!(output, "    context: parsed.context || {{}},").unwrap();
    writeln!(
        output,
        "    severity: (parsed.severity as ErrorSeverity) || getErrorSeverity(parsed.code),"
    )
    .unwrap();
    writeln!(output, "    sourceLocation: parsed.source_location,").unwrap();
    writeln!(output, "    timestampMs: parsed.timestamp_ms,").unwrap();
    writeln!(output, "  }};").unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();

    writeln!(output, "/**").unwrap();
    writeln!(
        output,
        " * Check if string looks like a structured error response (JSON)"
    )
    .unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export function isStructuredError(s: string): boolean {{"
    )
    .unwrap();
    writeln!(output, "  const trimmed = s.trimStart();").unwrap();
    writeln!(
        output,
        "  return trimmed.startsWith('{{') && trimmed.includes('code');"
    )
    .unwrap();
    writeln!(output, "}}").unwrap();
    writeln!(output).unwrap();

    writeln!(output, "/**").unwrap();
    writeln!(output, " * Parse structured error response").unwrap();
    writeln!(output, " */").unwrap();
    writeln!(
        output,
        "export function parseErrorResponse(s: string): TrackerError {{"
    )
    .unwrap();
    writeln!(output, "  if (!isStructuredError(s)) {{").unwrap();
    writeln!(
        output,
        "    throw new Error('Expected structured error JSON response');"
    )
    .unwrap();
    writeln!(output, "  }}").unwrap();
    writeln!(output, "  return parseError(s);").unwrap();
    writeln!(output, "}}").unwrap();
}
