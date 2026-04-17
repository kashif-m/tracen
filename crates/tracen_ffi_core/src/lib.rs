use serde::Serialize;
use serde_json::json;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use tracen_engine::{self, EngineError};
use tracen_export::{export_generic_sqlite, import_generic_sqlite, GenericExportPayload};
use tracen_ir::error::{ErrorCode, TrackerError};
use tracen_ir::{NormalizedEvent, Query, TrackerDefinition};

#[repr(C)]
pub struct FfiResult {
    pub success: bool,
    pub data: *mut c_char,
}

/// Frees a C string previously returned by this library.
///
/// # Safety
/// `ptr` must be a valid pointer produced by `CString::into_raw` in this library and must not
/// be freed more than once.
pub unsafe fn tracen_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        // SAFETY: Caller guarantees `ptr` was returned by `CString::into_raw` from this library
        // and has not been freed yet.
        drop(unsafe { CString::from_raw(ptr) });
    }
}

pub fn tracen_compile_tracker(dsl_ptr: *const c_char) -> FfiResult {
    handle(|| {
        let dsl = cstr_to_str(dsl_ptr)?;
        let def = tracen_engine::compile_tracker(dsl).map_err(to_ffi_error)?;
        Ok(json!({
            "tracker_id": def.tracker_id().as_str(),
            "dsl": def.dsl(),
        }))
    })
}

pub fn tracen_validate_event(dsl_ptr: *const c_char, event_json_ptr: *const c_char) -> FfiResult {
    handle(|| {
        let dsl = cstr_to_str(dsl_ptr)?;
        let event_json = cstr_to_str(event_json_ptr)?;
        let def = tracen_engine::compile_tracker(dsl).map_err(to_ffi_error)?;
        let normalized = tracen_engine::validate_event(&def, event_json).map_err(to_ffi_error)?;
        Ok(normalized)
    })
}

pub fn tracen_compute(
    dsl_ptr: *const c_char,
    events_json_ptr: *const c_char,
    query_json_ptr: *const c_char,
) -> FfiResult {
    handle(|| {
        let def = compile_from_ptr(dsl_ptr)?;
        let events = parse_events(events_json_ptr)?;
        let query = parse_query(query_json_ptr)?;
        let output = tracen_engine::compute(&def, &events, query).map_err(to_ffi_error)?;
        Ok(output)
    })
}

pub fn tracen_simulate(
    dsl_ptr: *const c_char,
    base_events_ptr: *const c_char,
    hypotheticals_ptr: *const c_char,
    query_json_ptr: *const c_char,
) -> FfiResult {
    handle(|| {
        let def = compile_from_ptr(dsl_ptr)?;
        let base = parse_events(base_events_ptr)?;
        let hypothetical = parse_events(hypotheticals_ptr)?;
        let query = parse_query(query_json_ptr)?;
        let output =
            tracen_engine::simulate(&def, &base, &hypothetical, query).map_err(to_ffi_error)?;
        Ok(output)
    })
}

pub fn tracen_export_generic_sqlite(
    payload_json_ptr: *const c_char,
    output_path_ptr: *const c_char,
) -> FfiResult {
    handle(|| {
        let payload_json = cstr_to_str(payload_json_ptr)?;
        let payload: GenericExportPayload =
            serde_json::from_str(payload_json).map_err(|err| err.to_string())?;

        let output_path = if output_path_ptr.is_null() {
            None
        } else {
            Some(cstr_to_str(output_path_ptr)?)
        };

        let summary = export_generic_sqlite(&payload, output_path)?;
        Ok(summary)
    })
}

pub fn tracen_import_generic_sqlite(input_path_ptr: *const c_char) -> FfiResult {
    handle(|| {
        let input_path = cstr_to_str(input_path_ptr)?;
        let bundle = import_generic_sqlite(input_path)?;
        Ok(bundle)
    })
}

pub fn compile_from_ptr(ptr: *const c_char) -> Result<TrackerDefinition, String> {
    let dsl = cstr_to_str(ptr)?;
    tracen_engine::compile_tracker(dsl).map_err(to_ffi_error)
}

pub fn parse_events(ptr: *const c_char) -> Result<Vec<NormalizedEvent>, String> {
    let events_json = cstr_to_str(ptr)?;
    serde_json::from_str(events_json).map_err(|err| err.to_string())
}

pub fn parse_query(ptr: *const c_char) -> Result<Query, String> {
    if ptr.is_null() {
        Ok(Query::default())
    } else {
        let json = cstr_to_str(ptr)?;
        if json.trim().is_empty() {
            Ok(Query::default())
        } else {
            serde_json::from_str(json).map_err(|err| err.to_string())
        }
    }
}

fn cstr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, String> {
    if ptr.is_null() {
        Err("null pointer".into())
    } else {
        unsafe { CStr::from_ptr(ptr) }
            .to_str()
            .map_err(|err| err.to_string())
    }
}

pub fn handle<T>(op: impl FnOnce() -> Result<T, String>) -> FfiResult
where
    T: Serialize,
{
    match op() {
        Ok(value) => success(value),
        Err(err) => error(err),
    }
}

fn success<T: Serialize>(value: T) -> FfiResult {
    match serde_json::to_string(&value) {
        Ok(json_string) => FfiResult {
            success: true,
            data: string_to_c(json_string),
        },
        Err(err) => error(err.to_string()),
    }
}

fn error(message: String) -> FfiResult {
    let payload = if is_structured_error(&message) {
        message
    } else {
        TrackerError::new_simple(ErrorCode::Unknown, message).to_json()
    };
    FfiResult {
        success: false,
        data: string_to_c(payload),
    }
}

fn string_to_c(value: String) -> *mut c_char {
    match CString::new(value) {
        Ok(cstring) => cstring.into_raw(),
        Err(_) => CString::new("string contains null bytes")
            .unwrap()
            .into_raw(),
    }
}

/// Convert error to FFI response string
fn to_ffi_error(error: EngineError) -> String {
    engine_error_to_tracker_error(&error).to_json()
}

fn engine_error_to_tracker_error(error: &EngineError) -> TrackerError {
    match error {
        EngineError::DslParse(msg) => {
            TrackerError::new_simple(ErrorCode::DslParseError, format!("DSL parse error: {msg}"))
                .with_context(serde_json::json!({"original": msg}))
        }
        EngineError::EventValidation(msg) => TrackerError::new_simple(
            ErrorCode::EventValidationFailed,
            format!("event validation error: {msg}"),
        )
        .with_context(serde_json::json!({"original": msg})),
        EngineError::TrackerMismatch { expected, actual } => TrackerError::new_simple(
            ErrorCode::TrackerMismatch,
            format!("tracker mismatch (expected {expected}, found {actual})"),
        )
        .with_context(serde_json::json!({
            "expected": expected.as_str(),
            "actual": actual.as_str()
        })),
        EngineError::StateMismatch { expected, actual } => TrackerError::new_simple(
            ErrorCode::StateMismatch,
            format!("state tracker mismatch (expected {expected}, found {actual})"),
        )
        .with_context(serde_json::json!({
            "expected": expected.as_str(),
            "actual": actual.as_str()
        })),
        EngineError::Evaluation(msg) => TrackerError::new_simple(
            ErrorCode::MetricEvaluationFailed,
            format!("evaluation error: {msg}"),
        )
        .with_context(serde_json::json!({"original": msg})),
    }
}

pub fn to_engine_error_string(error: EngineError) -> String {
    engine_error_to_tracker_error(&error).to_json()
}

/// Check if a response is a structured error (JSON with code field)
pub fn is_structured_error(response: &str) -> bool {
    response.trim_start().starts_with('{') && response.contains("\"code\"")
}

/// Parse FFI response to check if it's an error
pub fn parse_ffi_response(response: &str) -> Result<serde_json::Value, TrackerError> {
    if is_structured_error(response) {
        match TrackerError::from_json(response) {
            Ok(err) if err.code != ErrorCode::Success => Err(err),
            Ok(_) => serde_json::from_str(response).map_err(|e| {
                TrackerError::new_simple(
                    ErrorCode::DeserializationFailed,
                    format!("Failed to parse response: {}", e),
                )
            }),
            Err(_) => serde_json::from_str(response).map_err(|e| {
                TrackerError::new_simple(
                    ErrorCode::DeserializationFailed,
                    format!("Failed to parse response: {}", e),
                )
            }),
        }
    } else {
        serde_json::from_str(response).map_err(|_| {
            TrackerError::new_simple(
                ErrorCode::DeserializationFailed,
                format!("Invalid JSON response: {}", response),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_errors_serialize_as_structured_payloads() {
        let payload = to_engine_error_string(EngineError::DslParse("bad tracker".to_string()));
        assert!(is_structured_error(&payload));
        let parsed = TrackerError::from_json(&payload).expect("structured TrackerError payload");
        assert_eq!(parsed.code, ErrorCode::DslParseError);
    }

    #[test]
    fn parse_ffi_response_returns_tracker_error() {
        let err_json =
            TrackerError::new_simple(ErrorCode::MetricEvaluationFailed, "bad metric").to_json();
        let err = parse_ffi_response(&err_json).expect_err("should map to TrackerError");
        assert_eq!(err.code, ErrorCode::MetricEvaluationFailed);
    }

    #[test]
    fn parse_query_rejects_invalid_json() {
        let bad_json = CString::new("{bad json").expect("cstring");
        let err = parse_query(bad_json.as_ptr()).expect_err("invalid query should be rejected");
        assert!(err.contains("key must be a string") || err.contains("expected"));
    }
}
