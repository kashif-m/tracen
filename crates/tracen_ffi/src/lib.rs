//! C ABI wrappers for generic Tracen core APIs.

use std::os::raw::c_char;

pub use tracen_ffi_core::FfiResult;

/// Frees an allocated C string returned by Tracen FFI calls.
#[no_mangle]
///
/// # Safety
/// `ptr` must be a valid pointer produced by Tracen FFI and must not be freed more than once.
pub unsafe extern "C" fn tracen_free_string(ptr: *mut c_char) {
    // SAFETY: Guaranteed by this function's contract.
    unsafe { tracen_ffi_core::tracen_free_string(ptr) }
}

/// Compiles tracker DSL and returns a JSON payload with tracker metadata.
#[no_mangle]
pub extern "C" fn tracen_compile_tracker(dsl_ptr: *const c_char) -> FfiResult {
    tracen_ffi_core::tracen_compile_tracker(dsl_ptr)
}

/// Validates one event JSON object against a DSL definition.
#[no_mangle]
pub extern "C" fn tracen_validate_event(
    dsl_ptr: *const c_char,
    event_json_ptr: *const c_char,
) -> FfiResult {
    tracen_ffi_core::tracen_validate_event(dsl_ptr, event_json_ptr)
}

/// Computes engine output for a DSL + event list + query.
#[no_mangle]
pub extern "C" fn tracen_compute(
    dsl_ptr: *const c_char,
    events_json_ptr: *const c_char,
    query_json_ptr: *const c_char,
) -> FfiResult {
    tracen_ffi_core::tracen_compute(dsl_ptr, events_json_ptr, query_json_ptr)
}

/// Runs hypothetical simulation against a base event list.
#[no_mangle]
pub extern "C" fn tracen_simulate(
    dsl_ptr: *const c_char,
    base_events_ptr: *const c_char,
    hypotheticals_ptr: *const c_char,
    query_json_ptr: *const c_char,
) -> FfiResult {
    tracen_ffi_core::tracen_simulate(dsl_ptr, base_events_ptr, hypotheticals_ptr, query_json_ptr)
}

/// Exports normalized events into generic SQLite format.
#[no_mangle]
pub extern "C" fn tracen_export_generic_sqlite(
    payload_json_ptr: *const c_char,
    output_path_ptr: *const c_char,
) -> FfiResult {
    tracen_ffi_core::tracen_export_generic_sqlite(payload_json_ptr, output_path_ptr)
}

/// Imports normalized events from generic SQLite format.
#[no_mangle]
pub extern "C" fn tracen_import_generic_sqlite(input_path_ptr: *const c_char) -> FfiResult {
    tracen_ffi_core::tracen_import_generic_sqlite(input_path_ptr)
}
