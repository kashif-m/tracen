use std::ffi::{CStr, CString};

#[test]
fn ffi_roundtrip_compiles_tracker() {
    let dsl = r#"
        tracker "sample" v1 {
          fields { value: float optional }
          metrics { total_value = sum(value) over all_time }
        }
    "#;

    let dsl_c = CString::new(dsl).expect("dsl must be a valid C string");
    let result = tracen_ffi::tracen_compile_tracker(dsl_c.as_ptr());

    assert!(result.success, "compile should succeed");
    assert!(
        !result.data.is_null(),
        "ffi payload pointer should be non-null"
    );

    let payload = unsafe {
        CStr::from_ptr(result.data)
            .to_str()
            .expect("ffi payload should be utf-8")
            .to_string()
    };
    unsafe { tracen_ffi::tracen_free_string(result.data) };

    let parsed: serde_json::Value =
        serde_json::from_str(&payload).expect("ffi payload should be valid json");
    let tracker_id = parsed["tracker_id"]
        .as_str()
        .expect("compiled payload should contain tracker_id string");
    assert!(
        tracker_id.starts_with("sample_v1_"),
        "tracker_id should include normalized name/version prefix with hash suffix"
    );
    assert!(parsed["dsl"].as_str().is_some());
}

#[test]
fn ffi_roundtrip_compute_smoke() {
    let dsl = r#"
        tracker "sample" v1 {
          fields { value: float optional }
          metrics { total_value = sum(value) over all_time }
        }
    "#;

    let dsl_c = CString::new(dsl).expect("dsl must be a valid C string");
    let events_c = CString::new("[]").expect("events must be valid C string");
    let query_c = CString::new("").expect("query must be valid C string");

    let result = tracen_ffi::tracen_compute(dsl_c.as_ptr(), events_c.as_ptr(), query_c.as_ptr());

    assert!(result.success, "compute should succeed");
    assert!(
        !result.data.is_null(),
        "ffi payload pointer should be non-null"
    );

    let payload = unsafe {
        CStr::from_ptr(result.data)
            .to_str()
            .expect("ffi payload should be utf-8")
            .to_string()
    };
    unsafe { tracen_ffi::tracen_free_string(result.data) };

    let parsed: serde_json::Value =
        serde_json::from_str(&payload).expect("ffi payload should be valid json");
    assert!(parsed.get("metrics").is_some());
}
