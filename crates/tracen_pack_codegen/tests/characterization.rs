use std::io::Write;
use std::{fs, path::PathBuf};

fn fixture_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(file)
}

fn snapshot_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/snapshots")
        .join(file)
}

fn assert_snapshot(file: &str, actual: &str) {
    let path = snapshot_path(file);

    if std::env::var("TRACEN_UPDATE_SNAPSHOTS")
        .ok()
        .is_some_and(|value| value == "1")
    {
        let mut out = fs::File::create(&path).expect("write snapshot");
        out.write_all(actual.as_bytes())
            .expect("write snapshot bytes");
        return;
    }

    assert_eq!(fs::read_to_string(path).expect("snapshot file"), actual);
}

#[test]
fn codegen_snapshots() {
    let dsl = fs::read_to_string(fixture_path("workout_codegen.tracker")).expect("dsl fixture");
    let def = tracen_dsl::compile(&dsl).expect("compile fixture");
    let generator = tracen_pack_codegen::with_builtin_templates().expect("generator");
    let output = generator.generate_all(&def).expect("generate artifacts");

    assert_snapshot(
        "workout_codegen.rust_pack_runtime.rs",
        &output.rust_pack_runtime,
    );
    assert_snapshot("workout_codegenDslContract.ts", &output.ts_dsl_contract);
    assert_snapshot(
        "workout_codegenPackCoreDomainContract.ts",
        &output.ts_domain_contract,
    );
    assert_snapshot(
        "workout_codegenPackCoreApiContract.ts",
        &output.ts_api_contract,
    );
    assert_snapshot(
        "workout_codegenApiContract.ts",
        &output.ts_compat_api_contract,
    );
    assert_snapshot(
        "workout_codegenDomainContract.ts",
        &output.ts_compat_domain_contract,
    );
}

#[test]
fn compatibility_imports_are_unique_and_do_not_shadow_local_types() {
    let dsl = r#"
tracker "hydration" v1 {
  fields { amount: float }
  metrics { total = sum(amount) over all_time }
  types {
    type "AmountPoint" {
      contract = "api"
      fields = {"value":{"type":"float"}}
    }
  }
  views {
    view "custom" {
      config = {"result_kind":"metric_series","metrics":{"total":{"metric":"total"}},"group_by":{"amount":{"field":"amount"}}}
    }
    view "daily" {
      config = {"result_kind":"metric_series","metrics":{"total":{"metric":"total"}},"group_by":{"amount":{"field":"amount"}}}
    }
    view "weekly" {
      config = {"result_kind":"metric_series","metrics":{"total":{"metric":"total"}},"group_by":{"amount":{"field":"amount"}}}
    }
  }
  compat {
    view_aliases = {"custom":{"point_type":"AmountPoint"}}
  }
}
"#;
    let def = tracen_dsl::compile(dsl).unwrap();
    let output = tracen_pack_codegen::with_builtin_templates()
        .unwrap()
        .generate_all(&def)
        .unwrap();
    assert!(!output
        .ts_compat_api_contract
        .contains("import type { AmountPoint }"));
    assert_eq!(
        output
            .ts_compat_api_contract
            .matches("import type { PackMetricPoint }")
            .count(),
        1
    );
}

#[test]
fn optional_fields_describe_serde_null_values() {
    let def = tracen_dsl::compile(
        r#"
tracker "sleep" v1 {
  fields { duration: float }
  types {
    type "Reading" {
      contract = "domain"
      emit_rust = true
      fields = {"duration":{"type":"float","optional":true}}
    }
  }
  read_models {
    read_model "summary" {
      params = {"limit":{"type":"int","optional":true}}
      fields = {"reading":{"type":"Reading","optional":true}}
    }
  }
}
"#,
    )
    .unwrap();
    let output = tracen_pack_codegen::with_builtin_templates()
        .unwrap()
        .generate_all(&def)
        .unwrap();
    assert!(output
        .ts_domain_contract
        .contains("duration?: number | null;"));
    assert!(output
        .ts_compat_domain_contract
        .contains("duration?: number | null;"));
    assert!(output.ts_api_contract.contains("reading?: Reading | null;"));
    assert!(output.ts_api_contract.contains("limit?: number | null;"));
    assert!(output
        .rust_pack_runtime
        .contains("pub duration: Option<f64>"));
}

#[test]
fn minimal_domain_contracts_supply_default_identity_types() {
    let generator = tracen_pack_codegen::with_builtin_templates().unwrap();
    for (externs, supplies_defaults) in [
        ("", true),
        (
            r#"extern_ts { import "./identity" { names = {"EventId":{"rust":"String"},"TrackerId":{"rust":"String"},"BrandedString":{"rust":"String"}} } }"#,
            false,
        ),
    ] {
        let dsl = format!("tracker \"minimal\" v1 {{ fields {{ amount: float }} {externs} }}");
        let def = tracen_dsl::compile(&dsl).unwrap();
        let output = generator.generate_all(&def).unwrap();
        for contract in [output.ts_domain_contract, output.ts_compat_domain_contract] {
            for name in ["EventId", "TrackerId", "BrandedString"] {
                assert_eq!(
                    contract.contains(&format!("export type {name} = string;")),
                    supplies_defaults
                );
            }
        }
    }
}
