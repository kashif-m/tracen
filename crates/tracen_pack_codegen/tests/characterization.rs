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
