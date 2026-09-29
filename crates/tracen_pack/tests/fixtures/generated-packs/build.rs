use std::{collections::BTreeMap, env, path::PathBuf};
fn main() {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    let ts = env::var_os("TRACEN_CONFORMANCE_TS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| out.join("ts"));
    println!("cargo:rerun-if-env-changed=TRACEN_CONFORMANCE_TS_DIR");
    for name in ["hydration", "sleep"] {
        let dsl_path = root.join(format!("../{name}.tracker"));
        println!("cargo:rerun-if-changed={}", dsl_path.display());
        tracen_pack::build(&tracen_pack::PackBuildConfig {
            dsl_path,
            out_dir: out.clone(),
            generated_ts_dir: ts.clone(),
            base_source_paths: BTreeMap::new(),
        })
        .unwrap();
    }
    std::fs::write(ts.join("enum-wire-check.ts"), include_str!("enum-wire-check.ts")).unwrap();
}
