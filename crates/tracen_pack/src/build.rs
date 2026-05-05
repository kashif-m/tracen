use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use crate::{CompiledPack, PackError};

#[derive(Debug, Clone)]
pub struct PackBuildConfig {
    pub dsl_path: PathBuf,
    pub out_dir: PathBuf,
    pub generated_ts_dir: PathBuf,
    pub base_source_paths: BTreeMap<String, PathBuf>,
}

#[derive(Debug, Clone)]
pub struct PackBuildOutput {
    pub compiled: CompiledPack,
    pub rust_artifact_path: PathBuf,
    pub rust_ffi_glue_path: PathBuf,
    pub dsl_contract_path: PathBuf,
    pub api_contract_path: PathBuf,
    pub domain_contract_path: PathBuf,
    pub compat_api_contract_path: PathBuf,
    pub compat_domain_contract_path: PathBuf,
}

pub fn build(config: &PackBuildConfig) -> Result<PackBuildOutput, PackError> {
    let dsl = fs::read_to_string(&config.dsl_path)
        .map_err(|err| PackError::Io(format!("read {}: {err}", config.dsl_path.display())))?;
    let compiled = CompiledPack::compile(&dsl)?;
    let mut base_source_payloads = BTreeMap::new();
    for (name, path) in &config.base_source_paths {
        let payload = fs::read_to_string(path)
            .map_err(|err| PackError::Io(format!("read {}: {err}", path.display())))?;
        base_source_payloads.insert(name.clone(), payload);
    }
    let model = tracen_pack_codegen::PackGenModel::from_tracker_with_base_sources(
        compiled.definition(),
        &base_source_payloads,
    )
    .map_err(PackError::Compile)?;
    let generator = tracen_pack_codegen::with_builtin_templates()
        .map_err(|err| PackError::Compile(err.to_string()))?;
    let artifacts = generator
        .generate_all_from_model(&model)
        .map_err(|err| PackError::Compile(err.to_string()))?;

    fs::create_dir_all(&config.out_dir)
        .map_err(|err| PackError::Io(format!("create {}: {err}", config.out_dir.display())))?;
    fs::create_dir_all(&config.generated_ts_dir).map_err(|err| {
        PackError::Io(format!(
            "create {}: {err}",
            config.generated_ts_dir.display()
        ))
    })?;

    let rust_artifact_path = config
        .out_dir
        .join(format!("{}_tracker_compiled.rs", model.tracker_fn));
    let rust_ffi_glue_path = config
        .out_dir
        .join(format!("{}_tracker_ffi.rs", model.tracker_fn));
    let dsl_contract_path = config
        .generated_ts_dir
        .join(format!("{}DslContract.ts", model.tracker_fn));
    let api_contract_path = config
        .generated_ts_dir
        .join(format!("{}PackCoreApiContract.ts", model.tracker_fn));
    let domain_contract_path = config
        .generated_ts_dir
        .join(format!("{}PackCoreDomainContract.ts", model.tracker_fn));
    let compat_api_contract_path = config
        .generated_ts_dir
        .join(&model.compat_api_contract_file);
    let compat_domain_contract_path = config
        .generated_ts_dir
        .join(&model.compat_domain_contract_file);

    fs::write(&rust_artifact_path, artifacts.rust_pack_runtime)
        .map_err(|err| PackError::Io(format!("write {}: {err}", rust_artifact_path.display())))?;
    fs::write(&rust_ffi_glue_path, artifacts.rust_ffi_glue)
        .map_err(|err| PackError::Io(format!("write {}: {err}", rust_ffi_glue_path.display())))?;
    fs::write(&dsl_contract_path, artifacts.ts_dsl_contract)
        .map_err(|err| PackError::Io(format!("write {}: {err}", dsl_contract_path.display())))?;
    fs::write(&api_contract_path, artifacts.ts_api_contract)
        .map_err(|err| PackError::Io(format!("write {}: {err}", api_contract_path.display())))?;
    fs::write(&domain_contract_path, artifacts.ts_domain_contract)
        .map_err(|err| PackError::Io(format!("write {}: {err}", domain_contract_path.display())))?;
    fs::write(&compat_api_contract_path, artifacts.ts_compat_api_contract).map_err(|err| {
        PackError::Io(format!(
            "write {}: {err}",
            compat_api_contract_path.display()
        ))
    })?;
    fs::write(
        &compat_domain_contract_path,
        artifacts.ts_compat_domain_contract,
    )
    .map_err(|err| {
        PackError::Io(format!(
            "write {}: {err}",
            compat_domain_contract_path.display()
        ))
    })?;

    Ok(PackBuildOutput {
        compiled,
        rust_artifact_path,
        rust_ffi_glue_path,
        dsl_contract_path,
        api_contract_path,
        domain_contract_path,
        compat_api_contract_path,
        compat_domain_contract_path,
    })
}
