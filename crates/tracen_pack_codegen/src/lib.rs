//! Pack code generation using Handlebars templates for Rust and TypeScript.

use handlebars::{Handlebars, RenderError, TemplateError};
use serde::Serialize;
use tracen_ir::TrackerDefinition;

pub mod model;
pub mod naming;

pub use model::PackGenModel;

#[derive(Debug, thiserror::Error)]
pub enum CodegenError {
    #[error("template error: {0}")]
    Template(#[from] TemplateError),
    #[error("render error: {0}")]
    Render(#[from] RenderError),
    #[error("model building error: {0}")]
    Model(String),
}

pub type CodegenResult<T> = Result<T, CodegenError>;

/// Pack code generator using Handlebars templates.
pub struct PackGenerator {
    engine: Handlebars<'static>,
}

impl Default for PackGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl PackGenerator {
    /// Create a new generator with strict mode enabled.
    pub fn new() -> Self {
        let mut engine = Handlebars::new();
        engine.set_strict_mode(true);
        Self { engine }
    }

    /// Register a template from a string.
    pub fn register_template_string(&mut self, name: &str, template: &str) -> CodegenResult<()> {
        self.engine.register_template_string(name, template)?;
        Ok(())
    }

    /// Register a template from a file.
    pub fn register_template_file(&mut self, name: &str, path: &str) -> CodegenResult<()> {
        self.engine.register_template_file(name, path)?;
        Ok(())
    }

    /// Render a template with the given model.
    pub fn render<T: Serialize>(&self, template_name: &str, data: &T) -> CodegenResult<String> {
        self.engine
            .render(template_name, data)
            .map_err(CodegenError::from)
    }

    /// Generate all pack artifacts from a tracker definition.
    pub fn generate_all(&self, def: &TrackerDefinition) -> CodegenResult<GeneratedArtifacts> {
        let model = PackGenModel::from_tracker(def).map_err(CodegenError::Model)?;
        self.generate_all_from_model(&model)
    }

    pub fn generate_all_from_model(
        &self,
        model: &PackGenModel,
    ) -> CodegenResult<GeneratedArtifacts> {
        let rust_pack_runtime = self.render("rust_pack_runtime", model)?;
        let rust_ffi_glue = self.render("rust_ffi_glue", model)?;
        let ts_dsl_contract = self.render("ts_dsl_contract", model)?;
        let ts_api_contract = self.render("ts_api_contract", model)?;
        let ts_domain_contract = self.render("ts_domain_contract", model)?;
        let ts_compat_api_contract = self.render("ts_compat_api_contract", model)?;
        let ts_compat_domain_contract = self.render("ts_compat_domain_contract", model)?;

        Ok(GeneratedArtifacts {
            rust_pack_runtime,
            rust_ffi_glue,
            ts_dsl_contract,
            ts_api_contract,
            ts_domain_contract,
            ts_compat_api_contract,
            ts_compat_domain_contract,
        })
    }
}

/// Generated artifacts from pack code generation.
#[derive(Debug, Clone)]
pub struct GeneratedArtifacts {
    pub rust_pack_runtime: String,
    pub rust_ffi_glue: String,
    pub ts_dsl_contract: String,
    pub ts_api_contract: String,
    pub ts_domain_contract: String,
    pub ts_compat_api_contract: String,
    pub ts_compat_domain_contract: String,
}

/// Load built-in templates into a generator.
pub fn with_builtin_templates() -> CodegenResult<PackGenerator> {
    let mut gen = PackGenerator::new();

    gen.register_template_string("rust_pack_runtime", RUST_PACK_RUNTIME_TEMPLATE)?;
    gen.register_template_string("rust_ffi_glue", RUST_FFI_GLUE_TEMPLATE)?;
    gen.register_template_string("ts_dsl_contract", TS_DSL_CONTRACT_TEMPLATE)?;
    gen.register_template_string("ts_api_contract", TS_API_CONTRACT_TEMPLATE)?;
    gen.register_template_string("ts_domain_contract", TS_DOMAIN_CONTRACT_TEMPLATE)?;
    gen.register_template_string("ts_compat_api_contract", TS_COMPAT_API_CONTRACT_TEMPLATE)?;
    gen.register_template_string(
        "ts_compat_domain_contract",
        TS_COMPAT_DOMAIN_CONTRACT_TEMPLATE,
    )?;

    Ok(gen)
}

const RUST_PACK_RUNTIME_TEMPLATE: &str = include_str!("../templates/rust_pack_runtime.txt");

const RUST_FFI_GLUE_TEMPLATE: &str = include_str!("../templates/rust_ffi_glue.txt");

const TS_DSL_CONTRACT_TEMPLATE: &str = include_str!("../templates/ts_dsl_contract.txt");

const TS_API_CONTRACT_TEMPLATE: &str = include_str!("../templates/ts_api_contract.txt");

const TS_DOMAIN_CONTRACT_TEMPLATE: &str = include_str!("../templates/ts_domain_contract.txt");

const TS_COMPAT_API_CONTRACT_TEMPLATE: &str =
    include_str!("../templates/ts_compat_api_contract.txt");

const TS_COMPAT_DOMAIN_CONTRACT_TEMPLATE: &str =
    include_str!("../templates/ts_compat_domain_contract.txt");
