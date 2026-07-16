//! Assistant configuration and the model-registry builder the daemon mounts.
//!
//! An `assistant:` config section lists zero or more OpenAI-compatible providers.
//! Each becomes a named [`SequinsAssistantModel`] entry in the registry served by
//! [`crate::serve::completion_model_router`]. An empty `models` list is valid: the
//! `/v1/models` endpoint then lists nothing and clients bring their own model,
//! driving the tools over MCP instead.
//!
//! API keys are read from the environment (never stored in config).

use indexmap::IndexMap;
use rig::client::{CompletionClient, ProviderClient};
use rig::providers::openai;
use serde::Deserialize;

use crate::model::SequinsAssistantModel;
use crate::tools::Tools;

/// The backing model each registry entry wraps — an OpenAI Chat Completions model
/// (compatible with OpenAI, OpenRouter, vLLM, Ollama, … via `base_url`).
pub type BackingModel = openai::completion::CompletionModel<reqwest::Client>;

/// The named registry of assistant models served over `/v1`.
pub type ModelRegistry = IndexMap<String, SequinsAssistantModel<BackingModel>>;

/// Default environment variable holding an explicit provider's API key.
pub const DEFAULT_API_KEY_ENV: &str = "SEQUINS_ASSISTANT_API_KEY";

/// Env var Rig's OpenAI provider reads for `from_env` auto-config (the key's
/// presence is what enables the auto `default` model). `OPENAI_BASE_URL` (also read
/// by Rig) points it at any OpenAI-compatible endpoint.
const OPENAI_API_KEY_ENV: &str = "OPENAI_API_KEY";
/// Id of the model auto-registered from the environment.
const DEFAULT_MODEL_ID: &str = "default";

/// The `assistant:` config section.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssistantConfig {
    /// Configured providers/models. May be empty (then only MCP is useful).
    #[serde(default)]
    pub models: Vec<ModelConfig>,
    /// Optional override for the SeQL system grounding.
    #[serde(default)]
    pub grounding: Option<String>,
    /// Auto-register a `default` model from the environment (Rig's `from_env`:
    /// `OPENAI_API_KEY` + optional `OPENAI_BASE_URL`; model name from `OPENAI_MODEL`,
    /// else `gpt-4o`) when the key is present. Explicit `models` — including one with
    /// id `default` — take precedence. Defaults to `true`.
    #[serde(default = "default_true")]
    pub auto_from_env: bool,
}

impl Default for AssistantConfig {
    fn default() -> Self {
        Self {
            models: Vec::new(),
            grounding: None,
            auto_from_env: true,
        }
    }
}

fn default_true() -> bool {
    true
}

/// The backing model name for the env-auto-configured `default` model.
fn default_model_name() -> String {
    std::env::var("OPENAI_MODEL")
        .ok()
        .or_else(|| std::env::var("SEQUINS_ASSISTANT_MODEL").ok())
        .unwrap_or_else(|| "gpt-4o".to_string())
}

/// A single OpenAI-compatible provider entry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    /// Public model id exposed via `/v1/models` and selected by the client.
    pub id: String,
    /// OpenAI-compatible base URL. Omit for api.openai.com.
    #[serde(default)]
    pub base_url: Option<String>,
    /// Backing model name to request from the provider (e.g. `gpt-4o`).
    pub model: String,
    /// Environment variable holding the API key (default `SEQUINS_ASSISTANT_API_KEY`).
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// Optional sampling temperature (reserved for future per-model defaults).
    #[serde(default)]
    pub temperature: Option<f64>,
}

/// Errors building the model registry.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("model '{id}': API key env var '{env}' is not set")]
    MissingApiKey { id: String, env: String },
    #[error("model '{id}': failed to build provider client: {reason}")]
    Client { id: String, reason: String },
}

/// Build a single OpenAI-compatible backing model from explicit connection
/// parameters (used by the FFI's local assistant, where the config comes from the
/// app's connection profile). `base_url` omitted ⇒ api.openai.com.
pub fn build_backing_model(
    base_url: Option<&str>,
    model: &str,
    api_key: &str,
) -> Result<BackingModel, ConfigError> {
    let mut builder = openai::Client::builder().api_key(api_key);
    if let Some(base) = base_url {
        builder = builder.base_url(base);
    }
    let client = builder.build().map_err(|e| ConfigError::Client {
        id: model.to_string(),
        reason: e.to_string(),
    })?;
    Ok(client.completions_api().completion_model(model))
}

/// Build the named model registry from the config, wrapping each configured
/// provider in a [`SequinsAssistantModel`] over the shared tool set.
pub fn build_registry(
    config: &AssistantConfig,
    tools: Tools,
) -> Result<ModelRegistry, ConfigError> {
    let mut registry = ModelRegistry::new();

    // Explicit config entries first, so they win over any env auto-config.
    for m in &config.models {
        let env = m
            .api_key_env
            .clone()
            .unwrap_or_else(|| DEFAULT_API_KEY_ENV.to_string());
        let api_key = std::env::var(&env).map_err(|_| ConfigError::MissingApiKey {
            id: m.id.clone(),
            env: env.clone(),
        })?;

        let mut builder = openai::Client::builder().api_key(&api_key);
        if let Some(base) = &m.base_url {
            builder = builder.base_url(base);
        }
        let client = builder.build().map_err(|e| ConfigError::Client {
            id: m.id.clone(),
            reason: e.to_string(),
        })?;
        let backing = client.completions_api().completion_model(&m.model);
        tracing::info!(
            id = %m.id,
            base_url = %m.base_url.as_deref().unwrap_or("https://api.openai.com/v1 (default)"),
            backing_model = %m.model,
            api_key_env = %env,
            "assistant model registered"
        );
        registry.insert(m.id.clone(), wrap(backing, config, &tools));
    }

    // Env auto-config: register a `default` model from `OPENAI_API_KEY` (+ optional
    // `OPENAI_BASE_URL`) via Rig's `from_env`, unless config already defined `default`.
    if config.auto_from_env
        && std::env::var(OPENAI_API_KEY_ENV).is_ok()
        && !registry.contains_key(DEFAULT_MODEL_ID)
    {
        let client = openai::Client::from_env().map_err(|e| ConfigError::Client {
            id: DEFAULT_MODEL_ID.to_string(),
            reason: e.to_string(),
        })?;
        let backing_model = default_model_name();
        tracing::info!(
            id = %DEFAULT_MODEL_ID,
            base_url = %std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| "https://api.openai.com/v1 (default)".to_string()),
            backing_model = %backing_model,
            api_key_env = %OPENAI_API_KEY_ENV,
            "assistant model registered (env auto-config)"
        );
        let backing = client.completions_api().completion_model(backing_model);
        registry.insert(DEFAULT_MODEL_ID.to_string(), wrap(backing, config, &tools));
    }

    Ok(registry)
}

/// Wrap a backing model in the middleware, applying the config's grounding override.
fn wrap(
    backing: BackingModel,
    config: &AssistantConfig,
    tools: &Tools,
) -> SequinsAssistantModel<BackingModel> {
    let mut model = SequinsAssistantModel::new(backing, tools.clone());
    if let Some(grounding) = &config.grounding {
        model = model.with_grounding(grounding.clone());
    }
    model
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_datafusion_backend::DataFusionBackend;
    use sequins_storage::test_fixtures::TestStorageBuilder;
    use std::sync::Arc;

    async fn tools() -> (Tools, tempfile::TempDir) {
        let (storage, temp) = TestStorageBuilder::new().build().await;
        let backend = Arc::new(DataFusionBackend::new(Arc::new(storage)));
        (Tools::new(backend), temp)
    }

    #[tokio::test]
    async fn empty_config_yields_empty_registry() {
        let (tools, _t) = tools().await;
        // Disable env auto-config so the result doesn't depend on the ambient
        // `OPENAI_API_KEY` (which may be set in the developer/CI environment).
        let config = AssistantConfig {
            auto_from_env: false,
            ..Default::default()
        };
        let registry = build_registry(&config, tools).unwrap();
        assert!(registry.is_empty());
    }

    #[tokio::test]
    async fn missing_api_key_is_reported() {
        let (tools, _t) = tools().await;
        let config = AssistantConfig {
            models: vec![ModelConfig {
                id: "x".into(),
                base_url: None,
                model: "gpt-4o".into(),
                api_key_env: Some("SEQUINS_ASSISTANT_KEY_DEFINITELY_UNSET".into()),
                temperature: None,
            }],
            grounding: None,
            auto_from_env: false,
        };
        let result = build_registry(&config, tools);
        assert!(matches!(result, Err(ConfigError::MissingApiKey { .. })));
    }

    #[test]
    fn parses_yaml() {
        let yaml = r#"
models:
  - id: default
    base_url: http://localhost:11434/v1
    model: llama3.1
"#;
        let cfg: AssistantConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.models.len(), 1);
        assert_eq!(cfg.models[0].id, "default");
        assert_eq!(
            cfg.models[0].base_url.as_deref(),
            Some("http://localhost:11434/v1")
        );
        // Missing field defaults to enabled.
        assert!(cfg.auto_from_env);
    }

    #[tokio::test]
    async fn env_autoconfig_registers_default_model() {
        let (tools, _t) = tools().await;
        // This test mutates a process-global env var; it is the only test that
        // reads `OPENAI_API_KEY` (the others set `auto_from_env: false`).
        std::env::set_var(OPENAI_API_KEY_ENV, "test-key");
        let registry = build_registry(&AssistantConfig::default(), tools).unwrap();
        std::env::remove_var(OPENAI_API_KEY_ENV);
        assert!(registry.contains_key(DEFAULT_MODEL_ID));
    }
}
