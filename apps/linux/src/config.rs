//! App configuration — XDG paths, storage config, environment profiles.

use anyhow::{Context, Result};
use sequins_storage::StorageConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

// ── XDG paths ─────────────────────────────────────────────────────────────────

/// Resolve the app's data directory.
///
/// Follows XDG Base Directory spec:
/// `$XDG_DATA_HOME/sequins` (default `$HOME/.local/share/sequins`).
pub fn data_dir() -> Result<PathBuf> {
    let base = std::env::var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::var("HOME")
                .map(|h| PathBuf::from(h).join(".local/share"))
                .unwrap_or_else(|_| PathBuf::from("/tmp"))
        });

    let dir = base.join("sequins");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create data dir: {}", dir.display()))?;
    Ok(dir)
}

/// Resolve the app's config directory.
///
/// `$XDG_CONFIG_HOME/sequins` (default `$HOME/.config/sequins`).
pub fn config_dir() -> Result<PathBuf> {
    let base = std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            std::env::var("HOME")
                .map(|h| PathBuf::from(h).join(".config"))
                .unwrap_or_else(|_| PathBuf::from("/tmp"))
        });

    let dir = base.join("sequins");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create config dir: {}", dir.display()))?;
    Ok(dir)
}

/// Build a `StorageConfig` pointing at the app's XDG data directory.
pub fn default_storage_config() -> Result<StorageConfig> {
    let dir = data_dir()?;
    let uri = format!("file://{}", dir.display());
    let mut config = StorageConfig::default();
    config.cold_tier.uri = uri;
    Ok(config)
}

// ── Environment profiles ──────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub id: Uuid,
    pub name: String,
    /// Whether this is the built-in local profile (cannot be deleted).
    pub is_default: bool,
    #[serde(flatten)]
    pub kind: EnvironmentKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum EnvironmentKind {
    Local { grpc_port: u16, http_port: u16 },
    Remote { query_url: String },
}

impl EnvironmentKind {
    /// Short subtitle shown in the profile list (e.g. `:4317` or `host.example.com`).
    pub fn subtitle(&self) -> String {
        match self {
            Self::Local { grpc_port, .. } => format!(":{grpc_port}"),
            Self::Remote { query_url } => {
                // Strip scheme for display
                query_url
                    .trim_start_matches("http://")
                    .trim_start_matches("https://")
                    .to_string()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentStore {
    pub selected_id: Uuid,
    pub environments: Vec<Environment>,
}

impl EnvironmentStore {
    const FILE_NAME: &'static str = "environments.json";

    /// Load from `config_dir()/environments.json`, creating the default if absent.
    pub fn load() -> Result<Self> {
        let path = config_dir()?.join(Self::FILE_NAME);
        if path.exists() {
            let data = std::fs::read_to_string(&path)
                .with_context(|| format!("Failed to read {}", path.display()))?;
            serde_json::from_str(&data)
                .with_context(|| format!("Failed to parse {}", path.display()))
        } else {
            let store = Self::default();
            store.save()?;
            Ok(store)
        }
    }

    /// Write to disk atomically via a tmp file + rename.
    pub fn save(&self) -> Result<()> {
        let dir = config_dir()?;
        let path = dir.join(Self::FILE_NAME);
        let tmp = dir.join(format!(".{}.tmp", Self::FILE_NAME));
        let data =
            serde_json::to_string_pretty(self).context("Failed to serialize environments")?;
        std::fs::write(&tmp, &data)
            .with_context(|| format!("Failed to write {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("Failed to rename {} → {}", tmp.display(), path.display()))?;
        Ok(())
    }

    /// Return the currently selected environment.
    pub fn selected(&self) -> &Environment {
        self.environments
            .iter()
            .find(|e| e.id == self.selected_id)
            .unwrap_or_else(|| &self.environments[0])
    }

    /// Add a new remote environment and save. Returns the new environment's id.
    pub fn add_remote(&mut self, name: String, query_url: String) -> Result<Uuid> {
        let id = Uuid::new_v4();
        self.environments.push(Environment {
            id,
            name,
            is_default: false,
            kind: EnvironmentKind::Remote { query_url },
        });
        self.save()?;
        Ok(id)
    }

    /// Remove an environment by id. Fails if it is the default.
    /// If the removed environment was selected, the default is auto-selected.
    pub fn remove(&mut self, id: Uuid) -> Result<()> {
        let env = self
            .environments
            .iter()
            .find(|e| e.id == id)
            .context("Environment not found")?;
        if env.is_default {
            anyhow::bail!("Cannot remove the default environment");
        }
        self.environments.retain(|e| e.id != id);
        if self.selected_id == id {
            // Fall back to the default
            if let Some(default) = self.environments.iter().find(|e| e.is_default) {
                self.selected_id = default.id;
            }
        }
        self.save()
    }

    /// Change the selected environment and save.
    pub fn select(&mut self, id: Uuid) -> Result<()> {
        if !self.environments.iter().any(|e| e.id == id) {
            anyhow::bail!("Environment not found");
        }
        self.selected_id = id;
        self.save()
    }
}

impl Default for EnvironmentStore {
    fn default() -> Self {
        let local_id = Uuid::new_v4();
        Self {
            selected_id: local_id,
            environments: vec![Environment {
                id: local_id,
                name: "Development".to_string(),
                is_default: true,
                kind: EnvironmentKind::Local {
                    grpc_port: 4317,
                    http_port: 4318,
                },
            }],
        }
    }
}
