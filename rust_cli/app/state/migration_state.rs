/// Migration state management - load and save `migration_state.json`.
///
/// The state file format is backwards-compatible with the Python tool.
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const STATE_FILE: &str = "migration_state.json";

/// Top-level migration state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationState {
    pub migrations: Vec<Migration>,
}

/// A single migration job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    pub id: String,
    pub source_table: String,
    pub target_table: String,
    pub column_mappings: HashMap<String, Value>,
    pub last_evaluated_key: Option<HashMap<String, Value>>,
    pub processed_items: u64,
    pub status: MigrationStatus,
    /// Key schema is populated after the first run.
    #[serde(default)]
    pub key_schema: Option<HashMap<String, String>>,
}

/// Migration status values.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MigrationStatus {
    InProgress,
    Completed,
    Error,
    Undone,
}

impl std::fmt::Display for MigrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InProgress => write!(f, "in_progress"),
            Self::Completed => write!(f, "completed"),
            Self::Error => write!(f, "error"),
            Self::Undone => write!(f, "undone"),
        }
    }
}

impl MigrationState {
    /// Create a new empty state.
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }
}

impl Default for MigrationState {
    fn default() -> Self {
        Self::new()
    }
}

/// Load migration state from `state_dir/migration_state.json`.
///
/// Returns a fresh empty state if the file doesn't exist.
pub fn load_state(state_dir: &Path) -> Result<MigrationState> {
    let path = state_dir.join(STATE_FILE);

    if !path.exists() {
        return Ok(MigrationState::new());
    }

    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read state file: {}", path.display()))?;

    serde_json::from_str(&content)
        .with_context(|| format!("State file is corrupted: {}", path.display()))
}

/// Save migration state to `state_dir/migration_state.json`.
///
/// Uses atomic write (temp file + rename) to prevent corruption.
pub fn save_state(state_dir: &Path, state: &MigrationState) -> Result<()> {
    let path = state_dir.join(STATE_FILE);
    let tmp_path = state_dir.join(".migration_state.json.tmp");

    let content =
        serde_json::to_string_pretty(state).context("Failed to serialize migration state")?;

    fs::write(&tmp_path, &content)
        .with_context(|| format!("Failed to write temp state file: {}", tmp_path.display()))?;

    fs::rename(&tmp_path, &path)
        .with_context(|| format!("Failed to rename temp state file to: {}", path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_load_missing_file() {
        let dir = TempDir::new().unwrap();
        let state = load_state(dir.path()).unwrap();
        assert!(state.migrations.is_empty());
    }

    #[test]
    fn test_save_and_load_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut state = MigrationState::new();
        let mut mappings = HashMap::new();
        mappings.insert("id".to_string(), Value::String("{id upper}".to_string()));

        state.migrations.push(Migration {
            id: "migration_test_001".to_string(),
            source_table: "source".to_string(),
            target_table: "target".to_string(),
            column_mappings: mappings,
            last_evaluated_key: None,
            processed_items: 42,
            status: MigrationStatus::InProgress,
            key_schema: None,
        });

        save_state(dir.path(), &state).unwrap();
        let loaded = load_state(dir.path()).unwrap();

        assert_eq!(loaded.migrations.len(), 1);
        assert_eq!(loaded.migrations[0].id, "migration_test_001");
        assert_eq!(loaded.migrations[0].processed_items, 42);
    }
}
