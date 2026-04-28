/// Undo state management - load and save `undo_state.json`.
///
/// Stores the keys of items written during migration so they can be
/// batch-deleted for rollback.
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const UNDO_FILE: &str = "undo_state.json";

/// Top-level undo state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoState {
    pub undo_migrations: HashMap<String, UndoMigration>,
}

/// Undo data for a single migration - a list of keys to delete.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UndoMigration {
    #[serde(rename = "DeleteRequest")]
    pub delete_request: DeleteRequest,
}

/// The delete request containing all keys to remove.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    #[serde(rename = "Key")]
    pub keys: Vec<HashMap<String, Value>>,
}

impl UndoState {
    /// Create a new empty undo state.
    pub fn new() -> Self {
        Self {
            undo_migrations: HashMap::new(),
        }
    }

    /// Get or create the undo entry for a migration.
    pub fn ensure_migration(&mut self, migration_id: &str) -> &mut UndoMigration {
        self.undo_migrations
            .entry(migration_id.to_string())
            .or_insert_with(|| UndoMigration {
                delete_request: DeleteRequest { keys: Vec::new() },
            })
    }
}

impl Default for UndoState {
    fn default() -> Self {
        Self::new()
    }
}

/// Load undo state from `state_dir/undo_state.json`.
///
/// Returns a fresh empty state if the file doesn't exist.
pub fn load_undo_state(state_dir: &Path) -> Result<UndoState> {
    let path = state_dir.join(UNDO_FILE);

    if !path.exists() {
        return Ok(UndoState::new());
    }

    let content = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read undo file: {}", path.display()))?;

    serde_json::from_str(&content)
        .with_context(|| format!("Undo file is corrupted: {}", path.display()))
}

/// Save undo state to `state_dir/undo_state.json`.
///
/// Uses atomic write (temp file + rename) to prevent corruption.
pub fn save_undo_state(state_dir: &Path, state: &UndoState) -> Result<()> {
    let path = state_dir.join(UNDO_FILE);
    let tmp_path = state_dir.join(".undo_state.json.tmp");

    let content = serde_json::to_string_pretty(state).context("Failed to serialize undo state")?;

    fs::write(&tmp_path, &content)
        .with_context(|| format!("Failed to write temp undo file: {}", tmp_path.display()))?;

    fs::rename(&tmp_path, &path)
        .with_context(|| format!("Failed to rename temp undo file to: {}", path.display()))?;

    Ok(())
}
