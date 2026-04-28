/// `ddbm migrate` - Start a new migration.
///
/// Supports both interactive and non-interactive modes.
use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::Client;
use serde_json::Value;

use crate::engine::migration::run_migration;
use crate::state::migration_state::{self, Migration, MigrationStatus};
use crate::state::undo_state;
use crate::ui::prompts;

/// Run the migrate command interactively.
pub async fn run_interactive(client: &Client, state_dir: &Path) -> Result<()> {
    let mut state = migration_state::load_state(state_dir)?;
    let mut undo = undo_state::load_undo_state(state_dir)?;

    let (source_table, target_table, column_mappings) = prompts::get_migration_input()?;

    if !prompts::show_summary(&source_table, &target_table, &column_mappings)? {
        println!("Migration cancelled.");
        return Ok(());
    }

    let migration_id = prompts::create_migration_id();

    let migration = Migration {
        id: migration_id.clone(),
        source_table,
        target_table,
        column_mappings,
        last_evaluated_key: None,
        processed_items: 0,
        status: MigrationStatus::InProgress,
        key_schema: None,
    };

    state.migrations.push(migration);
    migration_state::save_state(state_dir, &state)?;

    println!(
        "\n📋 Migration '{}' created.",
        console::style(&migration_id).yellow()
    );

    let index = state.migrations.len() - 1;
    run_migration(client, &mut state, &mut undo, index, state_dir).await?;

    Ok(())
}

/// Run the migrate command non-interactively.
pub async fn run_non_interactive(
    client: &Client,
    state_dir: &Path,
    source: &str,
    target: &str,
    mappings_file: Option<&Path>,
    passthrough: bool,
    exclude: Option<&str>,
) -> Result<()> {
    let mut state = migration_state::load_state(state_dir)?;
    let mut undo = undo_state::load_undo_state(state_dir)?;

    let column_mappings = if passthrough {
        let mut m: HashMap<String, Value> = HashMap::new();
        m.insert(
            "__PASSTHROUGH__".to_string(),
            Value::String("true".to_string()),
        );

        if let Some(excl) = exclude {
            let exclude_list: Vec<Value> = excl
                .split(',')
                .map(|s| Value::String(s.trim().to_string()))
                .filter(|v| v.as_str().map(|s| !s.is_empty()).unwrap_or(false))
                .collect();
            if !exclude_list.is_empty() {
                m.insert("__EXCLUDE__".to_string(), Value::Array(exclude_list));
            }
        }

        m
    } else if let Some(file_path) = mappings_file {
        let content = std::fs::read_to_string(file_path)
            .with_context(|| format!("Failed to read mappings file: {}", file_path.display()))?;

        serde_json::from_str::<HashMap<String, Value>>(&content)
            .with_context(|| format!("Invalid JSON in mappings file: {}", file_path.display()))?
    } else {
        anyhow::bail!(
            "Either --passthrough or --mappings <file> must be specified in non-interactive mode"
        );
    };

    let migration_id = prompts::create_migration_id();

    let migration = Migration {
        id: migration_id.clone(),
        source_table: source.to_string(),
        target_table: target.to_string(),
        column_mappings,
        last_evaluated_key: None,
        processed_items: 0,
        status: MigrationStatus::InProgress,
        key_schema: None,
    };

    state.migrations.push(migration);
    migration_state::save_state(state_dir, &state)?;

    println!("📋 Migration '{}' created.", migration_id);

    let index = state.migrations.len() - 1;
    run_migration(client, &mut state, &mut undo, index, state_dir).await?;

    Ok(())
}
