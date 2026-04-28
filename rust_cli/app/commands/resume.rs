/// `ddbm resume` - Resume an existing migration.
use std::path::Path;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::Client;
use console::style;

use crate::engine::migration::run_migration;
use crate::state::migration_state::{self, MigrationStatus};
use crate::state::undo_state;
use crate::ui::prompts;

/// Run the resume command.
pub async fn run(client: &Client, state_dir: &Path, migration_id: Option<&str>) -> Result<()> {
    let mut state = migration_state::load_state(state_dir)?;
    let mut undo = undo_state::load_undo_state(state_dir)?;

    if state.migrations.is_empty() {
        println!("No migration jobs found.");
        return Ok(());
    }

    // If migration_id provided, find it directly
    let index = if let Some(id) = migration_id {
        state
            .migrations
            .iter()
            .position(|m| m.id == id)
            .with_context(|| format!("Migration '{id}' not found"))?
    } else {
        // Interactive selection
        match prompts::select_migration(&state) {
            Ok(Some(idx)) => idx,
            Ok(None) => {
                println!("No migration selected.");
                return Ok(());
            }
            Err(e) if e.to_string() == "undo" => {
                // User chose undo from the menu
                return crate::commands::undo::run(client, state_dir, None).await;
            }
            Err(e) => return Err(e),
        }
    };

    let migration = &state.migrations[index];

    println!(
        "\n📋 Selected: {} │ Status: {}",
        style(&migration.id).yellow(),
        match migration.status {
            MigrationStatus::Completed => style(migration.status.to_string()).green(),
            MigrationStatus::InProgress => style(migration.status.to_string()).yellow(),
            MigrationStatus::Error => style(migration.status.to_string()).red(),
            MigrationStatus::Undone => style(migration.status.to_string()).dim(),
        }
    );

    match migration.status {
        MigrationStatus::Completed => {
            println!("This migration has already been completed.");

            let choices = vec!["Delete and start fresh", "Exit"];
            let selection = dialoguer::Select::new()
                .with_prompt("What would you like to do?")
                .items(&choices)
                .default(1)
                .interact()
                .context("Selection cancelled")?;

            if selection == 0 {
                state.migrations.remove(index);
                migration_state::save_state(state_dir, &state)?;
                println!("Migration deleted. You can start a new migration now.");
            }

            return Ok(());
        }
        MigrationStatus::InProgress | MigrationStatus::Error | MigrationStatus::Undone => {
            let action = prompts::select_migration_action()?;

            match action {
                "continue" => {
                    println!("Continuing migration...");
                }
                "edit" => {
                    let migration = &mut state.migrations[index];
                    prompts::edit_migration(migration)?;

                    if !prompts::show_summary(
                        &migration.source_table,
                        &migration.target_table,
                        &migration.column_mappings,
                    )? {
                        println!("Migration cancelled.");
                        return Ok(());
                    }
                }
                "delete" => {
                    state.migrations.remove(index);
                    migration_state::save_state(state_dir, &state)?;
                    println!("Migration deleted.");
                    return Ok(());
                }
                _ => return Ok(()),
            }
        }
    }

    run_migration(client, &mut state, &mut undo, index, state_dir).await?;

    Ok(())
}
