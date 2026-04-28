/// Undo engine - delete migrated items from the target table.
use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::types::{AttributeValue, DeleteRequest, WriteRequest};
use aws_sdk_dynamodb::Client;
use indicatif::{ProgressBar, ProgressStyle};
use tracing::info;

use crate::aws::{operations, types};
use crate::state::migration_state::{MigrationState, MigrationStatus};
use crate::state::undo_state::UndoState;
use crate::state::{migration_state, undo_state};

const BATCH_SIZE: usize = 25;

/// Run the undo process for a specific migration.
pub async fn run_undo(
    client: &Client,
    state: &mut MigrationState,
    undo: &mut UndoState,
    migration_id: &str,
    state_dir: &Path,
) -> Result<()> {
    // Find the migration
    let migration = state
        .migrations
        .iter()
        .find(|migration| migration.id == migration_id)
        .context("Migration not found in state")?;

    let target_table = migration.target_table.clone();

    // Get undo keys
    let undo_entry = undo
        .undo_migrations
        .get(migration_id)
        .context("No undo data found for this migration")?;

    let undo_keys = &undo_entry.delete_request.keys;
    if undo_keys.is_empty() {
        println!("No items to undo for this migration.");
        return Ok(());
    }

    let total_items = undo_keys.len();
    println!("\n🔄 Starting undo operation ({} items)...", total_items);
    info!("Starting undo for migration '{migration_id}' ({total_items} items)");

    let progress = ProgressBar::new(total_items as u64);
    progress.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} [{bar:40.cyan/dim}] {pos}/{len} items ({eta} remaining)",
        )
        .unwrap()
        .progress_chars("█▓▒░"),
    );

    for chunk in undo_keys.chunks(BATCH_SIZE) {
        let write_requests: Vec<WriteRequest> = chunk
            .iter()
            .map(|undo_key| {
                let attribute_key: HashMap<String, AttributeValue> = undo_key
                    .iter()
                    .map(|(key_name, key_value)| {
                        (key_name.clone(), types::to_attribute_value(key_value))
                    })
                    .collect();

                let delete_request = DeleteRequest::builder()
                    .set_key(Some(attribute_key))
                    .build()
                    .expect("Failed to build DeleteRequest");

                WriteRequest::builder()
                    .delete_request(delete_request)
                    .build()
            })
            .collect();

        operations::batch_write(client, &target_table, write_requests).await?;

        progress.inc(chunk.len() as u64);
    }

    progress.finish_and_clear();

    // Clean up undo state
    undo.undo_migrations.remove(migration_id);
    undo_state::save_undo_state(state_dir, undo)?;

    // Update migration status
    if let Some(migration_entry) = state
        .migrations
        .iter_mut()
        .find(|migration| migration.id == migration_id)
    {
        if migration_entry.status == MigrationStatus::Completed {
            migration_entry.status = MigrationStatus::Undone;
            migration_entry.processed_items = 0;
        }
    }
    migration_state::save_state(state_dir, state)?;

    println!("✅ Undo completed successfully.");
    info!("Undo for migration '{migration_id}' completed.");

    Ok(())
}
