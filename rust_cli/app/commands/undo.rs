/// `ddbm undo` - Undo a completed migration.
use std::path::Path;

use anyhow::Result;
use aws_sdk_dynamodb::Client;

use crate::engine::undo::run_undo;
use crate::state::migration_state;
use crate::state::undo_state;
use crate::ui::prompts;

/// Run the undo command.
pub async fn run(client: &Client, state_dir: &Path, migration_id: Option<&str>) -> Result<()> {
    let mut state = migration_state::load_state(state_dir)?;
    let mut undo = undo_state::load_undo_state(state_dir)?;

    if undo.undo_migrations.is_empty() {
        println!("No migrations available to undo.");
        return Ok(());
    }

    let selected_id = if let Some(id) = migration_id {
        if !undo.undo_migrations.contains_key(id) {
            anyhow::bail!("No undo data found for migration '{id}'");
        }
        id.to_string()
    } else {
        let migration_ids: Vec<String> = undo.undo_migrations.keys().cloned().collect();

        match prompts::select_undo_migration(&migration_ids)? {
            Some(id) => id,
            None => {
                println!("Undo cancelled.");
                return Ok(());
            }
        }
    };

    run_undo(client, &mut state, &mut undo, &selected_id, state_dir).await?;

    Ok(())
}
