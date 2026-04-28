/// `ddbm list` - List all migration jobs.
use std::path::Path;

use anyhow::Result;
use console::style;

use crate::state::migration_state::{self, MigrationStatus};

/// Run the list command.
pub fn run(state_dir: &Path) -> Result<()> {
    let state = migration_state::load_state(state_dir)?;

    if state.migrations.is_empty() {
        println!("No migration jobs found.");
        return Ok(());
    }

    println!("\n{}", style("═══ Migration Jobs ═══").cyan().bold());
    println!();

    for (i, m) in state.migrations.iter().enumerate() {
        let status_styled = match m.status {
            MigrationStatus::Completed => style(m.status.to_string()).green(),
            MigrationStatus::InProgress => style(m.status.to_string()).yellow(),
            MigrationStatus::Error => style(m.status.to_string()).red(),
            MigrationStatus::Undone => style(m.status.to_string()).dim(),
        };

        println!("  {}. {}", style(i + 1).dim(), style(&m.id).yellow().bold());
        println!(
            "     {} → {}",
            style(&m.source_table).green(),
            style(&m.target_table).green()
        );
        println!(
            "     Status: {}  │  Items: {}",
            status_styled, m.processed_items
        );
        println!();
    }

    Ok(())
}
