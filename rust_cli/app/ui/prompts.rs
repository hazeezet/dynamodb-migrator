/// Interactive prompts for the CLI using `dialoguer`.
use std::collections::HashMap;

use anyhow::{Context, Result};
use chrono::Local;
use console::style;
use dialoguer::{Confirm, Input, Select};
use serde_json::Value;

use crate::state::migration_state::{Migration, MigrationState, MigrationStatus};

/// Display a styled header.
pub fn print_header(text: &str) {
    println!("\n{}", style(format!("═══ {text} ═══")).cyan().bold());
}

/// Display a styled sub-header.
pub fn print_subheader(text: &str) {
    println!("\n{}", style(format!("── {text} ──")).dim());
}

/// Create a unique migration ID.
pub fn create_migration_id() -> String {
    format!("migration_{}", Local::now().format("%Y%m%d_%H%M%S"))
}

/// Let user select an existing migration or create a new one.
///
/// Returns:
/// - `Ok(Some(index))` if a migration was selected
/// - `Ok(None)` if user wants a new migration
/// - `Err(...)` with a message "undo" if user wants to undo
pub fn select_migration(state: &MigrationState) -> Result<Option<usize>> {
    if state.migrations.is_empty() {
        return Ok(None);
    }

    print_header("Existing Migration Jobs");

    let mut items: Vec<String> = state
        .migrations
        .iter()
        .enumerate()
        .map(|(i, m)| {
            format!(
                "{}. {} │ {} → {} │ {}",
                i + 1,
                style(&m.id).yellow(),
                style(&m.source_table).green(),
                style(&m.target_table).green(),
                match m.status {
                    MigrationStatus::Completed => style(m.status.to_string()).green(),
                    MigrationStatus::InProgress => style(m.status.to_string()).yellow(),
                    MigrationStatus::Error => style(m.status.to_string()).red(),
                    MigrationStatus::Undone => style(m.status.to_string()).dim(),
                }
            )
        })
        .collect();

    items.push(format!("{}", style("✚ New migration").cyan().bold()));
    items.push(format!("{}", style("↩ Undo a migration").magenta()));

    let selection = Select::new()
        .with_prompt("Select an action")
        .items(&items)
        .default(0)
        .interact()
        .context("Selection cancelled")?;

    let migration_count = state.migrations.len();

    if selection == migration_count {
        // New migration
        Ok(None)
    } else if selection == migration_count + 1 {
        // Undo
        anyhow::bail!("undo")
    } else {
        Ok(Some(selection))
    }
}

/// Get migration configuration interactively.
pub fn get_migration_input() -> Result<(String, String, HashMap<String, Value>)> {
    print_subheader("New Migration");

    let source_table: String = Input::new()
        .with_prompt("Source table name")
        .interact_text()
        .context("Source table input cancelled")?;

    let target_table: String = Input::new()
        .with_prompt("Target table name")
        .interact_text()
        .context("Target table input cancelled")?;

    if source_table.is_empty() || target_table.is_empty() {
        anyhow::bail!("Table names cannot be empty");
    }

    print_subheader("Column Mappings");

    let mapping_choices = vec![
        "Copy all attributes directly (passthrough mode)",
        "Define specific column mappings",
    ];

    let mapping_type = Select::new()
        .with_prompt("Mapping mode")
        .items(&mapping_choices)
        .default(0)
        .interact()
        .context("Mapping mode selection cancelled")?;

    let mut column_mappings: HashMap<String, Value> = HashMap::new();

    if mapping_type == 0 {
        // Passthrough mode
        column_mappings.insert(
            "__PASSTHROUGH__".to_string(),
            Value::String("true".to_string()),
        );
        println!(
            "  {}",
            style("Passthrough mode: all source attributes will be copied.").dim()
        );

        let exclude: String = Input::new()
            .with_prompt("Columns to exclude (comma-separated, or leave empty)")
            .default(String::new())
            .interact_text()
            .context("Exclude input cancelled")?;

        if !exclude.trim().is_empty() {
            let exclude_list: Vec<Value> = exclude
                .split(',')
                .map(|s| Value::String(s.trim().to_string()))
                .filter(|v| v.as_str().map(|s| !s.is_empty()).unwrap_or(false))
                .collect();

            if !exclude_list.is_empty() {
                column_mappings.insert("__EXCLUDE__".to_string(), Value::Array(exclude_list));
            }
        }
    } else {
        // Specific mappings
        println!(
            "  {}",
            style("Format: target_column=template (e.g., name={firstName} {lastName})").dim()
        );
        println!("  {}", style("Enter 'done' when finished.").dim());

        loop {
            let mapping: String = Input::new()
                .with_prompt("Column mapping (or 'done')")
                .interact_text()
                .context("Mapping input cancelled")?;

            if mapping.trim().to_lowercase() == "done" {
                break;
            }

            if !mapping.contains('=') {
                println!(
                    "  {}",
                    style("Invalid format. Use: target_column=template").red()
                );
                continue;
            }

            let (target, template) = mapping.split_once('=').unwrap();
            let target = target.trim();
            let template = template.trim();

            if target.is_empty() || template.is_empty() {
                println!(
                    "  {}",
                    style("Both target column and template are required.").red()
                );
                continue;
            }

            column_mappings.insert(target.to_string(), Value::String(template.to_string()));
            println!(
                "  {} {} = {}",
                style("✓").green(),
                style(target).yellow(),
                style(template).dim()
            );
        }
    }

    if column_mappings.is_empty() {
        anyhow::bail!("You must define at least one column mapping");
    }

    Ok((source_table, target_table, column_mappings))
}

/// Display migration summary and ask for confirmation.
pub fn show_summary(
    source_table: &str,
    target_table: &str,
    column_mappings: &HashMap<String, Value>,
) -> Result<bool> {
    print_header("Migration Summary");

    println!("  Source: {}", style(source_table).green().bold());
    println!("  Target: {}", style(target_table).green().bold());

    if !column_mappings.is_empty() {
        println!("\n  Column Mappings:");
        for (target, template) in column_mappings {
            if target.starts_with("__") {
                continue; // Skip internal keys
            }
            println!(
                "    {} {} {}",
                style(target).yellow(),
                style("→").dim(),
                style(template).cyan()
            );
        }

        // Show passthrough info
        if column_mappings.contains_key("__PASSTHROUGH__") {
            println!(
                "    {} Passthrough mode (all attributes)",
                style("●").cyan()
            );
            if let Some(Value::Array(excludes)) = column_mappings.get("__EXCLUDE__") {
                let names: Vec<&str> = excludes.iter().filter_map(|v| v.as_str()).collect();
                if !names.is_empty() {
                    println!("    {} Excluding: {}", style("○").dim(), names.join(", "));
                }
            }
        }
    }

    let confirmed = Confirm::new()
        .with_prompt("\nProceed with migration?")
        .default(false)
        .interact()
        .context("Confirmation cancelled")?;

    Ok(confirmed)
}

/// Let user edit an existing migration.
pub fn edit_migration(migration: &mut Migration) -> Result<()> {
    print_subheader("Edit Migration");
    println!(
        "  {}",
        style("Press Enter to keep the current value.").dim()
    );

    let new_source: String = Input::new()
        .with_prompt(format!("Source Table [{}]", &migration.source_table))
        .default(migration.source_table.clone())
        .interact_text()
        .context("Source table input cancelled")?;
    migration.source_table = new_source;

    let new_target: String = Input::new()
        .with_prompt(format!("Target Table [{}]", &migration.target_table))
        .default(migration.target_table.clone())
        .interact_text()
        .context("Target table input cancelled")?;
    migration.target_table = new_target;

    print_subheader("Edit Column Mappings");

    for (target, template) in migration.column_mappings.clone() {
        if target.starts_with("__") {
            continue;
        }

        let current = match &template {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };

        let new_template: String = Input::new()
            .with_prompt(format!("Mapping for '{}' [{}]", target, current))
            .default(current)
            .interact_text()
            .context("Mapping edit cancelled")?;

        migration
            .column_mappings
            .insert(target, Value::String(new_template));
    }

    // Allow adding new mappings
    loop {
        let add_more = Confirm::new()
            .with_prompt("Add a new column mapping?")
            .default(false)
            .interact()
            .context("Prompt cancelled")?;

        if !add_more {
            break;
        }

        let mapping: String = Input::new()
            .with_prompt("Column mapping (target_column=template)")
            .interact_text()
            .context("Mapping input cancelled")?;

        if let Some((target, template)) = mapping.split_once('=') {
            let target = target.trim();
            let template = template.trim();
            if !target.is_empty() && !template.is_empty() {
                migration
                    .column_mappings
                    .insert(target.to_string(), Value::String(template.to_string()));
                println!(
                    "  {} {} = {}",
                    style("✓").green(),
                    style(target).yellow(),
                    style(template).dim()
                );
            }
        }
    }

    println!("\n  {}", style("Migration updated successfully.").green());

    Ok(())
}

/// Let user select a migration to undo.
pub fn select_undo_migration(migration_ids: &[String]) -> Result<Option<String>> {
    if migration_ids.is_empty() {
        println!("No migrations available to undo.");
        return Ok(None);
    }

    print_header("Undo Migration");

    let mut items: Vec<String> = migration_ids
        .iter()
        .map(|id| format!("{}", style(id).yellow()))
        .collect();
    items.push(format!("{}", style("Cancel").dim()));

    let selection = Select::new()
        .with_prompt("Select migration to undo")
        .items(&items)
        .default(0)
        .interact()
        .context("Selection cancelled")?;

    if selection == migration_ids.len() {
        Ok(None)
    } else {
        Ok(Some(migration_ids[selection].clone()))
    }
}

/// Prompt for action on an existing migration (continue/edit/delete).
pub fn select_migration_action() -> Result<&'static str> {
    let choices = vec!["Continue migration", "Edit migration", "Delete migration"];

    let selection = Select::new()
        .with_prompt("This migration is incomplete. What would you like to do?")
        .items(&choices)
        .default(0)
        .interact()
        .context("Action selection cancelled")?;

    Ok(match selection {
        0 => "continue",
        1 => "edit",
        2 => "delete",
        _ => "continue",
    })
}
