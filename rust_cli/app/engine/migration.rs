/// Core migration engine - scan source table, apply transformations, write to target.
use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::types::{AttributeValue, PutRequest, WriteRequest};
use aws_sdk_dynamodb::Client;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use serde_json::Value;
use tracing::{debug, info};

use crate::aws::{operations, types};
use crate::state::migration_state::{Migration, MigrationState, MigrationStatus};
use crate::state::undo_state::UndoState;
use crate::state::{migration_state, undo_state};
use crate::transform::template;

const BATCH_SIZE: usize = 25;

/// Run the migration process for a given migration job.
pub async fn run_migration(
    client: &Client,
    state: &mut MigrationState,
    undo: &mut UndoState,
    migration_index: usize,
    state_dir: &Path,
) -> Result<()> {
    // Get key schema (creating target table if needed)
    let migration = &state.migrations[migration_index];
    let target_table = &migration.target_table;
    let source_table = &migration.source_table;

    // Check if target table exists
    let target_exists = operations::table_exists(client, target_table).await?;
    if !target_exists {
        println!("\n⚠ Target table '{}' does not exist.", target_table);
        println!("  Creating it from source table schema...");
        operations::create_table_from_source(client, source_table, target_table).await?;
        println!("  ✓ Target table created successfully.");
    }

    // Get key schema
    let key_schema = operations::get_key_schema(client, target_table).await?;
    state.migrations[migration_index].key_schema = Some(key_schema.clone());
    migration_state::save_state(state_dir, state)?;

    // Ensure undo entry exists
    let migration_id = state.migrations[migration_index].id.clone();
    undo.ensure_migration(&migration_id);

    let progress = ProgressBar::new_spinner();
    progress.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );

    println!("\n🚀 Starting migration...");
    info!("Starting migration '{}'", migration_id);

    let mut total_items = state.migrations[migration_index].processed_items;
    let mut write_requests: Vec<WriteRequest> = Vec::new();

    // Build start key from state if resuming
    let mut start_key = build_start_key(&state.migrations[migration_index]);

    loop {
        let (items, next_key) = operations::scan_page(
            client,
            &state.migrations[migration_index].source_table,
            start_key,
        )
        .await?;

        if items.is_empty() && next_key.is_none() {
            break;
        }

        for item in &items {
            // Convert DynamoDB item to serde_json map for template processing
            let json_item: serde_json::Map<String, Value> = item
                .iter()
                .map(|(key, attr_value)| (key.clone(), types::from_attribute_value(attr_value)))
                .collect();

            // Apply column mappings
            let new_item = apply_column_mappings(
                &state.migrations[migration_index].column_mappings,
                &json_item,
            )?;

            // Convert back to DynamoDB AttributeValues
            let mut formatted_item: HashMap<String, AttributeValue> = HashMap::new();
            for (column_name, column_value) in &new_item {
                formatted_item.insert(column_name.clone(), types::to_attribute_value(column_value));
            }

            // Build undo key
            let mut undo_key: HashMap<String, Value> = HashMap::new();
            for key_name in key_schema.values() {
                if let Some(attribute_value) = formatted_item.get(key_name) {
                    undo_key.insert(
                        key_name.clone(),
                        types::from_attribute_value(attribute_value),
                    );
                } else if let Some(json_value) = json_item.get(key_name) {
                    undo_key.insert(key_name.clone(), json_value.clone());
                } else {
                    undo_key.insert(key_name.clone(), Value::String(String::new()));
                }
            }

            // Store undo key
            let undo_entry = undo.ensure_migration(&migration_id);
            undo_entry.delete_request.keys.push(
                undo_key
                    .into_iter()
                    .map(|(key, value)| {
                        let mut single_key_map = HashMap::new();
                        single_key_map.insert(key, value);
                        single_key_map
                    })
                    .fold(HashMap::new(), |mut accumulator, map| {
                        accumulator.extend(map);
                        accumulator
                    }),
            );

            // Build write request
            let put_request = PutRequest::builder()
                .set_item(Some(formatted_item))
                .build()
                .context("Failed to build PutRequest")?;

            write_requests.push(WriteRequest::builder().put_request(put_request).build());

            // Flush batch when full
            if write_requests.len() == BATCH_SIZE {
                operations::batch_write(
                    client,
                    &state.migrations[migration_index].target_table,
                    write_requests.clone(),
                )
                .await?;

                write_requests.clear();
                total_items += BATCH_SIZE as u64;
                state.migrations[migration_index].processed_items = total_items;
                migration_state::save_state(state_dir, state)?;

                progress.set_message(format!("Processed {} items...", total_items));
                progress.tick();
            }
        }

        // Update pagination state
        if let Some(ref evaluated_key) = next_key {
            let json_key: HashMap<String, Value> = evaluated_key
                .iter()
                .map(|(key, attr_value)| (key.clone(), types::from_attribute_value(attr_value)))
                .collect();
            state.migrations[migration_index].last_evaluated_key = Some(json_key);
            migration_state::save_state(state_dir, state)?;
        }

        start_key = next_key;
        if start_key.is_none() {
            break;
        }
    }

    // Flush remaining items
    if !write_requests.is_empty() {
        let remaining_count = write_requests.len() as u64;
        operations::batch_write(
            client,
            &state.migrations[migration_index].target_table,
            write_requests,
        )
        .await?;
        total_items += remaining_count;
        state.migrations[migration_index].processed_items = total_items;
    }

    // Mark completed
    state.migrations[migration_index].status = MigrationStatus::Completed;
    state.migrations[migration_index].last_evaluated_key = None;
    migration_state::save_state(state_dir, state)?;
    undo_state::save_undo_state(state_dir, undo)?;

    progress.finish_and_clear();
    println!(
        "\n✅ Migration completed successfully. Total items migrated: {}",
        total_items
    );
    info!(
        "Migration '{}' completed. Total items: {}",
        migration_id, total_items
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_passthrough_with_exclusions() {
        let mut item = serde_json::Map::new();
        item.insert("id".to_string(), json!("123"));
        item.insert("secret".to_string(), json!("password"));
        item.insert("public".to_string(), json!("hello"));

        let mut mappings = HashMap::new();
        mappings.insert("__PASSTHROUGH__".to_string(), json!("true"));
        mappings.insert("__EXCLUDE__".to_string(), json!(["secret"]));

        let result = apply_column_mappings(&mappings, &item).unwrap();

        assert_eq!(result.get("id").unwrap(), "123");
        assert_eq!(result.get("public").unwrap(), "hello");
        assert!(result.get("secret").is_none());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_specific_mappings() {
        let mut item = serde_json::Map::new();
        item.insert("first_name".to_string(), json!("john"));
        item.insert("last_name".to_string(), json!("doe"));
        item.insert("age".to_string(), json!(30));

        let mut mappings = HashMap::new();
        mappings.insert(
            "full_name".to_string(),
            json!("{first_name title} {last_name title}"),
        );
        mappings.insert("years".to_string(), json!("{age}"));
        mappings.insert("static".to_string(), json!("fixed_value"));

        let result = apply_column_mappings(&mappings, &item).unwrap();

        assert_eq!(result.get("full_name").unwrap(), "John Doe");
        assert_eq!(result.get("years").unwrap(), 30);
        assert_eq!(result.get("static").unwrap(), "fixed_value");
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_passthrough_disabled() {
        let mut item = serde_json::Map::new();
        item.insert("id".to_string(), json!("123"));
        item.insert("other".to_string(), json!("data"));

        let mut mappings = HashMap::new();
        mappings.insert("id_copy".to_string(), json!("{id}"));

        let result = apply_column_mappings(&mappings, &item).unwrap();

        assert_eq!(result.get("id_copy").unwrap(), "123");
        assert!(result.get("id").is_none());
        assert!(result.get("other").is_none());
    }
}

/// Apply column mappings to a source item.
fn apply_column_mappings(
    mappings: &HashMap<String, Value>,
    item: &serde_json::Map<String, Value>,
) -> Result<serde_json::Map<String, Value>> {
    let mut new_item = serde_json::Map::new();

    // Check passthrough mode
    let is_passthrough = mappings
        .get("__PASSTHROUGH__")
        .and_then(|v| v.as_str())
        .map(|s| s == "true")
        .unwrap_or(false);

    if is_passthrough {
        let exclude_list: Vec<String> = mappings
            .get("__EXCLUDE__")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        for (key, value) in item {
            if !exclude_list.contains(key) {
                new_item.insert(key.clone(), value.clone());
            }
        }

        return Ok(new_item);
    }

    // Specific column mappings
    let pure_placeholder = Regex::new(r"^\{(\w+)\}$").expect("invalid regex");

    for (target_col, template_value) in mappings {
        match template_value {
            Value::String(tmpl) => {
                // Check for pure placeholder (direct value copy)
                if let Some(captures) = pure_placeholder.captures(tmpl) {
                    let field_name = captures.get(1).unwrap().as_str();
                    let value = item.get(field_name).cloned().unwrap_or(Value::Null);
                    debug!("Pure placeholder {target_col}: {:?}", value);
                    new_item.insert(target_col.clone(), value);
                    continue;
                }

                // Apply template
                let result = template::apply_template(tmpl, item)
                    .with_context(|| format!("Failed to process template for '{target_col}'"))?;

                new_item.insert(target_col.clone(), Value::String(result));
            }
            // Direct values (numbers, booleans, etc.)
            other => {
                debug!("Direct value {target_col}: {:?}", other);
                new_item.insert(target_col.clone(), other.clone());
            }
        }
    }

    Ok(new_item)
}

/// Build a DynamoDB start key from the migration's saved pagination state.
fn build_start_key(migration: &Migration) -> Option<HashMap<String, AttributeValue>> {
    migration.last_evaluated_key.as_ref().map(|saved_key| {
        saved_key
            .iter()
            .map(|(key, value)| (key.clone(), types::to_attribute_value(value)))
            .collect()
    })
}
