/// DynamoDB operations - scan, batch write, describe/create table.
use std::collections::HashMap;

use anyhow::{Context, Result};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, GlobalSecondaryIndex, KeySchemaElement,
    Projection, ProvisionedThroughput, WriteRequest,
};
use aws_sdk_dynamodb::Client;
use tracing::{info, warn};

/// Scan a DynamoDB table page by page.
///
/// Returns items as a `Vec<HashMap<String, AttributeValue>>` and
/// an optional `LastEvaluatedKey` for pagination.
pub async fn scan_page(
    client: &Client,
    table_name: &str,
    exclusive_start_key: Option<HashMap<String, AttributeValue>>,
) -> Result<(
    Vec<HashMap<String, AttributeValue>>,
    Option<HashMap<String, AttributeValue>>,
)> {
    let mut req = client.scan().table_name(table_name);

    if let Some(start_key) = exclusive_start_key {
        for (k, v) in start_key {
            req = req.exclusive_start_key(k, v);
        }
    }

    let response = req
        .send()
        .await
        .with_context(|| format!("Failed to scan table '{table_name}'"))?;

    let items = response.items().to_vec();
    let last_key = response.last_evaluated_key().map(|k| k.to_owned());

    Ok((items, last_key))
}

/// Execute a batch write with automatic retry for unprocessed items.
pub async fn batch_write(
    client: &Client,
    table_name: &str,
    write_requests: Vec<WriteRequest>,
) -> Result<()> {
    if write_requests.is_empty() {
        return Ok(());
    }

    let mut remaining = write_requests;

    loop {
        let response = client
            .batch_write_item()
            .request_items(table_name, remaining.clone())
            .send()
            .await
            .with_context(|| format!("Batch write failed for table '{table_name}'"))?;

        let unprocessed = response.unprocessed_items();
        let items = unprocessed.and_then(|m| m.get(table_name));
        match items {
            Some(items) if !items.is_empty() => {
                warn!("Retrying {} unprocessed items...", items.len());
                remaining = items.clone();
                // Small delay before retry
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            _ => break,
        }
    }

    Ok(())
}

/// Get the key schema for a table.
///
/// Returns a map of key type ("HASH"/"RANGE") to attribute name.
pub async fn get_key_schema(client: &Client, table_name: &str) -> Result<HashMap<String, String>> {
    let response = client
        .describe_table()
        .table_name(table_name)
        .send()
        .await
        .with_context(|| format!("Failed to describe table '{table_name}'"))?;

    let table = response
        .table()
        .context("Table description missing from response")?;

    let mut keys = HashMap::new();
    for key in table.key_schema() {
        keys.insert(
            key.key_type().as_str().to_string(),
            key.attribute_name().to_string(),
        );
    }

    Ok(keys)
}

/// Check if a table exists.
pub async fn table_exists(client: &Client, table_name: &str) -> Result<bool> {
    match client.describe_table().table_name(table_name).send().await {
        Ok(_) => Ok(true),
        Err(err) => {
            let service_err = err.as_service_error();
            if let Some(e) = service_err {
                if e.is_resource_not_found_exception() {
                    return Ok(false);
                }
            }
            Err(err).with_context(|| format!("Failed to check if table '{table_name}' exists"))
        }
    }
}

/// Create a target table mirroring the source table's schema and GSIs.
pub async fn create_table_from_source(
    client: &Client,
    source_table_name: &str,
    target_table_name: &str,
) -> Result<()> {
    info!("Describing source table '{source_table_name}' for replication...");

    let source_desc = client
        .describe_table()
        .table_name(source_table_name)
        .send()
        .await
        .with_context(|| format!("Failed to describe source table '{source_table_name}'"))?;

    let source_table = source_desc
        .table()
        .context("Source table description missing")?;

    // Determine billing mode
    let is_pay_per_request = source_table
        .billing_mode_summary()
        .and_then(|s| s.billing_mode())
        .map(|m| m == &BillingMode::PayPerRequest)
        .unwrap_or(false);

    // Collect key attribute names (primary + GSI)
    let mut key_attribute_names: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    for key in source_table.key_schema() {
        key_attribute_names.insert(key.attribute_name().to_string());
    }

    // Build GSI configs
    let mut gsi_configs: Vec<GlobalSecondaryIndex> = Vec::new();

    let gsis = source_table.global_secondary_indexes();
    for gsi in gsis {
        for key in gsi.key_schema() {
            key_attribute_names.insert(key.attribute_name().to_string());
        }

        let key_schema: Vec<KeySchemaElement> = gsi
            .key_schema()
            .iter()
            .map(|key_element: &KeySchemaElement| {
                KeySchemaElement::builder()
                    .attribute_name(key_element.attribute_name())
                    .key_type(key_element.key_type().clone())
                    .build()
                    .expect("failed to build KeySchemaElement")
            })
            .collect();

        let projection = gsi.projection().expect("GSI missing projection");
        let proj_builder = Projection::builder().projection_type(
            projection
                .projection_type()
                .cloned()
                .unwrap_or(aws_sdk_dynamodb::types::ProjectionType::All),
        );

        let proj = proj_builder.build();

        let mut gsi_builder = GlobalSecondaryIndex::builder()
            .index_name(gsi.index_name().unwrap_or_default())
            .set_key_schema(Some(key_schema))
            .projection(proj);

        if !is_pay_per_request {
            let provisioned = gsi.provisioned_throughput();
            let read_capacity = provisioned
                .and_then(|p| p.read_capacity_units)
                .unwrap_or(1)
                .max(1);
            let write_capacity = provisioned
                .and_then(|p| p.write_capacity_units)
                .unwrap_or(1)
                .max(1);

            gsi_builder = gsi_builder.provisioned_throughput(
                ProvisionedThroughput::builder()
                    .read_capacity_units(read_capacity)
                    .write_capacity_units(write_capacity)
                    .build()
                    .expect("failed to build ProvisionedThroughput"),
            );
        }

        gsi_configs.push(gsi_builder.build().expect("failed to build GSI"));
    }

    // Filter attribute definitions to key attributes only
    let attribute_definitions: Vec<AttributeDefinition> = source_table
        .attribute_definitions()
        .iter()
        .filter(|ad| key_attribute_names.contains(ad.attribute_name()))
        .map(|ad| {
            AttributeDefinition::builder()
                .attribute_name(ad.attribute_name())
                .attribute_type(ad.attribute_type().clone())
                .build()
                .expect("failed to build AttributeDefinition")
        })
        .collect();

    // Build key schema
    let key_schema: Vec<KeySchemaElement> = source_table
        .key_schema()
        .iter()
        .map(|key_element| {
            KeySchemaElement::builder()
                .attribute_name(key_element.attribute_name())
                .key_type(key_element.key_type().clone())
                .build()
                .expect("failed to build KeySchemaElement")
        })
        .collect();

    // Build create table request
    let mut create_req = client
        .create_table()
        .table_name(target_table_name)
        .set_key_schema(Some(key_schema))
        .set_attribute_definitions(Some(attribute_definitions));

    if is_pay_per_request {
        create_req = create_req.billing_mode(BillingMode::PayPerRequest);
    } else {
        create_req = create_req.billing_mode(BillingMode::Provisioned);

        let provisioned = source_table.provisioned_throughput();
        let read_capacity = provisioned.and_then(|p| p.read_capacity_units).unwrap_or(5);
        let write_capacity = provisioned
            .and_then(|p| p.write_capacity_units)
            .unwrap_or(5);

        create_req = create_req.provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(read_capacity)
                .write_capacity_units(write_capacity)
                .build()
                .expect("failed to build ProvisionedThroughput"),
        );
    }

    if !gsi_configs.is_empty() {
        info!(
            "Copying {} Global Secondary Indexes from source table",
            gsi_configs.len()
        );
        create_req = create_req.set_global_secondary_indexes(Some(gsi_configs));
    }

    info!("Creating target table '{target_table_name}'...");
    create_req
        .send()
        .await
        .with_context(|| format!("Failed to create target table '{target_table_name}'"))?;

    // Wait for table to become active
    info!("Waiting for table to become active...");
    loop {
        let desc = client
            .describe_table()
            .table_name(target_table_name)
            .send()
            .await?;

        let status = desc
            .table()
            .and_then(|t| t.table_status())
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();

        if status == "ACTIVE" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    info!("Target table '{target_table_name}' created successfully.");
    Ok(())
}
