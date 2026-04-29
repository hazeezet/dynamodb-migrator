import boto3
import re
import sys
import traceback
from botocore.exceptions import NoCredentialsError
from .state_manager import save_state, load_undo_state, save_undo_state
from .dynamodb_operations import get_table_key_schema, execute_batch_write
from .utils.converters import convert_to_dynamodb_type
from .utils.template_processor import apply_template
from .utils.logger import get_logger

logger = get_logger()


def process_record(item, migration_config):
    """
    Process a single DynamoDB record based on migration configuration.
    This is the core transformation logic.
    """
    new_item = {}
    column_mappings = migration_config.get("column_mappings", {})

    # Check if we're in passthrough mode
    if column_mappings.get("__PASSTHROUGH__") == "true":
        # Copy all attributes except those in exclude list
        exclude_list = column_mappings.get("__EXCLUDE__", [])
        for key, value in item.items():
            if key not in exclude_list:
                new_item[key] = value
    else:
        # Original processing with specific mappings
        for target_col, template in column_mappings.items():
            if target_col.startswith("__"):  # Skip internal keys
                continue

            try:
                # Check if template is a pure placeholder
                if isinstance(template, str):
                    pure_placeholder_match = re.fullmatch(r"\{(\w+)\}", template)
                    if pure_placeholder_match:
                        placeholder = pure_placeholder_match.group(1)
                        new_item[target_col] = item.get(placeholder, None)
                        continue

                # Handle complex templates (using template_processor)
                if isinstance(template, str) and "{" in template:
                    new_item[target_col] = apply_template(template, item)
                else:
                    # Direct value
                    new_item[target_col] = template

            except Exception as e:
                logger.error(f"Error processing column '{target_col}': {e}")

    return new_item


def migrate_data(state, migration):
    """Execute the data migration process."""

    try:
        dynamodb = boto3.resource("dynamodb")
        source_table = dynamodb.Table(migration["source_table"])
        target_table = dynamodb.Table(migration["target_table"])
    except NoCredentialsError:
        logger.error("AWS credentials not found.")
        migration["status"] = "error"
        save_state(state)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
        migration["status"] = "error"
        save_state(state)
        sys.exit(1)

    undo_state = load_undo_state()
    mig_id = migration["id"]
    if mig_id not in undo_state["undo_migrations"]:
        undo_state["undo_migrations"][mig_id] = {"DeleteRequest": {"Key": []}}

    undo_keys = undo_state["undo_migrations"][mig_id]["DeleteRequest"]["Key"]

    try:
        print("\nStarting migration...")
        key_schema = get_table_key_schema(target_table, source_table)
        migration["key_schema"] = key_schema
        save_state(state)

        paginator = dynamodb.meta.client.get_paginator("scan")
        scan_kwargs = {"TableName": migration["source_table"]}
        if migration.get("last_evaluated_key"):
            scan_kwargs["ExclusiveStartKey"] = migration["last_evaluated_key"]

        response_iterator = paginator.paginate(**scan_kwargs)
        total_items = migration.get("processed_items", 0)
        write_requests = []
        batch_size = 25

        for page in response_iterator:
            items = page.get("Items", [])
            for item in items:
                # USE THE CORE LOGIC FUNCTION
                new_item = process_record(item, migration)

                # Format the item for DynamoDB
                formatted_item = {}
                for k, v in new_item.items():
                    formatted_item[k] = convert_to_dynamodb_type(v)

                write_requests.append({"PutRequest": {"Item": formatted_item}})

                # Build the key for undo operation
                key = {}
                for key_type, key_name in key_schema.items():
                    if key_name in formatted_item:
                        key[key_name] = formatted_item[key_name]
                    elif key_name in item:
                        key[key_name] = {"S": str(item[key_name])}
                    else:
                        key[key_name] = {"S": ""}

                undo_keys.append(key)
                total_items += 1

                if len(write_requests) >= batch_size:
                    execute_batch_write(target_table, write_requests)
                    write_requests = []
                    migration["processed_items"] = total_items
                    save_state(state)
                    save_undo_state(undo_state)
                    print(f"Processed {total_items} items...", end="\r")

            # Save progress after each page
            if page.get("LastEvaluatedKey"):
                migration["last_evaluated_key"] = page["LastEvaluatedKey"]
                save_state(state)

        # Final batch
        if write_requests:
            execute_batch_write(target_table, write_requests)

        migration["status"] = "completed"
        migration["processed_items"] = total_items
        save_state(state)
        save_undo_state(undo_state)

        print(f"\nMigration completed! Processed {total_items} items.")

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        traceback.print_exc()
        migration["status"] = "error"
        save_state(state)
        sys.exit(1)
