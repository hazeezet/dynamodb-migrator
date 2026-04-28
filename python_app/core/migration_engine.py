import boto3
import re
import sys
import numbers
import traceback
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError
from .state_manager import save_state, load_undo_state, save_undo_state
from .dynamodb_operations import get_table_key_schema, execute_batch_write
from .utils.converters import convert_to_dynamodb_type
from .utils.template_processor import apply_template
from .utils.logger import get_logger

logger = get_logger()


def migrate_data(state, migration):
    """Execute the data migration process."""

    try:

        dynamodb = boto3.resource("dynamodb")

        source_table = dynamodb.Table(migration["source_table"])

        target_table = dynamodb.Table(migration["target_table"])

    except NoCredentialsError:

        logger.error(
            "AWS credentials not found. Please configure your AWS credentials."
        )

        print("AWS credentials not found. Please configure your AWS credentials.")

        migration["status"] = "error"

        save_state(state)

        sys.exit(1)

    except ClientError as e:

        logger.error(f"Failed to connect to DynamoDB: {e}")

        print(f"Failed to connect to DynamoDB: {e}")

        migration["status"] = "error"

        save_state(state)

        sys.exit(1)

    except Exception as e:

        logger.error(f"Unexpected error: {e}")

        print(f"Unexpected error: {e}")

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

                new_item = {}

                # Check if we're in passthrough mode
                if migration["column_mappings"].get("__PASSTHROUGH__") == "true":

                    # Copy all attributes except those in exclude list
                    exclude_list = migration["column_mappings"].get("__EXCLUDE__", [])

                    for key, value in item.items():

                        if key not in exclude_list:

                            new_item[key] = value
                else:
                    # Original processing with specific mappings
                    for target_col, template in migration["column_mappings"].items():

                        try:

                            logger.info(
                                f"Processing mapping: {target_col} = {template} (type: {type(template)})"
                            )

                            # Check if template is a pure placeholder
                            if isinstance(template, str):

                                pure_placeholder_match = re.fullmatch(
                                    r"\{(\w+)\}", template
                                )

                                if pure_placeholder_match:

                                    placeholder = pure_placeholder_match.group(1)

                                    value = item.get(placeholder, None)

                                    new_item[target_col] = value

                                    logger.info(
                                        f"Pure placeholder {target_col}: {value}"
                                    )

                                    continue

                            # Handle direct values (numbers, booleans, etc.)
                            if isinstance(template, (int, float, bool)):

                                new_item[target_col] = template

                                logger.info(f"Direct value {target_col}: {template}")

                                continue

                            # Replace placeholders within the template string
                            result = apply_template(template, item)

                            new_item[target_col] = result

                            logger.info(f"Template result {target_col}: {result}")

                        except Exception as e:

                            logger.error(
                                f"Error processing column mapping {target_col}: {e}"
                            )

                            logger.error(f"Error traceback: {traceback.format_exc()}")

                            logger.error(
                                f"Template: {template} (type: {type(template)})"
                            )

                            logger.error(f"Item: {item}")

                            raise e

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

                # Append key to the DeleteRequest Key list
                undo_keys.append(key)

                if len(write_requests) == batch_size:

                    execute_batch_write(migration["target_table"], write_requests)

                    write_requests = []

                    total_items += batch_size

                    migration["processed_items"] = total_items

                    save_state(state)

                    print(f"Processed {total_items} items...")

            last_key = page.get("LastEvaluatedKey", None)

            if last_key:

                migration["last_evaluated_key"] = last_key

                save_state(state)

            else:

                migration["last_evaluated_key"] = None

        if write_requests:

            execute_batch_write(migration["target_table"], write_requests)

            total_items += len(write_requests)

            migration["processed_items"] = total_items

            migration["last_evaluated_key"] = None

            save_state(state)

            print(f"Processed {total_items} items...")

        migration["status"] = "completed"

        migration["processed_items"] = total_items

        migration["last_evaluated_key"] = None

        save_state(state)

        save_undo_state(undo_state)

        print(
            f"\nMigration completed successfully. Total items migrated: {total_items}"
        )

        logger.info(
            f"Migration '{mig_id}' completed successfully. Total items migrated: {total_items}"
        )

    except EndpointConnectionError as e:

        logger.error(f"Connection Error: {e}")

        print(f"Connection Error: {e}")

        migration["status"] = "error"

        save_state(state)

    except ClientError as e:

        logger.error(f"DynamoDB Client Error: {e}")

        print(f"DynamoDB Client Error: {e}")

        migration["status"] = "error"

        save_state(state)

    except Exception as e:

        logger.error(f"Migration Error: {e}")

        print(f"Migration Error: {e}")

        migration["status"] = "error"

        save_state(state)
