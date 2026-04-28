import boto3
import sys
from botocore.exceptions import ClientError, NoCredentialsError
from .utils.logger import get_logger

logger = get_logger()


def check_and_create_target_table(source_table_name, target_table_name):
    """Check if target table exists and offer to create it with source table's settings if it doesn't."""

    try:

        dynamodb = boto3.resource("dynamodb")

        client = dynamodb.meta.client

        # Get source table description
        source_description = client.describe_table(TableName=source_table_name)["Table"]

        # Ask for confirmation
        print(f"\nTarget table '{target_table_name}' does not exist.")

        confirm = (
            input(
                "Do you want to create it with the same settings as the source table? (yes/no): "
            )
            .strip()
            .lower()
        )

        if confirm != "yes":
            return False

        # Check billing mode FIRST - before working with GSIs
        billing_mode = "PROVISIONED"

        provisioned_throughput = {
            "ReadCapacityUnits": source_description.get(
                "ProvisionedThroughput", {}
            ).get("ReadCapacityUnits", 5),
            "WriteCapacityUnits": source_description.get(
                "ProvisionedThroughput", {}
            ).get("WriteCapacityUnits", 5),
        }

        if (
            source_description.get("BillingModeSummary", {}).get("BillingMode")
            == "PAY_PER_REQUEST"
        ):

            billing_mode = "PAY_PER_REQUEST"

        # Extract table configuration
        key_schema = source_description["KeySchema"]

        # Collect all attribute names used in any key schema (primary + GSIs)
        key_attributes = set()

        # Add primary key attributes
        for key in key_schema:
            key_attributes.add(key["AttributeName"])

        # Prepare GSI configurations if present
        gsi_configs = []

        if "GlobalSecondaryIndexes" in source_description:

            for gsi in source_description["GlobalSecondaryIndexes"]:

                gsi_config = {
                    "IndexName": gsi["IndexName"],
                    "KeySchema": gsi["KeySchema"],
                    "Projection": gsi["Projection"],
                }

                # Add GSI key attributes to our set
                for key in gsi["KeySchema"]:

                    key_attributes.add(key["AttributeName"])

                # Add provisioning info for GSI ONLY if table is in PROVISIONED mode
                if billing_mode == "PROVISIONED":

                    read_capacity = max(
                        1,
                        gsi.get("ProvisionedThroughput", {}).get(
                            "ReadCapacityUnits", 1
                        ),
                    )

                    write_capacity = max(
                        1,
                        gsi.get("ProvisionedThroughput", {}).get(
                            "WriteCapacityUnits", 1
                        ),
                    )

                    gsi_config["ProvisionedThroughput"] = {
                        "ReadCapacityUnits": read_capacity,
                        "WriteCapacityUnits": write_capacity,
                    }

                gsi_configs.append(gsi_config)

        # Filter attribute definitions to include only the attributes used in keys
        attribute_definitions = []

        for attr_def in source_description["AttributeDefinitions"]:

            if attr_def["AttributeName"] in key_attributes:

                attribute_definitions.append(attr_def)

        # Create table parameters
        create_params = {
            "TableName": target_table_name,
            "KeySchema": key_schema,
            "AttributeDefinitions": attribute_definitions,
            "BillingMode": billing_mode,
        }

        # Add provisioned throughput if needed
        if billing_mode == "PROVISIONED":

            create_params["ProvisionedThroughput"] = provisioned_throughput

        # Add GSIs if we have any
        if gsi_configs:

            create_params["GlobalSecondaryIndexes"] = gsi_configs

            print(
                f"Copying {len(gsi_configs)} Global Secondary Indexes from source table"
            )

        # Create the table
        print(f"Creating target table '{target_table_name}'...")

        client.create_table(**create_params)

        # Wait for table to be created
        print("Waiting for table to be created (this may take a few minutes)...")

        waiter = client.get_waiter("table_exists")

        waiter.wait(TableName=target_table_name)

        print(f"Target table '{target_table_name}' created successfully.")

        return True

    except ClientError as e:

        logger.error(f"Error creating target table: {e}")

        print(f"Error creating target table: {e}")

        return False


def get_table_key_schema(target_table, source_table):
    """Get key schema for a table, creating target table if it doesn't exist."""

    try:
        table_description = target_table.meta.client.describe_table(
            TableName=target_table.name
        )

        key_schema = table_description["Table"]["KeySchema"]

        keys = {}

        for key in key_schema:

            keys[key["KeyType"]] = key["AttributeName"]

        return keys

    except ClientError as e:

        if e.response["Error"]["Code"] == "ResourceNotFoundException":

            if check_and_create_target_table(source_table.name, target_table.name):

                # Retry after creating the table
                return get_table_key_schema(target_table, source_table)

            else:

                logger.error("Target table doesn't exist and wasn't created")

                print("Migration cannot proceed as target table doesn't exist")

                sys.exit(1)

        else:

            logger.error(f"Error fetching key schema: {e}")

            print(f"Error fetching key schema: {e}")

            sys.exit(1)

    except Exception as e:

        logger.error(f"Unexpected error fetching key schema: {e}")

        print(f"Unexpected error fetching key schema: {e}")

        sys.exit(1)


def execute_batch_write(table_name, write_requests):
    """Execute batch write operations to DynamoDB."""

    dynamodb = boto3.client("dynamodb")

    try:

        response = dynamodb.batch_write_item(RequestItems={table_name: write_requests})

        unprocessed = response.get("UnprocessedItems", {})

        if unprocessed.get(table_name):

            logger.warning(f"Unprocessed items detected. Retrying...")

            execute_batch_write(table_name, unprocessed[table_name])

    except ClientError as e:

        logger.error(f"Batch Write Error: {e}")

        print(f"Batch Write Error: {e}")

    except Exception as e:

        logger.error(f"Unexpected Batch Write Error: {e}")

        print(f"Unexpected Batch Write Error: {e}")
