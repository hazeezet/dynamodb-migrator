import boto3
from botocore.exceptions import ClientError
from .state_manager import load_undo_state, save_undo_state, save_state
from .dynamodb_operations import execute_batch_write
from .utils.logger import get_logger

logger = get_logger()


def undo_last_migration(state):
    """Undo a previously completed migration."""

    undo_state = load_undo_state()

    print("\n=== Undo Migration ===")

    if not undo_state["undo_migrations"]:

        print("No undo information available.")

        return

    print("\n=== Available Migrations to Undo ===")

    migration_ids = list(undo_state["undo_migrations"].keys())

    for idx, mig_id in enumerate(migration_ids, start=1):

        print(f"{idx}. ID: {mig_id}", "\n\n")

    while True:

        choice = input(
            "Select a migration to undo by number (or 'cancel' to exit): "
        ).strip()

        if choice.lower() == "cancel":

            return

        if not choice.isdigit() or not (1 <= int(choice) <= len(migration_ids)):

            print("Invalid selection. Please try again.")

            continue

        selected_mig_id = migration_ids[int(choice) - 1]

        break

    migration = next(
        (m for m in state["migrations"] if m["id"] == selected_mig_id), None
    )

    if not migration:

        print("Migration not found in state.")

        return

    target_table_name = migration["target_table"]

    key_schema = migration.get("key_schema")

    if not key_schema:

        print("Key schema information missing. Cannot perform undo.")

        return

    try:

        dynamodb = boto3.client("dynamodb")

    except Exception as e:

        logger.error(f"Error initializing DynamoDB client: {e}")

        print(f"Error initializing DynamoDB client: {e}")

        return

    undo_data = undo_state["undo_migrations"].get(selected_mig_id, {})

    delete_request = undo_data.get("DeleteRequest", {})

    undo_keys = delete_request.get("Key", [])

    if not undo_keys:

        print("No undo items found for this migration.")

        return

    try:

        print("\nStarting undo operation...")

        batch_size = 25

        total_undo = len(undo_keys)

        processed_undo = 0

        for i in range(0, total_undo, batch_size):

            batch_keys = undo_keys[i : i + batch_size]

            write_requests = []

            for key in batch_keys:

                write_requests.append({"DeleteRequest": {"Key": key}})

            response = dynamodb.batch_write_item(
                RequestItems={target_table_name: write_requests}
            )

            unprocessed = response.get("UnprocessedItems", {})

            if unprocessed.get(target_table_name):

                logger.warning(f"Unprocessed items detected during undo. Retrying...")

                retry_write_requests = unprocessed[target_table_name]

                execute_batch_write(target_table_name, retry_write_requests)

            processed_undo += len(write_requests)

            print(f"Undid {processed_undo}/{total_undo} items...")

        print("\nUndo operation completed successfully.")

        logger.info(
            f"Undo operation for migration '{selected_mig_id}' completed successfully."
        )

        undo_state["undo_migrations"].pop(selected_mig_id, None)

        save_undo_state(undo_state)

        # Update migration status to 'undone'
        migration_status = next(
            (m for m in state["migrations"] if m["id"] == selected_mig_id), None
        )

        if migration_status and migration_status["status"] == "completed":

            migration_status["status"] = "undone"

            migration_status["processed_items"] = 0

            save_state(state)

            print(f"Migration '{selected_mig_id}' status updated to 'undone'.")

    except ClientError as e:

        logger.error(f"Undo Error: {e}")

        print(f"Undo Error: {e}")

    except Exception as e:

        logger.error(f"Unexpected Undo Error: {e}")

        print(f"Unexpected Undo Error: {e}")
