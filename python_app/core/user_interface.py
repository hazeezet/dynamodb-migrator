import sys
from datetime import datetime


def list_migrations(state):
    """Display list of existing migration jobs."""

    if not state["migrations"]:

        print("No existing migration jobs found.")

        return

    print("\n=== Existing Migration Jobs ===")

    for idx, mig in enumerate(state["migrations"], start=1):

        print(
            f"{idx}. ID: {mig['id']} | Source: {mig['source_table']} | Target: {mig['target_table']} | Status: {mig['status']}",
            "\n\n",
        )


def select_migration(state):
    """Allow user to select an existing migration or create new one."""

    if not state["migrations"]:

        return None

    list_migrations(state)

    while True:

        choice = input(
            "Select a migration job by number (or 'new' to create a new migration, 'undo' to undo last migration): "
        ).strip()

        if choice.lower() == "new":

            return None

        if choice.lower() == "undo":

            return "undo"

        if not choice.isdigit() or not (1 <= int(choice) <= len(state["migrations"])):

            print("Invalid selection. Please try again.")

            continue

        return state["migrations"][int(choice) - 1]


def edit_migration(migration):
    """Edit an existing migration job."""

    print("\n--- Edit Migration Job ---")

    print("Press Enter to keep the current value.\n")

    new_source = input(f"Source Table [{migration['source_table']}]: ").strip()

    if new_source:

        migration["source_table"] = new_source

    new_target = input(f"Target Table [{migration['target_table']}]: ").strip()

    if new_target:

        migration["target_table"] = new_target

    print("\n--- Edit Column Mappings ---")

    if migration["column_mappings"]:

        for target, template in migration["column_mappings"].items():

            new_template = input(f"Mapping for '{target}' [{template}]: ").strip()

            if new_template:

                migration["column_mappings"][target] = new_template

    else:

        print("No existing column mappings.")

    while True:

        add_mapping = (
            input("Do you want to add a new column mapping? (yes/no): ").strip().lower()
        )

        if add_mapping == "yes":

            mapping = input(
                "Enter column mapping (format: target_column=template): "
            ).strip()

            if "=" not in mapping:

                print("Invalid format. Please use 'target_column=template'.")

                continue

            target_column, template = mapping.split("=", 1)

            target_column = target_column.strip()

            template = template.strip()

            if target_column and template:

                migration["column_mappings"][target_column] = template

                print(f"Added mapping: {target_column} = {template}")

            else:

                print("Invalid mapping. Both target column and template are required.")

        elif add_mapping == "no":

            break

        else:

            print("Please enter 'yes' or 'no'.")

    print("\nMigration job updated successfully.\n")


def get_user_input():
    """Get migration configuration from user input."""

    try:

        source_table_name = input("Enter the source table name: ").strip()

        target_table_name = input("Enter the target table name: ").strip()

        if not source_table_name or not target_table_name:

            raise ValueError("Table names cannot be empty.")

        print("\n--- Column Mappings ---")

        # Add passthrough option
        mapping_type = input(
            "Do you want to: \n"
            "1. Copy all attributes directly (passthrough mode)\n"
            "2. Define specific column mappings\n"
            "Enter choice (1/2): "
        ).strip()

        column_mappings = {}

        if mapping_type == "1":

            # Passthrough mode - use a special marker in the mappings

            column_mappings["__PASSTHROUGH__"] = "true"

            print(
                "\nPassthrough mode selected. All source attributes will be copied to the target table."
            )

            # Allow for excluding certain columns
            exclude_columns = input(
                "\nOptionally, enter column names to exclude (comma-separated, or leave empty): "
            ).strip()

            if exclude_columns:

                exclude_list = [
                    col.strip() for col in exclude_columns.split(",") if col.strip()
                ]

                column_mappings["__EXCLUDE__"] = exclude_list

                print(
                    f"The following columns will be excluded: {', '.join(exclude_list)}"
                )

        elif mapping_type == "2":

            print("\nDefine your column mappings using the format:")

            print("target_column=prefix{source_column1}middle{source_column2}suffix")

            print(
                "Use '{}' to insert source column values. You can add prefixes and suffixes as needed."
            )

            print("Enter 'done' when finished.\n")

            while True:

                mapping = input("Enter column mapping (or 'done' to finish): ").strip()

                if mapping.lower() == "done":
                    break

                if "=" not in mapping:

                    print("Invalid format. Please use 'target_column=template'.")

                    continue

                target_column, template = mapping.split("=", 1)

                target_column = target_column.strip()

                template = template.strip()

                if not target_column or not template:

                    print(
                        "Invalid mapping. Target column and template cannot be empty."
                    )

                    continue

                column_mappings[target_column] = template

        else:

            print("Invalid choice. Defaulting to specific column mappings.")

            print("\nDefine your column mappings using the format:")

            print("target_column=prefix{source_column1}middle{source_column2}suffix")

            print(
                "Use '{}' to insert source column values. You can add prefixes and suffixes as needed."
            )

            print("Enter 'done' when finished.\n")

            while True:

                mapping = input("Enter column mapping (or 'done' to finish): ").strip()

                if mapping.lower() == "done":

                    break

                if "=" not in mapping:

                    print("Invalid format. Please use 'target_column=template'.")

                    continue

                target_column, template = mapping.split("=", 1)

                target_column = target_column.strip()

                template = template.strip()

                if not target_column or not template:

                    print(
                        "Invalid mapping. Target column and template cannot be empty."
                    )

                    continue

                column_mappings[target_column] = template

        if not column_mappings:

            raise ValueError("You must define at least one column mapping.")

        return source_table_name, target_table_name, column_mappings

    except ValueError as ve:

        from .utils.logger import get_logger

        logger = get_logger()

        logger.error(f"Input Error: {ve}")

        print(f"Input Error: {ve}")

        sys.exit(1)


def show_summary(source_table, target_table, column_mappings):
    """Display migration summary and get user confirmation."""

    try:

        print("\n=== Migration Summary ===")

        print(f"Source Table: {source_table}")

        print(f"Target Table: {target_table}\n")

        if column_mappings:

            print("Column Mappings:")

            for target, template in column_mappings.items():

                print(f"  {target} = {template}")

        confirm = (
            input("\nDo you want to proceed with the migration? (yes/no): ")
            .strip()
            .lower()
        )

        return confirm == "yes"

    except Exception as e:

        from .utils.logger import get_logger

        logger = get_logger()

        logger.error(f"Summary Error: {e}")

        print(f"Summary Error: {e}")

        sys.exit(1)


def create_migration_id():
    """Create a unique migration ID."""
    return f"migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
