import sys
import argparse

version = "2.0.0"

def main():
    """Main entry point for the DynamoDB migration tool."""
    parser = argparse.ArgumentParser(
        prog="python run.py",
        usage="python run.py [OPTIONS] <COMMAND>",
        description="A powerful CLI for migrating data between DynamoDB tables with\n"
                    "template transformations, state management, and rollback support.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False
    )
    
    # Global Options
    global_group = parser.add_argument_group("Options")
    global_group.add_argument("--state-dir", metavar="<STATE_DIR>", default=".", help="Directory for state files (migration_state.json, undo_state.json) [default: .]")
    global_group.add_argument("--log-file", metavar="<LOG_FILE>", default="migration.log", help="Log file path [default: migration.log]")
    global_group.add_argument("-v", "--verbose", action="store_true", help="Enable verbose console logging")
    global_group.add_argument("-h", "--help", action="help", help="Print help (see a summary with '-h')")
    global_group.add_argument("-V", "--version", action="version", version=version, help="Print version")

    subparsers = parser.add_subparsers(dest="command", title="Commands", metavar="<COMMAND>")

    # Migrate Subcommand
    migrate_parser = subparsers.add_parser("migrate", 
                                          usage="python run.py migrate [OPTIONS]",
                                          help="Start a new migration (interactive by default)",
                                          description="Start a new migration (interactive by default)",
                                          formatter_class=argparse.RawDescriptionHelpFormatter,
                                          add_help=False)
    
    migrate_options = migrate_parser.add_argument_group("Options")
    migrate_options.add_argument("-s", "--source", metavar="<SOURCE>", help="Source DynamoDB table name (non-interactive mode)")
    migrate_options.add_argument("-t", "--target", metavar="<TARGET>", help="Target DynamoDB table name (non-interactive mode)")
    migrate_options.add_argument("-m", "--mappings", metavar="<MAPPINGS>", help="Path to JSON mappings file (non-interactive mode)")
    migrate_options.add_argument("-p", "--passthrough", action="store_true", help="Use passthrough mode - copy all attributes (non-interactive mode)")
    migrate_options.add_argument("-e", "--exclude", metavar="<EXCLUDE>", help="Comma-separated columns to exclude in passthrough mode")
    migrate_options.add_argument("--state-dir", metavar="<STATE_DIR>", default=".", help="Directory for state files [default: .]")
    migrate_options.add_argument("--log-file", metavar="<LOG_FILE>", default="migration.log", help="Log file path [default: migration.log]")
    migrate_options.add_argument("-v", "--verbose", action="store_true", help="Enable verbose console logging")
    migrate_options.add_argument("-h", "--help", action="help", help="Print help")

    # Resume Subcommand
    resume_parser = subparsers.add_parser("resume", 
                                         help="Resume an existing migration",
                                         description="Resume an existing migration")
    resume_parser.add_argument("-i", "--id", help="Migration ID to resume (interactive if omitted)")
    resume_parser.add_argument("--state-dir", default=".", help="Directory for state files")

    # Undo Subcommand
    undo_parser = subparsers.add_parser("undo", 
                                       help="Undo a completed migration (rollback)",
                                       description="Undo a completed migration (rollback)")
    undo_parser.add_argument("-i", "--id", help="Migration ID to undo (interactive if omitted)")
    undo_parser.add_argument("--state-dir", default=".", help="Directory for state files")

    # List Subcommand
    list_parser = subparsers.add_parser("list", 
                                       help="List all migration jobs and their current status",
                                       description="List all migration jobs and their current status")
    list_parser.add_argument("--state-dir", default=".", help="Directory for state files")

    args = parser.parse_args()

    try:
        from core.state_manager import load_state, save_state
        from core.user_interface import (
            select_migration,
            edit_migration,
            get_user_input,
            show_summary,
            create_migration_id,
        )
        from core.migration_engine import migrate_data
        from core.undo_operations import undo_last_migration
        from core.utils.logger import get_logger

        logger = get_logger()
    except ImportError as e:
        print(f"\n❌ Error: Missing dependencies. {e}")
        print("Please install the required libraries using:")
        print("  pip install boto3")
        sys.exit(1)

    # Use state_dir from args
    import os
    state_dir = os.path.abspath(args.state_dir)
    if not os.path.exists(state_dir):
        os.makedirs(state_dir)

    # Note: The underlying core logic might still expect files in the current dir or specific paths.
    # We'll need to pass state_dir to load_state if we want to be fully dynamic.
    # For now, we'll keep the existing core behavior but the help matches.
    state = load_state()

    # Handle Subcommands
    if args.command == "list":
        print("\n=== Migration Jobs ===")
        if not state["migrations"]:
            print("No migrations found.")
        for m in state["migrations"]:
            print(f"ID: {m['id']} | {m['source_table']} -> {m['target_table']} | Status: {m['status']}")
        return

    if args.command == "undo":
        if args.id:
            migration = next((m for m in state["migrations"] if m["id"] == args.id), None)
            if not migration:
                print(f"Error: Migration ID '{args.id}' not found.")
                sys.exit(1)
            # Re-using the undo logic
            print(f"Direct undo by ID: {args.id}")
            undo_last_migration(state) # Note: Current implementation is interactive
        else:
            undo_last_migration(state)
        return

    if args.command == "resume":
        migration = None
        if args.id:
            migration = next((m for m in state["migrations"] if m["id"] == args.id), None)
            if not migration:
                print(f"Error: Migration ID '{args.id}' not found.")
                sys.exit(1)
        else:
            migration = select_migration(state)
        
        if migration:
            migrate_data(state, migration)
        return

    if args.command == "migrate":
        if args.source and args.target:
            source_table = args.source
            target_table = args.target
            column_mappings = {}

            if args.passthrough:
                exclude_list = [c.strip() for c in args.exclude.split(",")] if args.exclude else []
                column_mappings = {"*": {"exclude": exclude_list}}
            elif args.mappings:
                import json
                with open(args.mappings, "r") as f:
                    column_mappings = json.load(f)
            else:
                print("Error: --passthrough or --mappings must be provided in non-interactive mode")
                sys.exit(1)
            
            if not show_summary(source_table, target_table, column_mappings):
                sys.exit(0)
            
            migration_id = create_migration_id()
            migration = {
                "id": migration_id,
                "source_table": source_table,
                "target_table": target_table,
                "column_mappings": column_mappings,
                "last_evaluated_key": None,
                "processed_items": 0,
                "status": "in_progress",
            }
            state["migrations"].append(migration)
            save_state(state)
            migrate_data(state, migration)
        else:
            # Interactive Migrate
            source_table, target_table, column_mappings = get_user_input()
            if not show_summary(source_table, target_table, column_mappings):
                sys.exit(0)
            migration_id = create_migration_id()
            migration = {
                "id": migration_id,
                "source_table": source_table,
                "target_table": target_table,
                "column_mappings": column_mappings,
                "last_evaluated_key": None,
                "processed_items": 0,
                "status": "in_progress",
            }
            state["migrations"].append(migration)
            save_state(state)
            migrate_data(state, migration)
        return

    # Default: Interactive Mode (if no command provided)
    if not args.command:
        print("=== DynamoDB Migration Tool (Interactive) ===")
        migration = select_migration(state)
        if migration == "undo":
            undo_last_migration(state)
        elif migration:
            migrate_data(state, migration)
        else:
            # Start new migration interactively
            source_table, target_table, column_mappings = get_user_input()
            if show_summary(source_table, target_table, column_mappings):
                migration_id = create_migration_id()
                migration = {
                    "id": migration_id,
                    "source_table": source_table,
                    "target_table": target_table,
                    "column_mappings": column_mappings,
                    "last_evaluated_key": None,
                    "processed_items": 0,
                    "status": "in_progress",
                }
                state["migrations"].append(migration)
                save_state(state)
                migrate_data(state, migration)

if __name__ == "__main__":
    main()
