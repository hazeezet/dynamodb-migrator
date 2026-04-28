# Application Core

This directory contains the primary modules for the DynamoDB Migration CLI.

## Modules

- `main.rs`: Entry point and CLI configuration.
- `commands/`: Implementation of CLI subcommands (migrate, resume, undo, list).
- `aws/`: AWS SDK wrappers and DynamoDB operations.
- `engine/`: Core logic for migration and rollback loops.
- `transform/`: Data transformation and template processing engine.
- `state/`: Persistence layer for migration and undo states.
- `ui/`: Terminal UI components and interaction logic.
