# Python Implementation Core

This directory contains the internal logic for the Python version of the migration tool.

## Modules

- `migration_engine.py`: The scan-transform-write loop.
- `state_manager.py`: JSON-based state persistence.
- `user_interface.py`: CLI prompts and feedback.
- `undo_operations.py`: Rollback logic.
- `utils/`: Support modules for logging and data conversion.
- `transform/`: Template processing and transformation functions.
