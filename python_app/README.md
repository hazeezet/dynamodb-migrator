# ddbm - Python Implementation

This directory contains the Python-based implementation of the DynamoDB migration tool. It is designed for simplicity and ease of modification.

## Installation

### 1. Requirements
- Python 3.7+
- AWS Credentials configured (`aws configure`)

### 2. Setup Virtual Environment (Recommended)
```bash
# Linux/macOS
python3 -m venv venv
source venv/bin/activate

# Windows
python -m venv venv
.\venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install boto3
```

## Usage

The Python version follows the same subcommand structure as the Rust CLI.

### Subcommands
- `python run.py migrate` - Start a new migration (Interactive or Non-interactive).
- `python run.py resume` - Continue an interrupted job.
- `python run.py undo` - Rollback a completed migration.
- `python run.py list` - View all migration status.

### Non-Interactive Example
```bash
python run.py migrate --source UsersOld --target UsersNew --passthrough
```

## Testing

We use `pytest` for automated testing.
```bash
# Ensure venv is active
python -m pytest tests/
```

---

## Reference
- **Transformations**: For details on the `{placeholder}` syntax and available functions, see the [Root README](../README.md#transformation-engine).
- **Core Logic**: For internal module documentation, see [core/README.md](core/README.md).

## Implementation Details
This version uses the `boto3` library and handles data in memory batches of 25 items to satisfy DynamoDB `BatchWriteItem` limits. State is persisted in `migration_state.json`.
