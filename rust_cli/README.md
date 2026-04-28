# ddbm - Rust CLI

The high-performance, compiled CLI implementation of the DynamoDB migration tool. Built with Rust for maximum throughput and safety.

## Installation

### 1. From Source
Requires the [Rust toolchain](https://rustup.rs/) installed.
```bash
cargo build --release
# The binary will be at
# - ./target/release/ddbm
# - ./target/release/dynamodb-migrator
```

### 2. Install to PATH
Installs both `ddbm` and `dynamodb-migrator` binaries to your system:
```bash
cargo install --path .
```

### 3. Uninstalling
This will remove both `ddbm` and `dynamodb-migrator` binaries:
```bash
cargo uninstall ddbm
cargo uninstall dynamodb-migrator
```

### 4. Cleaning Build Files
To remove the local `target/` directory and build artifacts:
```bash
cargo clean
```

## Usage

The CLI is invoked via the `ddbm` command.

### Subcommands
- `ddbm migrate` - Start a new migration. Supports `--passthrough` or `--mappings <FILE>`.
- `ddbm resume` - Resume a migration by ID or interactive selection.
- `ddbm undo` - Rollback a migration to revert changes in the target table.
- `ddbm list` - List all migration jobs in the current state directory.

### Examples
```bash
# Interactive migration
ddbm migrate

# Non-interactive clone
ddbm ddbm migrate -s source-table -t target-table --passthrough
```

## Reference
- **Transformations**: For details on mapping syntax, see the [Root README](../README.md#the-transformation-engine).
- **Internal Structure**: For documentation on the Rust modules, see [app/README.md](app/README.md).

## Performance Features
- **Async I/O**: Powered by `tokio` for efficient concurrent scanning and writing.
- **Zero-Cost Abstractions**: Minimal overhead during data transformation.
- **Cross-Platform**: Binaries are automatically built for Linux, macOS, and Windows via GitHub Actions.
