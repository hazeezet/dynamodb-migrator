# ddbm - DynamoDB Migration Tool

![License](https://img.shields.io/badge/license-Apache--2.0-green)
![Version](https://img.shields.io/github/v/release/hazeezet/dynamodb-migrator?label=CLI%20Version&color=blue)
![Build](https://img.shields.io/github/actions/workflow/status/hazeezet/dynamodb-migrator/release.yml?branch=main&label=Build)

A powerful, high-performance tool for migrating data between DynamoDB tables with advanced template transformations, atomic state management, and full rollback capabilities.

Available as both a **cross-platform Rust CLI** (optimized for speed) and a **Python implementation** (optimized for flexibility).

## Core Features

- **Interchangeable State** - The migration state (`migration_state.json`) and undo logs (`undo_state.json`) are fully compatible between the Python and Rust versions.
- **Atomic Rollback** - Every migration generates an undo log, allowing you to revert changes in the target table safely.
- **Resumable Jobs** - Interrupted migrations can be resumed from the exact last evaluated key.
- **Template Engine** - Powerful DSL for renaming columns and transforming data in-flight.

---

## Transformation Engine

Both implementations use the same template syntax for mapping source columns to target columns.

### Basic Syntax
Templates are defined as strings with placeholders: `"{column_name} {transformation}"`.

### Available Transformations

#### String Operations
| Operation | Example | Result |
|---|---|---|
| `upper` | `{name upper}` | `JOHN DOE` |
| `lower` | `{email lower}` | `john@example.com` |
| `title` | `{name title}` | `John Doe` |
| `strip` | `{text strip}` | Removes whitespace |
| `substring` | `{id substring 0 5}` | First 5 characters |
| `replace` | `{key replace _ -}` | Replaces `_` with `-` |

#### Number Operations
| Operation | Example | Result |
|---|---|---|
| `add` | `{count add 1}` | Increment value |
| `multiply` | `{price multiply 1.1}` | Add 10% tax |
| `round_to` | `{val round_to 2}` | `10.55` |
| `abs_value`| `{diff abs_value}` | Absolute value |

---

## Getting Started

### 1. Choose Your Implementation

| Feature | Rust CLI (ddbm) | Python Script |
|---|---|---|
| **Best For** | Production, large datasets | Fast hacking, custom logic |
| **Speed** | 🚀 Extremely Fast | Moderate |
| **Install** | Binary or Cargo | Python + Boto3 |
| **Commands** | `ddbm migrate` | `python run.py migrate` |

### 2. Get the Code

You can clone the entire repository or use **sparse checkout** to get only what you need:

```bash
git clone --filter=blob:none --sparse https://github.com/hazeezet/dynamodb-migrator.git
cd dynamodb-migrator

# To get only the Rust CLI:
git sparse-checkout set rust_cli

# To get only the Python app:
git sparse-checkout set python_app
```

### 3. Quick Links

- [**Rust CLI Documentation**](rust_cli/README.md) - How to install and run the binary.
- [**Python App Documentation**](python_app/README.md) - How to set up venv and run the script.

---

## Downloads

Pre-built binaries for Windows, Linux, and macOS are available in the [**Releases**](https://github.com/hazeezet/dynamodb-migrator/releases) section.

### Quick Install (Binary)

Download and install the latest version of `ddbm` for your system.

| OS | Architecture | Install Command (Copy & Paste) |
| :--- | :--- | :--- |
| **Linux** | x86_64 | `curl -L https://github.com/hazeezet/dynamodb-migrator/releases/latest/download/ddbm-linux-x86_64.zip -o ddbm.zip && unzip ddbm.zip -d ddbm-cli && sudo ./ddbm-cli/install` |
| **Linux** | ARM64 | `curl -L https://github.com/hazeezet/dynamodb-migrator/releases/latest/download/ddbm-linux-arm64.zip -o ddbm.zip && unzip ddbm.zip -d ddbm-cli && sudo ./ddbm-cli/install` |
| **macOS** | Intel | `curl -L https://github.com/hazeezet/dynamodb-migrator/releases/latest/download/ddbm-macos-x86_64.zip -o ddbm.zip && unzip ddbm.zip -d ddbm-cli && sudo ./ddbm-cli/install` |
| **macOS** | Apple Silicon | `curl -L https://github.com/hazeezet/dynamodb-migrator/releases/latest/download/ddbm-macos-arm64.zip -o ddbm.zip && unzip ddbm.zip -d ddbm-cli && sudo ./ddbm-cli/install` |

**Windows**
1. Download the latest `ddbm-windows-x86_64.msi` from the [**Releases**](https://github.com/hazeezet/dynamodb-migrator/releases) page.
2. Run the `.msi` file and follow the on-screen instructions.
3. Open a new terminal and verify with `ddbm --version`.

---

## Uninstallation

### Linux & macOS
If you installed via the binary script:
```bash
sudo ./ddbm-cli/uninstall
```

### Windows
1. Open **Settings** > **Apps** > **Installed Apps**.
2. Search for **ddbm**.
3. Click **Uninstall**.

### Cargo
If you installed via Cargo:
```bash
cargo uninstall ddbm
```

---

Simple, reliable DynamoDB table migration. 🚀