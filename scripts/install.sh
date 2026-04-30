#!/bin/bash
set -e

# ddbm Installation Script
# This script moves the ddbm binaries to /usr/local/bin

INSTALL_DIR="/usr/local/bin"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check if binaries exist in the script's directory
if [[ ! -f "$SCRIPT_DIR/ddbm" ]] || [[ ! -f "$SCRIPT_DIR/dynamodb-migrator" ]]; then
    echo "Error: ddbm or dynamodb-migrator binary not found in $SCRIPT_DIR."
    exit 1
fi

BINARY="$SCRIPT_DIR/ddbm"
ALIAS="$SCRIPT_DIR/dynamodb-migrator"

# Move binaries
echo -e "${BLUE}==>${NC} Moving binaries to ${INSTALL_DIR} (requires sudo)..."
sudo cp "$BINARY" "$INSTALL_DIR/ddbm"
sudo cp "$ALIAS" "$INSTALL_DIR/dynamodb-migrator"

# Set permissions
sudo chmod +x "$INSTALL_DIR/ddbm"
sudo chmod +x "$INSTALL_DIR/dynamodb-migrator"

echo -e "${GREEN}==>${NC} ddbm installed successfully!"
echo -e "${BLUE}==>${NC} Run 'ddbm --version' to verify."
