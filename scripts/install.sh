#!/bin/bash
set -e

# ddbm Installation Script
# This script moves the ddbm binaries to /usr/local/bin

INSTALL_DIR="/usr/local/bin"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==>${NC} Installing ddbm..."

# Check if binaries exist in the current directory
if [[ ! -f "ddbm" ]] && [[ ! -f "ddbm.exe" ]]; then
    echo "Error: ddbm binary not found in current directory."
    exit 1
fi

# Determine binary names (handling .exe for Windows-style extraction if needed, though this is bash)
BINARY="ddbm"
ALIAS="dynamodb-migrator"

if [[ -f "ddbm.exe" ]]; then
    BINARY="ddbm.exe"
    ALIAS="dynamodb-migrator.exe"
fi

# Move binaries
echo -e "${BLUE}==>${NC} Moving binaries to ${INSTALL_DIR} (requires sudo)..."
sudo cp "$BINARY" "$INSTALL_DIR/ddbm"
sudo cp "$ALIAS" "$INSTALL_DIR/dynamodb-migrator"

# Set permissions
sudo chmod +x "$INSTALL_DIR/ddbm"
sudo chmod +x "$INSTALL_DIR/dynamodb-migrator"

echo -e "${GREEN}==>${NC} ddbm installed successfully!"
echo -e "${BLUE}==>${NC} Run 'ddbm --version' to verify."
