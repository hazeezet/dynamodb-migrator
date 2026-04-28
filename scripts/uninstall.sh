#!/bin/bash
set -e

# ddbm Uninstallation Script
INSTALL_DIR="/usr/local/bin"

# Colors for output
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}==>${NC} Uninstalling ddbm..."

# Remove binaries
echo -e "${BLUE}==>${NC} Removing binaries from ${INSTALL_DIR} (requires sudo)..."
sudo rm -f "$INSTALL_DIR/ddbm"
sudo rm -f "$INSTALL_DIR/dynamodb-migrator"

echo -e "${RED}==>${NC} ddbm has been uninstalled."
