#!/bin/bash
# ============================================================
# MongoPLSQL-Bridge Test Environment Reset Script
# ============================================================
# This script completely resets the test environment by removing
# all containers, volumes, and reinitializing from scratch.
# ============================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  Resetting MongoPLSQL-Bridge Test Environment${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

cd "$PROJECT_ROOT"

# ------------------------------------------------------------
# Confirm reset
# ------------------------------------------------------------
echo -e "${YELLOW}WARNING: This will delete all data in the test databases!${NC}"
read -p "Are you sure you want to continue? (y/N) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Reset cancelled."
    exit 0
fi

echo ""

# ------------------------------------------------------------
# Stop and remove containers and volumes
# ------------------------------------------------------------
echo -e "${YELLOW}Stopping and removing containers and volumes...${NC}"
docker compose down -v --remove-orphans 2>/dev/null || true

# Remove any orphaned volumes
docker volume rm mongoplsql-bridge_oracle-data 2>/dev/null || true
docker volume rm mongoplsql-bridge_mongodb-data 2>/dev/null || true

echo -e "${GREEN}Cleanup complete${NC}"
echo ""

# ------------------------------------------------------------
# Start fresh environment
# ------------------------------------------------------------
echo -e "${YELLOW}Starting fresh environment...${NC}"
"$SCRIPT_DIR/start-env.sh"
