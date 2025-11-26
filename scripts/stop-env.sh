#!/bin/bash
# ============================================================
# MongoPLSQL-Bridge Test Environment Shutdown Script
# ============================================================

set -e

# Colors for output
BLUE='\033[0;34m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Get the project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  Stopping MongoPLSQL-Bridge Test Environment${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

cd "$PROJECT_ROOT"

docker compose down

echo ""
echo -e "${GREEN}Test environment stopped${NC}"
echo ""
echo "Note: Data volumes are preserved. To remove all data, run:"
echo "  docker compose down -v"
