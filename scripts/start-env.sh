#!/bin/bash
# ============================================================
# MongoPLSQL-Bridge Test Environment Startup Script
# ============================================================
# This script starts both MongoDB and Oracle containers and
# waits for them to be healthy before returning.
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
echo -e "${BLUE}  Starting MongoPLSQL-Bridge Test Environment${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

cd "$PROJECT_ROOT"

# ------------------------------------------------------------
# Start containers
# ------------------------------------------------------------
echo -e "${YELLOW}Starting Docker containers...${NC}"
docker compose up -d oracle mongodb

echo ""
echo -e "${YELLOW}Waiting for containers to be healthy...${NC}"
echo ""

# ------------------------------------------------------------
# Wait for MongoDB to be healthy
# ------------------------------------------------------------
echo -n "  MongoDB: "
MONGODB_READY=false
for i in {1..30}; do
    if docker exec mongo-translator-mongodb mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
        MONGODB_READY=true
        echo -e "${GREEN}ready${NC}"
        break
    fi
    echo -n "."
    sleep 2
done

if [ "$MONGODB_READY" = false ]; then
    echo -e "${RED}timeout${NC}"
    echo -e "${RED}MongoDB failed to start within 60 seconds${NC}"
    exit 1
fi

# ------------------------------------------------------------
# Wait for Oracle to be healthy
# ------------------------------------------------------------
echo -n "  Oracle:  "
ORACLE_READY=false
for i in {1..60}; do
    if docker exec mongo-translator-oracle sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 <<< "SELECT 1 FROM DUAL; EXIT;" > /dev/null 2>&1; then
        ORACLE_READY=true
        echo -e "${GREEN}ready${NC}"
        break
    fi
    echo -n "."
    sleep 3
done

if [ "$ORACLE_READY" = false ]; then
    echo -e "${RED}timeout${NC}"
    echo -e "${RED}Oracle failed to start within 180 seconds${NC}"
    echo -e "${YELLOW}Check logs with: docker-compose logs oracle${NC}"
    exit 1
fi

echo ""

# ------------------------------------------------------------
# Run validation
# ------------------------------------------------------------
echo -e "${YELLOW}Running validation...${NC}"
echo ""
"$SCRIPT_DIR/validate-env.sh"
