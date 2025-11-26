#!/bin/bash
# ============================================================
# MongoPLSQL-Bridge Query Tests - Environment Teardown
# ============================================================
# Cleans up test data from both MongoDB and Oracle databases.
# ============================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY_TESTS_DIR="$(dirname "$SCRIPT_DIR")"

# Parse arguments
KEEP_RESULTS=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-results)
            KEEP_RESULTS=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --keep-results    Don't delete test results"
            echo "  --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  MongoPLSQL-Bridge Query Tests - Teardown${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# Check if containers are running
MONGO_RUNNING=false
ORACLE_RUNNING=false

if docker ps --format '{{.Names}}' | grep -q 'mongo-translator-mongodb'; then
    MONGO_RUNNING=true
fi

if docker ps --format '{{.Names}}' | grep -q 'mongo-translator-oracle'; then
    ORACLE_RUNNING=true
fi

# Clean MongoDB test collections
if [ "$MONGO_RUNNING" = true ]; then
    echo -e "${YELLOW}Cleaning MongoDB test collections...${NC}"

    docker exec mongo-translator-mongodb mongosh --quiet testdb \
        -u admin -p admin123 --authenticationDatabase admin \
        --eval "
            db.sales.drop();
            db.employees.drop();
            db.products.drop();
            print('Collections dropped');
        " 2>/dev/null || true

    echo -e "  ${GREEN}✓${NC} MongoDB collections dropped"
else
    echo -e "  ${YELLOW}⚠${NC} MongoDB container not running, skipping cleanup"
fi

# Clean Oracle test tables
if [ "$ORACLE_RUNNING" = true ]; then
    echo -e "${YELLOW}Cleaning Oracle test tables...${NC}"

    docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOSQL'
SET FEEDBACK OFF
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE employees CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/
EXIT;
EOSQL" 2>/dev/null || true

    echo -e "  ${GREEN}✓${NC} Oracle tables dropped"
else
    echo -e "  ${YELLOW}⚠${NC} Oracle container not running, skipping cleanup"
fi

# Clean results directory
if [ "$KEEP_RESULTS" = false ]; then
    echo -e "${YELLOW}Cleaning test results...${NC}"

    rm -rf "$QUERY_TESTS_DIR/results/"*.txt
    rm -rf "$QUERY_TESTS_DIR/results/"*.json

    echo -e "  ${GREEN}✓${NC} Test results cleaned"
else
    echo -e "  ${YELLOW}⚠${NC} Keeping test results (--keep-results)"
fi

echo ""
echo -e "${BLUE}============================================================${NC}"
echo -e "${GREEN}  Teardown complete${NC}"
echo -e "${BLUE}============================================================${NC}"
