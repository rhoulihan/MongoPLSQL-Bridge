#!/bin/bash
# ============================================================
# MongoPLSQL-Bridge Query Tests - Environment Setup
# ============================================================
# Sets up the test environment by loading test data into both
# MongoDB and Oracle databases.
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
PROJECT_ROOT="$(dirname "$QUERY_TESTS_DIR")"
DATA_DIR="$QUERY_TESTS_DIR/data"

# Parse arguments
RESET=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --reset)
            RESET=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --reset    Drop and recreate all test data"
            echo "  --help     Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  MongoPLSQL-Bridge Query Tests - Environment Setup${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# Check if containers are running
echo -e "${YELLOW}Checking Docker containers...${NC}"

if ! docker ps --format '{{.Names}}' | grep -q 'mongo-translator-mongodb'; then
    echo -e "${RED}MongoDB container is not running.${NC}"
    echo "Please start the environment first: ./scripts/start-env.sh"
    exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q 'mongo-translator-oracle'; then
    echo -e "${RED}Oracle container is not running.${NC}"
    echo "Please start the environment first: ./scripts/start-env.sh"
    exit 1
fi

echo -e "  ${GREEN}✓${NC} Both containers are running"
echo ""

# Load MongoDB test data
echo -e "${YELLOW}Loading MongoDB test data...${NC}"

docker exec -i mongo-translator-mongodb mongosh testdb \
    -u admin -p admin123 --authenticationDatabase admin \
    < "$DATA_DIR/load-mongodb.js"

echo -e "  ${GREEN}✓${NC} MongoDB test data loaded"
echo ""

# Load Oracle test data
echo -e "${YELLOW}Loading Oracle test data...${NC}"

docker exec -i mongo-translator-oracle sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 \
    < "$DATA_DIR/load-oracle.sql"

echo -e "  ${GREEN}✓${NC} Oracle test data loaded"
echo ""

# Validate data counts
echo -e "${YELLOW}Validating data counts...${NC}"

MONGO_SALES=$(docker exec mongo-translator-mongodb mongosh --quiet testdb \
    -u admin -p admin123 --authenticationDatabase admin \
    --eval "db.sales.countDocuments()")

MONGO_EMPLOYEES=$(docker exec mongo-translator-mongodb mongosh --quiet testdb \
    -u admin -p admin123 --authenticationDatabase admin \
    --eval "db.employees.countDocuments()")

MONGO_PRODUCTS=$(docker exec mongo-translator-mongodb mongosh --quiet testdb \
    -u admin -p admin123 --authenticationDatabase admin \
    --eval "db.products.countDocuments()")

ORACLE_SALES=$(docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOF'
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SELECT COUNT(*) FROM sales;
EXIT;
EOF" | tr -d ' \n\t')

ORACLE_EMPLOYEES=$(docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOF'
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SELECT COUNT(*) FROM employees;
EXIT;
EOF" | tr -d ' \n\t')

ORACLE_PRODUCTS=$(docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOF'
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SELECT COUNT(*) FROM products;
EXIT;
EOF" | tr -d ' \n\t')

echo ""
echo -e "  Collection/Table     MongoDB    Oracle"
echo -e "  ─────────────────    ───────    ──────"
echo -e "  sales                $MONGO_SALES          $ORACLE_SALES"
echo -e "  employees            $MONGO_EMPLOYEES          $ORACLE_EMPLOYEES"
echo -e "  products             $MONGO_PRODUCTS           $ORACLE_PRODUCTS"
echo ""

# Verify counts match
VALID=true

if [ "$MONGO_SALES" != "$ORACLE_SALES" ]; then
    echo -e "${RED}Sales count mismatch!${NC}"
    VALID=false
fi

if [ "$MONGO_EMPLOYEES" != "$ORACLE_EMPLOYEES" ]; then
    echo -e "${RED}Employees count mismatch!${NC}"
    VALID=false
fi

if [ "$MONGO_PRODUCTS" != "$ORACLE_PRODUCTS" ]; then
    echo -e "${RED}Products count mismatch!${NC}"
    VALID=false
fi

if [ "$VALID" = true ]; then
    echo -e "${GREEN}✓ All data counts match${NC}"
else
    echo -e "${RED}✗ Data count mismatch detected${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}============================================================${NC}"
echo -e "${GREEN}  Test environment setup complete${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""
echo "You can now run tests with: ./scripts/run-tests.sh"
