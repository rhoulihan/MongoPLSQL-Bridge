#!/bin/bash
# ============================================================
# MongoPLSQL-Bridge Test Environment Validation Script
# ============================================================
# This script validates that both MongoDB and Oracle containers
# are running and accessible with the correct test data.
# ============================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ORACLE_HOST="${ORACLE_HOST:-localhost}"
ORACLE_PORT="${ORACLE_PORT:-1521}"
ORACLE_USER="${ORACLE_USER:-translator}"
ORACLE_PASSWORD="${ORACLE_PASSWORD:-translator123}"
ORACLE_SERVICE="${ORACLE_SERVICE:-FREEPDB1}"

MONGODB_HOST="${MONGODB_HOST:-localhost}"
MONGODB_PORT="${MONGODB_PORT:-27017}"
MONGODB_USER="${MONGODB_USER:-translator}"
MONGODB_PASSWORD="${MONGODB_PASSWORD:-translator123}"
MONGODB_DB="${MONGODB_DB:-testdb}"

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  MongoPLSQL-Bridge Test Environment Validation${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# Track overall status
VALIDATION_PASSED=true

# ------------------------------------------------------------
# Check Docker containers are running
# ------------------------------------------------------------
echo -e "${YELLOW}Checking Docker containers...${NC}"

if docker ps --format '{{.Names}}' | grep -q 'mongo-translator-oracle'; then
    echo -e "  ${GREEN}✓${NC} Oracle container is running"
else
    echo -e "  ${RED}✗${NC} Oracle container is NOT running"
    VALIDATION_PASSED=false
fi

if docker ps --format '{{.Names}}' | grep -q 'mongo-translator-mongodb'; then
    echo -e "  ${GREEN}✓${NC} MongoDB container is running"
else
    echo -e "  ${RED}✗${NC} MongoDB container is NOT running"
    VALIDATION_PASSED=false
fi

echo ""

# ------------------------------------------------------------
# Validate MongoDB Connection
# ------------------------------------------------------------
echo -e "${YELLOW}Validating MongoDB connection...${NC}"

MONGO_URI="mongodb://${MONGODB_USER}:${MONGODB_PASSWORD}@${MONGODB_HOST}:${MONGODB_PORT}/${MONGODB_DB}?authSource=testdb"

# Check MongoDB connectivity
if docker exec mongo-translator-mongodb mongosh --quiet --eval "db.adminCommand('ping')" > /dev/null 2>&1; then
    echo -e "  ${GREEN}✓${NC} MongoDB is responding to ping"

    # Check collections exist
    COLLECTIONS=$(docker exec mongo-translator-mongodb mongosh --quiet testdb --eval "db.getCollectionNames().join(',')" -u admin -p admin123 --authenticationDatabase admin 2>/dev/null)

    if echo "$COLLECTIONS" | grep -q "test_customers"; then
        echo -e "  ${GREEN}✓${NC} test_customers collection exists"
    else
        echo -e "  ${RED}✗${NC} test_customers collection NOT found"
        VALIDATION_PASSED=false
    fi

    if echo "$COLLECTIONS" | grep -q "test_orders"; then
        echo -e "  ${GREEN}✓${NC} test_orders collection exists"
    else
        echo -e "  ${RED}✗${NC} test_orders collection NOT found"
        VALIDATION_PASSED=false
    fi

    if echo "$COLLECTIONS" | grep -q "test_products"; then
        echo -e "  ${GREEN}✓${NC} test_products collection exists"
    else
        echo -e "  ${RED}✗${NC} test_products collection NOT found"
        VALIDATION_PASSED=false
    fi

    # Check document counts
    CUSTOMER_COUNT=$(docker exec mongo-translator-mongodb mongosh --quiet testdb --eval "db.test_customers.countDocuments()" -u admin -p admin123 --authenticationDatabase admin 2>/dev/null)
    ORDER_COUNT=$(docker exec mongo-translator-mongodb mongosh --quiet testdb --eval "db.test_orders.countDocuments()" -u admin -p admin123 --authenticationDatabase admin 2>/dev/null)
    PRODUCT_COUNT=$(docker exec mongo-translator-mongodb mongosh --quiet testdb --eval "db.test_products.countDocuments()" -u admin -p admin123 --authenticationDatabase admin 2>/dev/null)

    echo -e "  ${GREEN}✓${NC} Document counts: customers=$CUSTOMER_COUNT, orders=$ORDER_COUNT, products=$PRODUCT_COUNT"

    # Run a sample aggregation
    AGG_RESULT=$(docker exec mongo-translator-mongodb mongosh --quiet testdb --eval "db.test_orders.aggregate([{\$match: {status: 'completed'}}, {\$group: {_id: null, total: {\$sum: '\$amount'}}}]).toArray()[0].total" -u admin -p admin123 --authenticationDatabase admin 2>/dev/null)
    echo -e "  ${GREEN}✓${NC} Sample aggregation (sum of completed orders): \$${AGG_RESULT}"

else
    echo -e "  ${RED}✗${NC} MongoDB is NOT responding"
    VALIDATION_PASSED=false
fi

echo ""

# ------------------------------------------------------------
# Validate Oracle Connection
# ------------------------------------------------------------
echo -e "${YELLOW}Validating Oracle connection...${NC}"

# Check Oracle connectivity via SQL
ORACLE_TEST=$(docker exec mongo-translator-oracle sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@//localhost:1521/${ORACLE_SERVICE} << 'EOF' 2>/dev/null
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SELECT 'CONNECTED' FROM DUAL;
EXIT;
EOF
)

if echo "$ORACLE_TEST" | grep -q "CONNECTED"; then
    echo -e "  ${GREEN}✓${NC} Oracle is responding to queries"

    # Check tables exist
    TABLE_CHECK=$(docker exec mongo-translator-oracle sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@//localhost:1521/${ORACLE_SERVICE} << 'EOF' 2>/dev/null
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SELECT table_name FROM user_tables WHERE table_name LIKE 'TEST_%' ORDER BY table_name;
EXIT;
EOF
)

    if echo "$TABLE_CHECK" | grep -q "TEST_CUSTOMERS"; then
        echo -e "  ${GREEN}✓${NC} TEST_CUSTOMERS table exists"
    else
        echo -e "  ${RED}✗${NC} TEST_CUSTOMERS table NOT found"
        VALIDATION_PASSED=false
    fi

    if echo "$TABLE_CHECK" | grep -q "TEST_ORDERS"; then
        echo -e "  ${GREEN}✓${NC} TEST_ORDERS table exists"
    else
        echo -e "  ${RED}✗${NC} TEST_ORDERS table NOT found"
        VALIDATION_PASSED=false
    fi

    if echo "$TABLE_CHECK" | grep -q "TEST_PRODUCTS"; then
        echo -e "  ${GREEN}✓${NC} TEST_PRODUCTS table exists"
    else
        echo -e "  ${RED}✗${NC} TEST_PRODUCTS table NOT found"
        VALIDATION_PASSED=false
    fi

    # Check row counts
    ROW_COUNTS=$(docker exec mongo-translator-oracle sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@//localhost:1521/${ORACLE_SERVICE} << 'EOF' 2>/dev/null
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SELECT 'customers=' || COUNT(*) FROM test_customers;
SELECT 'orders=' || COUNT(*) FROM test_orders;
SELECT 'products=' || COUNT(*) FROM test_products;
EXIT;
EOF
)
    echo -e "  ${GREEN}✓${NC} Row counts: $(echo $ROW_COUNTS | tr '\n' ' ')"

    # Run equivalent SQL/JSON query
    SQL_AGG=$(docker exec mongo-translator-oracle sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@//localhost:1521/${ORACLE_SERVICE} << 'EOF' 2>/dev/null
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
SELECT SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER))
FROM test_orders
WHERE JSON_VALUE(data, '$.status') = 'completed';
EXIT;
EOF
)
    echo -e "  ${GREEN}✓${NC} Sample SQL/JSON query (sum of completed orders): \$$(echo $SQL_AGG | tr -d ' ')"

else
    echo -e "  ${RED}✗${NC} Oracle is NOT responding"
    VALIDATION_PASSED=false
fi

echo ""

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
echo -e "${BLUE}============================================================${NC}"
if [ "$VALIDATION_PASSED" = true ]; then
    echo -e "${GREEN}  ✓ All validations PASSED${NC}"
    echo -e "${GREEN}  Test environment is ready for use${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
    echo -e "Connection Details:"
    echo -e "  MongoDB: mongodb://translator:translator123@localhost:27017/testdb"
    echo -e "  Oracle:  jdbc:oracle:thin:@localhost:1521/FREEPDB1 (translator/translator123)"
    echo ""
    exit 0
else
    echo -e "${RED}  ✗ Some validations FAILED${NC}"
    echo -e "${RED}  Please check the errors above${NC}"
    echo -e "${BLUE}============================================================${NC}"
    echo ""
    exit 1
fi
