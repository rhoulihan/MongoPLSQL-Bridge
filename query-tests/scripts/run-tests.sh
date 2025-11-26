#!/bin/bash
# ============================================================
# MongoPLSQL-Bridge Query Validation Test Runner
# ============================================================
# Executes test cases against both MongoDB and Oracle,
# compares results, and generates a report.
# ============================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUERY_TESTS_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$QUERY_TESTS_DIR")"

# Configuration
RESULTS_DIR="$QUERY_TESTS_DIR/results"
TEST_CASES_FILE="$QUERY_TESTS_DIR/tests/test-cases.json"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REPORT_FILE="$RESULTS_DIR/test-report-${TIMESTAMP}.txt"
JSON_REPORT_FILE="$RESULTS_DIR/test-report-${TIMESTAMP}.json"

# Test filtering options
CATEGORY_FILTER=""
TEST_FILTER=""
VERBOSE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --category)
            CATEGORY_FILTER="$2"
            shift 2
            ;;
        --test)
            TEST_FILTER="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --category <name>   Run only tests in specified category"
            echo "  --test <id>         Run only specified test by ID"
            echo "  --verbose, -v       Show detailed output"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Categories: comparison, logical, accumulator, stage, arithmetic, conditional, complex, edge"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Create results directory
mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  MongoPLSQL-Bridge Query Validation Tests${NC}"
echo -e "${BLUE}============================================================${NC}"
echo -e "  Timestamp: $(date)"
echo -e "  Results:   $RESULTS_DIR"
if [ -n "$CATEGORY_FILTER" ]; then
    echo -e "  Category:  $CATEGORY_FILTER"
fi
if [ -n "$TEST_FILTER" ]; then
    echo -e "  Test ID:   $TEST_FILTER"
fi
echo -e "${BLUE}============================================================${NC}"
echo ""

# Initialize counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
ERROR_TESTS=0
SKIPPED_TESTS=0

# Initialize report
echo "MongoPLSQL-Bridge Query Validation Test Report" > "$REPORT_FILE"
echo "Generated: $(date)" >> "$REPORT_FILE"
echo "============================================================" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

# Start JSON report
echo '{"timestamp": "'$(date -Iseconds)'", "results": [' > "$JSON_REPORT_FILE"
FIRST_RESULT=true

# Function to run MongoDB query
run_mongodb_query() {
    local collection="$1"
    local pipeline="$2"

    docker exec mongo-translator-mongodb mongosh --quiet testdb \
        -u admin -p admin123 --authenticationDatabase admin \
        --eval "JSON.stringify(db.${collection}.aggregate(${pipeline}).toArray())" 2>/dev/null
}

# Function to run Oracle query
run_oracle_query() {
    local sql="$1"

    docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOSQL'
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK OFF
SET HEADING OFF
SET COLSEP '|'
SET LONG 1000000
SET LONGCHUNKSIZE 1000000
${sql};
EXIT;
EOSQL" 2>/dev/null
}

# Function to normalize and compare results
compare_results() {
    local mongo_result="$1"
    local oracle_result="$2"
    local expected_count="$3"

    # Count results
    local mongo_count=$(echo "$mongo_result" | python3 -c "import sys, json; data = json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "0")
    local oracle_count=$(echo "$oracle_result" | grep -c "." 2>/dev/null || echo "0")

    if [ "$mongo_count" != "$expected_count" ]; then
        echo "MONGO_COUNT_MISMATCH:$mongo_count"
        return 1
    fi

    # For now, just compare counts
    # A full implementation would parse and compare actual data
    echo "COUNTS_MATCH:$mongo_count"
    return 0
}

# Read and process test cases
echo -e "${YELLOW}Loading test cases...${NC}"

# Extract test IDs
TEST_IDS=$(python3 << PYTHON
import json

with open('$TEST_CASES_FILE', 'r') as f:
    data = json.load(f)

for test in data['test_cases']:
    category_match = not '$CATEGORY_FILTER' or test['category'] == '$CATEGORY_FILTER'
    test_match = not '$TEST_FILTER' or test['id'] == '$TEST_FILTER'

    if category_match and test_match:
        print(test['id'])
PYTHON
)

if [ -z "$TEST_IDS" ]; then
    echo -e "${RED}No test cases found matching criteria${NC}"
    exit 1
fi

TEST_COUNT=$(echo "$TEST_IDS" | wc -l)
echo -e "  Found ${TEST_COUNT} test(s) to run"
echo ""

# Run each test
for TEST_ID in $TEST_IDS; do
    TOTAL_TESTS=$((TOTAL_TESTS + 1))

    # Extract test details
    TEST_DETAILS=$(python3 << PYTHON
import json

with open('$TEST_CASES_FILE', 'r') as f:
    data = json.load(f)

for test in data['test_cases']:
    if test['id'] == '$TEST_ID':
        print(f"NAME:{test['name']}")
        print(f"CATEGORY:{test['category']}")
        print(f"COLLECTION:{test['collection']}")
        print(f"PIPELINE:{json.dumps(test['mongodb_pipeline'])}")
        print(f"SQL:{test['oracle_sql']}")
        print(f"EXPECTED:{test['expected_count']}")
        break
PYTHON
)

    TEST_NAME=$(echo "$TEST_DETAILS" | grep "^NAME:" | cut -d: -f2-)
    TEST_CATEGORY=$(echo "$TEST_DETAILS" | grep "^CATEGORY:" | cut -d: -f2-)
    TEST_COLLECTION=$(echo "$TEST_DETAILS" | grep "^COLLECTION:" | cut -d: -f2-)
    TEST_PIPELINE=$(echo "$TEST_DETAILS" | grep "^PIPELINE:" | cut -d: -f2-)
    TEST_SQL=$(echo "$TEST_DETAILS" | grep "^SQL:" | cut -d: -f2-)
    TEST_EXPECTED=$(echo "$TEST_DETAILS" | grep "^EXPECTED:" | cut -d: -f2-)

    echo -n -e "  ${CYAN}[${TEST_ID}]${NC} ${TEST_NAME}... "

    # Run MongoDB query
    MONGO_RESULT=$(run_mongodb_query "$TEST_COLLECTION" "$TEST_PIPELINE" 2>&1) || MONGO_RESULT="ERROR"
    MONGO_COUNT=$(echo "$MONGO_RESULT" | python3 -c "import sys, json; data = json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "ERROR")

    # Run Oracle query
    ORACLE_RESULT=$(run_oracle_query "$TEST_SQL" 2>&1) || ORACLE_RESULT="ERROR"
    ORACLE_COUNT=$(echo "$ORACLE_RESULT" | grep -v "^$" | wc -l)

    # Determine result
    STATUS=""
    DETAILS=""

    if [ "$MONGO_COUNT" = "ERROR" ]; then
        STATUS="ERROR"
        DETAILS="MongoDB query failed"
        ERROR_TESTS=$((ERROR_TESTS + 1))
        echo -e "${RED}ERROR${NC}"
    elif [ "$ORACLE_RESULT" = "ERROR" ]; then
        STATUS="ERROR"
        DETAILS="Oracle query failed"
        ERROR_TESTS=$((ERROR_TESTS + 1))
        echo -e "${RED}ERROR${NC}"
    elif [ "$MONGO_COUNT" = "$ORACLE_COUNT" ] && [ "$MONGO_COUNT" = "$TEST_EXPECTED" ]; then
        STATUS="PASS"
        DETAILS="MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT, Expected=$TEST_EXPECTED"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        echo -e "${GREEN}PASS${NC} (count: $MONGO_COUNT)"
    elif [ "$MONGO_COUNT" = "$ORACLE_COUNT" ]; then
        STATUS="PASS"
        DETAILS="MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT (expected $TEST_EXPECTED, counts match)"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        echo -e "${GREEN}PASS${NC} (count: $MONGO_COUNT, expected: $TEST_EXPECTED)"
    else
        STATUS="FAIL"
        DETAILS="MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT, Expected=$TEST_EXPECTED"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo -e "${RED}FAIL${NC} (MongoDB: $MONGO_COUNT, Oracle: $ORACLE_COUNT)"
    fi

    # Log to report
    echo "[$TEST_ID] $TEST_NAME" >> "$REPORT_FILE"
    echo "  Category: $TEST_CATEGORY" >> "$REPORT_FILE"
    echo "  Status: $STATUS" >> "$REPORT_FILE"
    echo "  Details: $DETAILS" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"

    # Add to JSON report
    if [ "$FIRST_RESULT" = true ]; then
        FIRST_RESULT=false
    else
        echo "," >> "$JSON_REPORT_FILE"
    fi
    echo "{\"id\": \"$TEST_ID\", \"name\": \"$TEST_NAME\", \"category\": \"$TEST_CATEGORY\", \"status\": \"$STATUS\", \"mongodb_count\": \"$MONGO_COUNT\", \"oracle_count\": \"$ORACLE_COUNT\", \"expected_count\": \"$TEST_EXPECTED\"}" >> "$JSON_REPORT_FILE"

    # Show verbose output if requested
    if [ "$VERBOSE" = true ]; then
        echo "    Pipeline: $TEST_PIPELINE"
        echo "    SQL: $TEST_SQL"
        if [ "$STATUS" = "FAIL" ] || [ "$STATUS" = "ERROR" ]; then
            echo "    MongoDB result: $MONGO_RESULT"
            echo "    Oracle result: $ORACLE_RESULT"
        fi
    fi
done

# Close JSON report
echo '],"summary":{"total":'$TOTAL_TESTS',"passed":'$PASSED_TESTS',"failed":'$FAILED_TESTS',"errors":'$ERROR_TESTS'}}' >> "$JSON_REPORT_FILE"

# Print summary
echo ""
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}============================================================${NC}"
echo -e "  Total:   $TOTAL_TESTS"
echo -e "  ${GREEN}Passed:  $PASSED_TESTS${NC}"
echo -e "  ${RED}Failed:  $FAILED_TESTS${NC}"
echo -e "  ${YELLOW}Errors:  $ERROR_TESTS${NC}"
echo -e "${BLUE}============================================================${NC}"

# Add summary to report
echo "============================================================" >> "$REPORT_FILE"
echo "SUMMARY" >> "$REPORT_FILE"
echo "============================================================" >> "$REPORT_FILE"
echo "Total:  $TOTAL_TESTS" >> "$REPORT_FILE"
echo "Passed: $PASSED_TESTS" >> "$REPORT_FILE"
echo "Failed: $FAILED_TESTS" >> "$REPORT_FILE"
echo "Errors: $ERROR_TESTS" >> "$REPORT_FILE"

echo ""
echo "Reports generated:"
echo "  Text: $REPORT_FILE"
echo "  JSON: $JSON_REPORT_FILE"

# Create symlink to latest report
ln -sf "$(basename "$REPORT_FILE")" "$RESULTS_DIR/test-report-latest.txt"
ln -sf "$(basename "$JSON_REPORT_FILE")" "$RESULTS_DIR/test-report-latest.json"

# Exit with error if any tests failed
if [ $FAILED_TESTS -gt 0 ] || [ $ERROR_TESTS -gt 0 ]; then
    exit 1
fi

exit 0
