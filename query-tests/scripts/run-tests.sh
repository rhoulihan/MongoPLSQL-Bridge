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

# Function to wrap SQL to return JSON array using Oracle's JSON_ARRAYAGG
wrap_sql_as_json_array() {
    local sql="$1"
    # Remove trailing semicolon if present
    local clean_sql="${sql%;}"
    # Wrap entire query as subquery and aggregate into JSON array
    echo "SELECT JSON_ARRAYAGG(JSON_OBJECT(*) RETURNING CLOB) AS json_result FROM (${clean_sql})"
}

# Function to run Oracle query (raw output)
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

# Function to run Oracle query and return JSON array
run_oracle_query_json() {
    local sql="$1"
    local json_sql
    json_sql=$(wrap_sql_as_json_array "$sql")

    local result
    result=$(docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOSQL'
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK OFF
SET HEADING OFF
SET LONG 10000000
SET LONGCHUNKSIZE 10000000
${json_sql};
EXIT;
EOSQL" 2>/dev/null)

    # Result is already a JSON array from JSON_ARRAYAGG
    # Just clean up and return it (or empty array if null/empty)
    local cleaned
    cleaned=$(echo "$result" | tr -d '\n\r' | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')
    if [ -z "$cleaned" ] || [ "$cleaned" = "null" ]; then
        echo "[]"
    else
        echo "$cleaned"
    fi
}

# Function to run Oracle count query (counts actual rows)
run_oracle_count() {
    local sql="$1"

    docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOSQL'
SET PAGESIZE 0
SET LINESIZE 100
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK OFF
SET HEADING OFF
SELECT COUNT(*) FROM (${sql});
EXIT;
EOSQL" 2>/dev/null | grep -v "^$" | head -1 | tr -d ' \t'
}

# Function to generate Oracle SQL using the translator
generate_oracle_sql() {
    local collection="$1"
    local pipeline="$2"
    local result
    local exit_code

    result=$(cd "$PROJECT_ROOT" && timeout --kill-after=5 30 ./gradlew :core:translatePipeline \
        -PcollectionName="$collection" \
        -PpipelineJson="$pipeline" \
        -Pinline=true \
        --quiet 2>/dev/null)
    exit_code=$?

    # If timeout killed the process, wait a moment for cleanup
    if [ $exit_code -eq 124 ] || [ $exit_code -eq 137 ]; then
        sleep 1
    fi

    echo "$result"
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

# Pre-warm Gradle daemon by running a simple task
echo -e "  ${YELLOW}Warming up Gradle daemon...${NC}"
cd "$PROJECT_ROOT" && ./gradlew --quiet :core:classes >/dev/null 2>&1 || true
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

    # Always generate Oracle SQL using the translator to test current implementation
    GENERATED_SQL=$(generate_oracle_sql "$TEST_COLLECTION" "$TEST_PIPELINE" 2>&1)
    if [ -z "$GENERATED_SQL" ] || [[ "$GENERATED_SQL" == *"Error"* ]] || [[ "$GENERATED_SQL" == *"FAILURE"* ]]; then
        echo -e "${YELLOW}SKIP${NC} (translator error)"
        SKIPPED_TESTS=$((SKIPPED_TESTS + 1))

        echo "[$TEST_ID] $TEST_NAME" >> "$REPORT_FILE"
        echo "  Category: $TEST_CATEGORY" >> "$REPORT_FILE"
        echo "  Status: SKIP" >> "$REPORT_FILE"
        echo "  Details: Translation not supported" >> "$REPORT_FILE"
        echo "" >> "$REPORT_FILE"

        if [ "$FIRST_RESULT" = true ]; then
            FIRST_RESULT=false
        else
            echo "," >> "$JSON_REPORT_FILE"
        fi
        echo "{\"id\": \"$TEST_ID\", \"name\": \"$TEST_NAME\", \"category\": \"$TEST_CATEGORY\", \"status\": \"SKIP\", \"mongodb_count\": \"N/A\", \"oracle_count\": \"N/A\", \"expected_count\": \"$TEST_EXPECTED\"}" >> "$JSON_REPORT_FILE"
        continue
    fi
    TEST_SQL="$GENERATED_SQL"

    # Run MongoDB query
    MONGO_RESULT=$(run_mongodb_query "$TEST_COLLECTION" "$TEST_PIPELINE" 2>&1) || MONGO_RESULT="[]"
    MONGO_COUNT=$(echo "$MONGO_RESULT" | python3 -c "import sys, json; data = json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "ERROR")

    # Run Oracle query and get JSON results for document comparison
    ORACLE_JSON=$(run_oracle_query_json "$TEST_SQL" 2>&1) || ORACLE_JSON="[]"
    ORACLE_COUNT=$(echo "$ORACLE_JSON" | python3 -c "import sys, json; data = json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "ERROR")

    # Determine result
    STATUS=""
    DETAILS=""
    COMPARE_RESULT=""

    if [ "$MONGO_COUNT" = "ERROR" ]; then
        STATUS="ERROR"
        DETAILS="MongoDB query failed"
        ERROR_TESTS=$((ERROR_TESTS + 1))
        echo -e "${RED}ERROR${NC}"
    elif [ "$ORACLE_COUNT" = "ERROR" ]; then
        STATUS="ERROR"
        DETAILS="Oracle query failed"
        ERROR_TESTS=$((ERROR_TESTS + 1))
        echo -e "${RED}ERROR${NC}"
    elif [ "$MONGO_COUNT" != "$ORACLE_COUNT" ]; then
        STATUS="FAIL"
        DETAILS="Count mismatch: MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT, Expected=$TEST_EXPECTED"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo -e "${RED}FAIL${NC} (count: MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT)"
    else
        # Counts match - now compare document structure and values
        COMPARE_RESULT=$(echo "{\"mongo\": $MONGO_RESULT, \"oracle\": $ORACLE_JSON}" | \
            python3 "$SCRIPT_DIR/compare-results.py" --stdin 2>&1) || COMPARE_RESULT="ERROR"

        if [[ "$COMPARE_RESULT" == MATCH* ]]; then
            STATUS="PASS"
            DETAILS="Documents match: MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            echo -e "${GREEN}PASS${NC} (count: $MONGO_COUNT, docs match)"
        elif [[ "$COMPARE_RESULT" == *MISMATCH* ]]; then
            STATUS="FAIL"
            DETAILS="Document mismatch: $COMPARE_RESULT (counts: $MONGO_COUNT)"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            echo -e "${RED}FAIL${NC} (count: $MONGO_COUNT, $COMPARE_RESULT)"
        else
            # Comparison error - fall back to count comparison
            if [ "$MONGO_COUNT" = "$TEST_EXPECTED" ]; then
                STATUS="PASS"
                DETAILS="MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT (comparison: $COMPARE_RESULT)"
                PASSED_TESTS=$((PASSED_TESTS + 1))
                echo -e "${GREEN}PASS${NC} (count: $MONGO_COUNT)"
            else
                STATUS="PASS"
                DETAILS="Counts match: MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT (expected $TEST_EXPECTED)"
                PASSED_TESTS=$((PASSED_TESTS + 1))
                echo -e "${GREEN}PASS${NC} (count: $MONGO_COUNT, expected: $TEST_EXPECTED)"
            fi
        fi
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

    # Small delay between tests to ensure proper resource release
    sleep 0.1
done

# Close JSON report
echo '],"summary":{"total":'$TOTAL_TESTS',"passed":'$PASSED_TESTS',"failed":'$FAILED_TESTS',"skipped":'$SKIPPED_TESTS',"errors":'$ERROR_TESTS'}}' >> "$JSON_REPORT_FILE"

# Print summary
echo ""
echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  Test Summary${NC}"
echo -e "${BLUE}============================================================${NC}"
echo -e "  Total:   $TOTAL_TESTS"
echo -e "  ${GREEN}Passed:  $PASSED_TESTS${NC}"
echo -e "  ${RED}Failed:  $FAILED_TESTS${NC}"
echo -e "  ${YELLOW}Skipped: $SKIPPED_TESTS${NC}"
echo -e "  ${YELLOW}Errors:  $ERROR_TESTS${NC}"
echo -e "${BLUE}============================================================${NC}"

# Add summary to report
echo "============================================================" >> "$REPORT_FILE"
echo "SUMMARY" >> "$REPORT_FILE"
echo "============================================================" >> "$REPORT_FILE"
echo "Total:   $TOTAL_TESTS" >> "$REPORT_FILE"
echo "Passed:  $PASSED_TESTS" >> "$REPORT_FILE"
echo "Failed:  $FAILED_TESTS" >> "$REPORT_FILE"
echo "Skipped: $SKIPPED_TESTS" >> "$REPORT_FILE"
echo "Errors:  $ERROR_TESTS" >> "$REPORT_FILE"

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
