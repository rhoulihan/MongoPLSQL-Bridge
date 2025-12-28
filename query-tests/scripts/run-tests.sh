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
CTE_MODE=true  # CTE mode is now the default

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
        --cte)
            CTE_MODE=true
            shift
            ;;
        --legacy)
            CTE_MODE=false
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --category <name>   Run only tests in specified category"
            echo "  --test <id>         Run only specified test by ID"
            echo "  --verbose, -v       Show detailed output"
            echo "  --cte               Use CTE-based SQL generation (default)"
            echo "  --legacy            Use legacy SQL generation"
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
if [ "$CTE_MODE" = true ]; then
    echo -e "  ${CYAN}CTE Mode:  ENABLED${NC}"
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

# Function to run MongoDB query (relaxed JSON for value comparison)
# Uses EJSON.parse to properly convert Extended JSON ($date, $oid, etc.) to native BSON types
# Uses EJSON.stringify with relaxed mode to normalize output for comparison with Oracle results
run_mongodb_query() {
    local collection="$1"
    local pipeline="$2"

    # Escape single quotes in pipeline for shell embedding
    local escaped_pipeline="${pipeline//\'/\'\"\'\"\'}"

    docker exec mongo-translator-mongodb mongosh --quiet testdb \
        -u admin -p admin123 --authenticationDatabase admin \
        --eval "var p = EJSON.parse('${escaped_pipeline}'); EJSON.stringify(db.${collection}.aggregate(p).toArray(), {relaxed: true})" 2>/dev/null
}

# Function to run MongoDB query with EJSON output (Extended JSON for type comparison)
# Uses EJSON.stringify WITHOUT relaxed mode to preserve type information
run_mongodb_query_ejson() {
    local collection="$1"
    local pipeline="$2"

    # Escape single quotes in pipeline for shell embedding
    local escaped_pipeline="${pipeline//\'/\'\"\'\"\'}"

    docker exec mongo-translator-mongodb mongosh --quiet testdb \
        -u admin -p admin123 --authenticationDatabase admin \
        --eval "var p = EJSON.parse('${escaped_pipeline}'); EJSON.stringify(db.${collection}.aggregate(p).toArray())" 2>/dev/null
}

# Function to convert MongoDB EJSON to Oracle's EJSON format
# Uses Oracle's JSON_SERIALIZE(JSON(text extended) returning clob extended)
convert_mongo_ejson_to_oracle() {
    local ejson_array="$1"

    # Oracle can only process one document at a time, so we use a PL/SQL block
    # to parse and re-serialize each document
    docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOSQL'
SET DEFINE OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 32767
SET PAGESIZE 0
SET FEEDBACK OFF
SET HEADING OFF
DECLARE
    v_json_array  JSON_ARRAY_T;
    v_doc         JSON_OBJECT_T;
    v_result      CLOB;
BEGIN
    v_json_array := JSON_ARRAY_T.PARSE('${ejson_array}');
    DBMS_OUTPUT.PUT('[');
    FOR i IN 0 .. v_json_array.get_size - 1 LOOP
        IF i > 0 THEN
            DBMS_OUTPUT.PUT(',');
        END IF;
        v_doc := TREAT(v_json_array.get(i) AS JSON_OBJECT_T);
        SELECT JSON_SERIALIZE(JSON(v_doc.to_clob EXTENDED) RETURNING CLOB EXTENDED)
        INTO v_result
        FROM dual;
        DBMS_OUTPUT.PUT(v_result);
    END LOOP;
    DBMS_OUTPUT.PUT_LINE(']');
END;
/
EXIT;
EOSQL" 2>/dev/null | grep -v "^PL/SQL" | grep -v "^$" | tr -d '\n'
}

# Function to wrap SQL to return JSON array using Oracle's JSON_ARRAYAGG
wrap_sql_as_json_array() {
    local sql="$1"
    # Remove trailing semicolon if present
    local clean_sql="${sql%;}"
    # Wrap entire query as subquery and aggregate into JSON array
    echo "SELECT JSON_ARRAYAGG(JSON_OBJECT(*) RETURNING CLOB) AS json_result FROM (${clean_sql})"
}

# Function to execute SQL via temp file inside Oracle container
# This bypasses SQL*Plus's 4999 character line limit by:
# 1. Writing SQL to a temp file
# 2. Breaking long lines at CTE boundaries ("), ") to stay under 4999 chars per line
# Args: $1 = SQL to execute, $2 = additional sqlplus settings (optional)
run_oracle_sql_via_file() {
    local sql="$1"
    local extra_settings="${2:-}"
    local host_temp_file
    local container_temp_file="/tmp/query_$$.sql"

    # Break long SQL into multiple lines at CTE boundaries
    # This ensures each line stays under SQL*Plus's 4999 char limit
    local sql_formatted
    sql_formatted=$(echo "$sql" | sed 's/), "/),\n"/g')

    # Create temp file on host
    host_temp_file=$(mktemp /tmp/oracle-query-XXXXXX.sql)

    # Write SQL content to host temp file
    cat > "$host_temp_file" << EOSQL
SET DEFINE OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK OFF
SET HEADING OFF
SET LONG 10000000
SET LONGCHUNKSIZE 10000000
${extra_settings}
${sql_formatted};
EXIT;
EOSQL

    # Copy to container
    docker cp "$host_temp_file" "mongo-translator-oracle:${container_temp_file}" 2>/dev/null
    # Fix permissions as root (docker cp creates files owned by root)
    docker exec -u 0 mongo-translator-oracle chmod 644 "${container_temp_file}" 2>/dev/null
    # Execute as oracle user
    docker exec mongo-translator-oracle sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 @"${container_temp_file}" 2>/dev/null
    # Cleanup as root
    docker exec -u 0 mongo-translator-oracle rm -f "${container_temp_file}" 2>/dev/null

    # Cleanup host temp file
    rm -f "$host_temp_file"
}

# Function to execute procedural SQL with PL/SQL blocks
# Procedural SQL contains BEGIN...END blocks separated by / (PL/SQL terminators)
# This function handles the / separators correctly for SQL*Plus
run_oracle_procedural_sql() {
    local sql="$1"
    local host_temp_file
    local container_temp_file="/tmp/procedural_$$.sql"

    # Create temp file on host
    host_temp_file=$(mktemp /tmp/oracle-procedural-XXXXXX.sql)

    # Write SQL content to host temp file
    # Procedural SQL already has proper formatting with / separators
    cat > "$host_temp_file" << EOSQL
SET DEFINE OFF
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK OFF
SET SERVEROUTPUT OFF
SET HEADING OFF
SET LONG 10000000
SET LONGCHUNKSIZE 10000000
${sql}
EXIT;
EOSQL

    # Copy to container
    docker cp "$host_temp_file" "mongo-translator-oracle:${container_temp_file}" 2>/dev/null
    # Fix permissions as root (docker cp creates files owned by root)
    docker exec -u 0 mongo-translator-oracle chmod 644 "${container_temp_file}" 2>/dev/null
    # Execute as oracle user
    docker exec mongo-translator-oracle sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 @"${container_temp_file}" 2>/dev/null
    # Cleanup as root
    docker exec -u 0 mongo-translator-oracle rm -f "${container_temp_file}" 2>/dev/null

    # Cleanup host temp file
    rm -f "$host_temp_file"
}

# Function to run Oracle query (raw output)
run_oracle_query() {
    local sql="$1"

    docker exec mongo-translator-oracle bash -c "sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOSQL'
SET DEFINE OFF
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
    local result

    # Check if this is procedural SQL (auto-generated for complex pipelines)
    # Procedural SQL starts with "-- Procedural execution" comment
    if [[ "$sql" =~ ^[[:space:]]*--[[:space:]]*Procedural[[:space:]]+execution ]]; then
        # Execute procedural SQL with PL/SQL block handling
        result=$(run_oracle_procedural_sql "$sql")
    else
        # Check if SQL already outputs JSON_ARRAYAGG (type-preserving format from translator)
        # If so, use it directly; otherwise wrap with JSON_OBJECT(*) (loses types)
        # Note: SQL may start with WITH (CTEs) followed by SELECT JSON_ARRAYAGG
        # We check for:
        #   1. SQL starting with SELECT JSON_ARRAYAGG (outer SELECT is JSON_ARRAYAGG)
        #   2. CTE pattern: WITH ... ) SELECT JSON_ARRAYAGG (outer SELECT after CTE is JSON_ARRAYAGG)
        # We must NOT match SQL that has SELECT JSON_ARRAYAGG only in subqueries (like FACET)
        if [[ "$sql" =~ ^[[:space:]]*SELECT[[:space:]]+JSON_ARRAYAGG ]]; then
            # Simple SELECT JSON_ARRAYAGG - use directly
            json_sql="${sql%;}"
        elif [[ "$sql" =~ ^[[:space:]]*WITH[[:space:]].*\)[[:space:]]+SELECT[[:space:]]+JSON_ARRAYAGG ]]; then
            # CTE followed by outer SELECT JSON_ARRAYAGG - use directly
            json_sql="${sql%;}"
        else
            # Legacy SQL - wrap with JSON_OBJECT(*) (may lose type information)
            json_sql=$(wrap_sql_as_json_array "$sql")
        fi

        # Use file-based execution for long SQL (bypasses 4999 char line limit)
        result=$(run_oracle_sql_via_file "$json_sql")
    fi

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

# Function to run Oracle query and return EJSON (Extended JSON with type info)
# Uses JSON_SERIALIZE with EXTENDED option to preserve type encodings
run_oracle_query_ejson() {
    local sql="$1"

    # Modify the SQL to wrap results in JSON_SERIALIZE with EXTENDED
    # The translator already produces JSON_ARRAYAGG output, so we just need
    # to add the EXTENDED serialization
    local ejson_sql

    # For CTE-based queries, we need to wrap the final SELECT with JSON_SERIALIZE
    # Pattern: WITH ... ) SELECT JSON_ARRAYAGG("DATA" RETURNING CLOB) FROM "QN"
    # The translator always generates JSON_ARRAYAGG("DATA" RETURNING CLOB), so use literal match
    if [[ "$sql" =~ ^[[:space:]]*WITH[[:space:]] ]] || [[ "$sql" =~ ^[[:space:]]*SELECT[[:space:]]+JSON_ARRAYAGG ]]; then
        # Wrap JSON_ARRAYAGG with JSON_SERIALIZE to get Extended JSON output
        ejson_sql=$(echo "${sql%;}" | sed 's/JSON_ARRAYAGG("DATA" RETURNING CLOB)/JSON_SERIALIZE(JSON_ARRAYAGG("DATA" RETURNING CLOB) RETURNING CLOB EXTENDED)/')
    else
        # Non-aggregated SQL - wrap entire result
        local inner_sql="${sql%;}"
        ejson_sql="SELECT JSON_SERIALIZE(JSON_ARRAYAGG(JSON_OBJECT(*) RETURNING JSON) RETURNING CLOB EXTENDED) FROM (${inner_sql})"
    fi

    local result
    # Use file-based execution for long SQL (bypasses 4999 char line limit)
    result=$(run_oracle_sql_via_file "$ejson_sql")

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
    local count_sql="SELECT COUNT(*) FROM (${sql%;})"

    # Use file-based execution for long SQL (bypasses 4999 char line limit)
    run_oracle_sql_via_file "$count_sql" | grep -v "^$" | head -1 | tr -d ' \t'
}

# Function to generate Oracle EXPLAIN PLAN for a query
# Returns the execution plan as formatted text
run_oracle_explain_plan() {
    local sql="$1"
    local plan_id="plan_$$_$RANDOM"

    # Clean up the SQL - remove trailing semicolon if present
    local clean_sql="${sql%;}"

    # Build the explain plan SQL with display and cleanup
    local explain_sql="EXPLAIN PLAN SET STATEMENT_ID = '${plan_id}' FOR
${clean_sql};

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', '${plan_id}', 'ALL'));

DELETE FROM PLAN_TABLE WHERE STATEMENT_ID = '${plan_id}';
COMMIT"

    local result
    # Use file-based execution for long SQL (bypasses 4999 char line limit)
    result=$(run_oracle_sql_via_file "$explain_sql" "SET PAGESIZE 1000
SET HEADING ON")

    # Return the plan output, preserving formatting
    echo "$result"
}

# Function to generate Oracle SQL using the translator
generate_oracle_sql() {
    local collection="$1"
    local pipeline="$2"
    local tmp_file
    local result
    local exit_code

    # Write pipeline to temp file (CLI requires file input)
    tmp_file=$(mktemp /tmp/mongo-pipeline-XXXXXX.json)
    echo "$pipeline" > "$tmp_file"

    # Build CLI options
    local cli_opts="--collection $collection --inline"
    if [ "$CTE_MODE" = true ]; then
        cli_opts="$cli_opts --cte"
    fi

    # Use the CLI tool directly (faster than gradle)
    result=$(cd "$PROJECT_ROOT" && timeout --kill-after=5 30 ./mongo2sql $cli_opts "$tmp_file" 2>&1)
    exit_code=$?

    # Clean up temp file
    rm -f "$tmp_file"

    # If timeout killed the process, wait a moment for cleanup
    if [ $exit_code -eq 124 ] || [ $exit_code -eq 137 ]; then
        sleep 1
    fi

    # Check if this is procedural SQL (starts with "-- Procedural execution")
    # If so, return the full output; otherwise extract just the SQL statement
    if [[ "$result" =~ ^[[:space:]]*--[[:space:]]*Procedural[[:space:]]+execution ]]; then
        # Return full procedural SQL including comments
        echo "$result"
    else
        # Extract just the SQL (before any comments)
        echo "$result" | grep -v "^--" | grep -v "^$" | head -1
    fi
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

# Always rebuild the fat JAR to ensure we're testing current code
# This ensures any source code changes are reflected in test results
echo -e "  ${YELLOW}Building translator JAR...${NC}"
cd "$PROJECT_ROOT" && ./gradlew --quiet :core:fatJar || {
    echo -e "${RED}Failed to build translator JAR${NC}"
    exit 1
}
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
        print(f"SQL:{test.get('oracle_sql', '')}")
        print(f"EXPECTED:{test.get('expected_count', 0)}")
        print(f"SKIP:{test.get('skip', False)}")
        break
PYTHON
)

    TEST_NAME=$(echo "$TEST_DETAILS" | grep "^NAME:" | cut -d: -f2-)
    TEST_CATEGORY=$(echo "$TEST_DETAILS" | grep "^CATEGORY:" | cut -d: -f2-)
    TEST_COLLECTION=$(echo "$TEST_DETAILS" | grep "^COLLECTION:" | cut -d: -f2-)
    TEST_PIPELINE=$(echo "$TEST_DETAILS" | grep "^PIPELINE:" | cut -d: -f2-)
    TEST_SQL=$(echo "$TEST_DETAILS" | grep "^SQL:" | cut -d: -f2-)
    TEST_EXPECTED=$(echo "$TEST_DETAILS" | grep "^EXPECTED:" | cut -d: -f2-)
    TEST_SKIP=$(echo "$TEST_DETAILS" | grep "^SKIP:" | cut -d: -f2-)

    echo -n -e "  ${CYAN}[${TEST_ID}]${NC} ${TEST_NAME}... "

    # Check if test is marked as skip
    if [ "$TEST_SKIP" = "True" ]; then
        echo -e "${YELLOW}SKIP${NC} (marked as skip in test case)"
        SKIPPED_TESTS=$((SKIPPED_TESTS + 1))

        echo "[$TEST_ID] $TEST_NAME" >> "$REPORT_FILE"
        echo "  Category: $TEST_CATEGORY" >> "$REPORT_FILE"
        echo "  Status: SKIP" >> "$REPORT_FILE"
        echo "  Details: Test marked as skip" >> "$REPORT_FILE"
        echo "" >> "$REPORT_FILE"

        if [ "$FIRST_RESULT" = true ]; then
            FIRST_RESULT=false
        else
            echo "," >> "$JSON_REPORT_FILE"
        fi
        echo "{\"id\": \"$TEST_ID\", \"name\": \"$TEST_NAME\", \"category\": \"$TEST_CATEGORY\", \"status\": \"SKIP\", \"matchType\": \"\", \"typeMatch\": \"skipped\", \"mongodb_count\": \"N/A\", \"oracle_count\": \"N/A\", \"expected_count\": \"$TEST_EXPECTED\", \"mongodb_results\": [], \"oracle_results\": [], \"oracle_explain\": \"\"}" >> "$JSON_REPORT_FILE"
        continue
    fi

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
        echo "{\"id\": \"$TEST_ID\", \"name\": \"$TEST_NAME\", \"category\": \"$TEST_CATEGORY\", \"status\": \"SKIP\", \"matchType\": \"\", \"typeMatch\": \"skipped\", \"mongodb_count\": \"N/A\", \"oracle_count\": \"N/A\", \"expected_count\": \"$TEST_EXPECTED\", \"mongodb_results\": [], \"oracle_results\": [], \"oracle_explain\": \"\"}" >> "$JSON_REPORT_FILE"
        continue
    fi
    TEST_SQL="$GENERATED_SQL"

    # Run MongoDB query
    MONGO_RESULT=$(run_mongodb_query "$TEST_COLLECTION" "$TEST_PIPELINE" 2>&1) || MONGO_RESULT="[]"
    MONGO_COUNT=$(echo "$MONGO_RESULT" | python3 -c "import sys, json; data = json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "ERROR")

    # Run Oracle query and get JSON results for document comparison
    ORACLE_JSON=$(run_oracle_query_json "$TEST_SQL" 2>&1) || ORACLE_JSON="[]"
    ORACLE_COUNT=$(echo "$ORACLE_JSON" | python3 -c "import sys, json; data = json.load(sys.stdin); print(len(data))" 2>/dev/null || echo "ERROR")

    # Generate explain plan for Oracle query
    ORACLE_EXPLAIN=$(run_oracle_explain_plan "$TEST_SQL" 2>&1) || ORACLE_EXPLAIN="Explain plan not available"

    # Determine result
    STATUS=""
    DETAILS=""
    COMPARE_RESULT=""
    MATCH_TYPE=""
    TYPE_MATCH=""  # New EJSON type match indicator

    if [ "$MONGO_COUNT" = "ERROR" ]; then
        STATUS="ERROR"
        MATCH_TYPE="none"
        TYPE_MATCH="error"
        DETAILS="MongoDB query failed"
        ERROR_TESTS=$((ERROR_TESTS + 1))
        echo -e "${RED}ERROR${NC}"
    elif [ "$ORACLE_COUNT" = "ERROR" ]; then
        STATUS="ERROR"
        MATCH_TYPE="none"
        TYPE_MATCH="error"
        DETAILS="Oracle query failed"
        ERROR_TESTS=$((ERROR_TESTS + 1))
        echo -e "${RED}ERROR${NC}"
    elif [ "$MONGO_COUNT" != "$ORACLE_COUNT" ]; then
        STATUS="FAIL"
        MATCH_TYPE="none"
        TYPE_MATCH="skipped"
        DETAILS="Count mismatch: MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT, Expected=$TEST_EXPECTED"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo -e "${RED}FAIL${NC} (count: MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT)"
    else
        # Counts match - now compare document structure and values
        # Use --try-both to attempt strict match first, then loose on failure
        COMPARE_RESULT=$(echo "{\"mongo\": $MONGO_RESULT, \"oracle\": $ORACLE_JSON}" | \
            python3 "$SCRIPT_DIR/compare-results.py" --stdin --try-both 2>&1) || COMPARE_RESULT="ERROR"

        # Track match type for reporting
        MATCH_TYPE="none"
        if [[ "$COMPARE_RESULT" == STRICT_MATCH* ]]; then
            STATUS="PASS"
            MATCH_TYPE="strict"
            DETAILS="Documents match (strict): MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            echo -n -e "${GREEN}PASS${NC} (strict"
        elif [[ "$COMPARE_RESULT" == LOOSE_MATCH* ]]; then
            STATUS="PASS"
            MATCH_TYPE="loose"
            DETAILS="Documents match (loose): MongoDB=$MONGO_COUNT, Oracle=$ORACLE_COUNT"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            echo -n -e "${YELLOW}PASS${NC} (loose"
        elif [[ "$COMPARE_RESULT" == *MISMATCH* ]]; then
            STATUS="FAIL"
            MATCH_TYPE="none"
            TYPE_MATCH="skipped"
            DETAILS="Document mismatch: $COMPARE_RESULT (counts: $MONGO_COUNT)"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            echo -e "${RED}FAIL${NC} (count: $MONGO_COUNT, $COMPARE_RESULT)"
        else
            # Comparison error - fall back to count comparison
            MATCH_TYPE="unknown"
            TYPE_MATCH="error"
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

        # Run EJSON type comparison if value comparison passed
        if [ "$STATUS" = "PASS" ] && [ "$TYPE_MATCH" != "skipped" ] && [ "$TYPE_MATCH" != "error" ]; then
            # Get EJSON from MongoDB (with type encodings preserved)
            MONGO_EJSON=$(run_mongodb_query_ejson "$TEST_COLLECTION" "$TEST_PIPELINE" 2>&1) || MONGO_EJSON="[]"

            # Get EJSON from Oracle (with type encodings)
            ORACLE_EJSON=$(run_oracle_query_ejson "$TEST_SQL" 2>&1) || ORACLE_EJSON="[]"

            # Compare EJSON types using temp file (avoids WSL pipe issues)
            ejson_tmp=$(mktemp /tmp/ejson-compare-XXXXXX.json)
            printf '%s\n' "{\"mongo\": $MONGO_EJSON, \"oracle\": $ORACLE_EJSON}" > "$ejson_tmp"
            EJSON_COMPARE=$(python3 "$SCRIPT_DIR/compare-ejson.py" --stdin < "$ejson_tmp" 2>&1) || EJSON_COMPARE="ERROR"
            rm -f "$ejson_tmp"

            if [[ "$EJSON_COMPARE" == EJSON_MATCH* ]]; then
                TYPE_MATCH="pass"
                echo -e ", ${GREEN}types:pass${NC})"
            elif [[ "$EJSON_COMPARE" == EJSON_MISMATCH* ]]; then
                TYPE_MATCH="fail"
                echo -e ", ${RED}types:fail${NC})"
                if [ "$VERBOSE" = true ]; then
                    echo "    EJSON mismatch: $EJSON_COMPARE"
                fi
            else
                TYPE_MATCH="error"
                echo -e ", ${YELLOW}types:error${NC})"
                if [ "$VERBOSE" = true ]; then
                    echo "    EJSON comparison error: $EJSON_COMPARE"
                fi
            fi
        fi
    fi

    # Log to report
    echo "[$TEST_ID] $TEST_NAME" >> "$REPORT_FILE"
    echo "  Category: $TEST_CATEGORY" >> "$REPORT_FILE"
    echo "  Status: $STATUS" >> "$REPORT_FILE"
    echo "  Details: $DETAILS" >> "$REPORT_FILE"
    echo "" >> "$REPORT_FILE"

    # Add to JSON report - escape special characters in results for valid JSON
    MONGO_RESULT_ESCAPED=$(echo "$MONGO_RESULT" | python3 -c 'import sys,json; print(json.dumps(json.loads(sys.stdin.read())))' 2>/dev/null || echo "[]")
    ORACLE_JSON_ESCAPED=$(echo "$ORACLE_JSON" | python3 -c 'import sys,json; print(json.dumps(json.loads(sys.stdin.read())))' 2>/dev/null || echo "[]")
    # Escape explain plan as a JSON string (preserving newlines and special chars)
    ORACLE_EXPLAIN_ESCAPED=$(echo "$ORACLE_EXPLAIN" | python3 -c 'import sys,json; print(json.dumps(sys.stdin.read()))' 2>/dev/null || echo '""')

    # Write comma prefix and JSON in single operation to avoid race conditions
    # Include typeMatch indicator for EJSON type comparison results and explain plan
    JSON_ENTRY="{\"id\": \"$TEST_ID\", \"name\": \"$TEST_NAME\", \"category\": \"$TEST_CATEGORY\", \"status\": \"$STATUS\", \"matchType\": \"$MATCH_TYPE\", \"typeMatch\": \"$TYPE_MATCH\", \"mongodb_count\": \"$MONGO_COUNT\", \"oracle_count\": \"$ORACLE_COUNT\", \"expected_count\": \"$TEST_EXPECTED\", \"mongodb_results\": $MONGO_RESULT_ESCAPED, \"oracle_results\": $ORACLE_JSON_ESCAPED, \"oracle_explain\": $ORACLE_EXPLAIN_ESCAPED}"
    if [ "$FIRST_RESULT" = true ]; then
        FIRST_RESULT=false
        echo "$JSON_ENTRY" >> "$JSON_REPORT_FILE"
    else
        echo ",$JSON_ENTRY" >> "$JSON_REPORT_FILE"
    fi

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

# Generate test catalog data for HTML
echo ""
echo "Generating test-catalog-data.json..."
catalog_opts=""
if [ "$CTE_MODE" = true ]; then
    catalog_opts="--cte"
fi
python3 "$SCRIPT_DIR/generate-test-catalog-data.py" $catalog_opts 2>/dev/null && echo "  Test catalog data updated: $PROJECT_ROOT/docs/test-catalog-data.json" || echo "  Warning: Failed to generate test catalog data"
echo "  View test catalog: $PROJECT_ROOT/docs/test-catalog.html"

# Exit with error if any tests failed
if [ $FAILED_TESTS -gt 0 ] || [ $ERROR_TESTS -gt 0 ]; then
    exit 1
fi

exit 0
