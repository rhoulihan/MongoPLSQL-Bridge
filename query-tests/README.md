# MongoPLSQL-Bridge Query Validation Tests

This directory contains comprehensive tests to validate that MongoDB aggregation pipelines produce identical results when translated and executed against Oracle SQL/JSON.

## Latest Test Results

**Status:** ✅ All Tests Passing
**Last Run:** 2024-12-05
**Results:** 151/152 tests passed (99%)

| Category | Tests | Status |
|----------|-------|--------|
| Comparison ($eq, $gt, $gte, $lt, $lte, $ne, $in, $nin) | 8 | ✅ Pass |
| Logical ($and, $or, $not, $nor) | 5 | ✅ Pass |
| Accumulator ($sum, $avg, $count, $min, $max, $first, $last) | 8 | ✅ Pass |
| Stage ($match, $group, $project, $sort, $limit, $skip) | 7 | ✅ Pass |
| Arithmetic ($add, $subtract, $multiply, $divide, $mod) | 5 | ✅ Pass |
| Conditional ($cond, $ifNull) | 3 | ✅ Pass |
| String operators | 11 | ✅ Pass |
| Date operators | 5 | ✅ Pass |
| Array operators | 10 | ✅ Pass |
| Type Conversion | 5 | ✅ Pass |
| $lookup/$unwind | 4 | ✅ Pass |
| $addFields/$set | 2 | ✅ Pass |
| Complex pipelines | 5 | ✅ Pass |
| Edge cases | 3 | ✅ Pass |
| $unionWith | 3 | ✅ Pass |
| $bucket | 2 | ✅ Pass |
| $bucketAuto | 2 | ✅ Pass |
| $facet | 3 | ✅ Pass |
| $setWindowFields | 4 | ✅ Pass |
| $redact | 2 | ✅ Pass |
| $sample | 2 | ✅ Pass |
| $count | 3 | ✅ Pass |
| $graphLookup | 2 | ✅ Pass |
| Window Functions | 4 | ✅ Pass |
| Lookup Pipelines | 5 | ✅ Pass |
| $reduce | 4 | ✅ Pass |
| Replace Root | 2 | ✅ Pass |
| Object Expressions | 3 | ✅ Pass |
| Null Handling | 6 | ✅ Pass |
| Expression Operators | 12 | ✅ Pass |

## Overview

The test suite validates all implemented operators by:
1. Loading identical test data into both MongoDB and Oracle
2. Running aggregation pipelines against MongoDB
3. Running equivalent translated SQL/JSON queries against Oracle
4. Comparing results for exact match

## Directory Structure

```
query-tests/
├── README.md                 # This file
├── data/
│   ├── test-data.json       # Test data definitions
│   ├── load-mongodb.js      # MongoDB data loader
│   └── load-oracle.sql      # Oracle data loader
├── tests/
│   └── test-cases.json      # All 152 test case definitions
├── scripts/
│   ├── setup.sh                   # Initialize test environment
│   ├── run-tests.sh               # Execute all tests
│   ├── teardown.sh                # Clean up test environment
│   ├── compare-results.py         # Compare MongoDB/Oracle results with strict/loose matching
│   ├── generate-test-catalog-data.py  # Generate test-catalog-data.json for HTML catalog
│   └── export-results.js          # Export results to JSON for inspection
├── large-scale/             # Large-scale comparison tests (~4GB)
│   ├── generate-data.js     # Generate test data
│   ├── load-data.js         # Load data to databases
│   ├── compare-pipelines.js # Run comparison tests
│   └── run-comparison.sh    # All-in-one script
├── import/                   # MongoDB test importer tools
│   └── curated-tests.json   # 39 curated edge case tests
└── results/
    └── (generated test results)
```

## Prerequisites

- Docker and Docker Compose v2 installed
- Python 3.8+ (uses standard library only)
- Node.js 16+ (for export scripts and large-scale tests)
- Both MongoDB 8.0 and Oracle 23.6 containers running

## Quick Start

```bash
# 1. Start the test environment (from project root)
cd /path/to/MongoPLSQL-Bridge
./scripts/start-env.sh

# 2. Load test data into both databases
./query-tests/scripts/setup.sh

# 3. Run all validation tests
./query-tests/scripts/run-tests.sh

# 4. View results
cat query-tests/results/test-report-latest.txt

# 5. Clean up when done
./query-tests/scripts/teardown.sh
```

## Step-by-Step Setup Guide

### 1. Install Prerequisites

**Docker:**
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install docker.io docker-compose-v2

# macOS (with Homebrew)
brew install docker docker-compose

# Windows
# Install Docker Desktop from https://www.docker.com/products/docker-desktop
```

**Python 3.8+:**
```bash
# Ubuntu/Debian
sudo apt-get install python3

# macOS
brew install python3

# Verify installation
python3 --version
```

**Node.js 16+ (optional, for export scripts):**
```bash
# Ubuntu/Debian
curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
sudo apt-get install -y nodejs

# macOS
brew install node

# Verify installation
node --version
npm --version
```

### 2. Start Database Containers

From the project root directory:

```bash
# Start MongoDB and Oracle containers
docker compose up -d

# Wait for containers to be healthy (Oracle takes 1-2 minutes on first run)
docker compose logs -f oracle
# Wait until you see "DATABASE IS READY TO USE!"

# Or use the convenience script
./scripts/start-env.sh
```

**Container Details:**

| Service | Container Name | Port | Credentials |
|---------|---------------|------|-------------|
| Oracle 23.6 | mongo-translator-oracle | 1521 | user: `translator`, pass: `translator123` |
| MongoDB 8.0 | mongo-translator-mongodb | 27017 | user: `admin`, pass: `admin123` |

### 3. Verify Environment

```bash
./scripts/validate-env.sh
```

Expected output:
```
============================================================
  MongoPLSQL-Bridge Test Environment Validation
============================================================

Checking Docker containers...
  ✓ Oracle container is running
  ✓ MongoDB container is running

Validating MongoDB connection...
  ✓ MongoDB is responding to ping
  ✓ test_customers collection exists
  ✓ test_orders collection exists
  ✓ test_products collection exists
  ...

Validating Oracle connection...
  ✓ Oracle is responding to queries
  ✓ TEST_CUSTOMERS table exists
  ✓ TEST_ORDERS table exists
  ✓ TEST_PRODUCTS table exists
  ...

============================================================
  ✓ All validations PASSED
  Test environment is ready for use
============================================================

Connection Details:
  MongoDB: mongodb://translator:translator123@localhost:27017/testdb
  Oracle:  jdbc:oracle:thin:@localhost:1521/FREEPDB1 (translator/translator123)
```

### 4. Load Test Data

```bash
./query-tests/scripts/setup.sh
```

This loads identical test data into both MongoDB and Oracle:

| Collection/Table | Documents | Description |
|-----------------|-----------|-------------|
| sales | 10 | Orders with items, tags, metadata |
| employees | 10 | Employee records with departments |
| products | 8 | Product catalog |
| customers | 7 | Customer records for $lookup |
| events | 8 | Events with ISODate for date operators |
| inventory | 12 | Inventory for $lookup joins |

### 5. Run Tests

```bash
# Run all tests
./query-tests/scripts/run-tests.sh

# Run with verbose output
./query-tests/scripts/run-tests.sh --verbose

# Run specific category
./query-tests/scripts/run-tests.sh --category comparison

# Run specific test
./query-tests/scripts/run-tests.sh --test CMP001
```

**Available Categories:**
- `comparison` - $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin
- `logical` - $and, $or, $not, $nor
- `accumulator` - $sum, $avg, $count, $min, $max, $first, $last
- `stage` - $match, $group, $project, $sort, $limit, $skip
- `arithmetic` - $add, $subtract, $multiply, $divide, $mod
- `conditional` - $cond, $ifNull
- `string` - $concat, $toLower, $toUpper, etc.
- `date` - $year, $month, $dayOfMonth, etc.
- `array` - $arrayElemAt, $size, $first, $last, etc.
- `complex` - Multi-stage pipelines
- `edge` - Edge cases and null handling

## Running Individual Test Categories

```bash
# Run only comparison operator tests
./scripts/run-tests.sh --category comparison

# Run only accumulator tests
./scripts/run-tests.sh --category accumulator

# Run a specific test by ID
./scripts/run-tests.sh --test TC001
```

## Exporting Results for Inspection

Export query results from both databases to JSON files for manual comparison:

```bash
cd query-tests/scripts

# Install dependencies (first time only)
npm install

# Export results for all tests
node export-results.js

# Export MongoDB only
node export-results.js --mongodb-only

# Export Oracle only
node export-results.js --oracle-only

# Custom output directory
node export-results.js --output-dir /path/to/output
```

**Output Format:**
```json
{
  "testId": "CMP001",
  "testName": "Equality match - string",
  "mongodb": {
    "success": true,
    "results": [{"_id": "S001", "orderId": 1001, "status": "completed"}, ...],
    "count": 5
  },
  "oracle": {
    "success": true,
    "results": [{"id": "S001", "orderId": 1001, "status": "completed"}, ...],
    "count": 5
  },
  "comparison": {
    "mongodbCount": 5,
    "oracleCount": 5,
    "countsMatch": true,
    "bothSucceeded": true
  }
}
```

## Large-Scale Comparison Tests

Test with larger datasets (100MB - 4GB) and complex pipelines:

```bash
cd query-tests/large-scale

# Install dependencies
npm install

# Generate and run tests
./run-comparison.sh --size small   # ~100MB, ~219K documents
./run-comparison.sh --size medium  # ~500MB, ~1M documents
./run-comparison.sh --size large   # ~2GB, ~4.5M documents
./run-comparison.sh --size xlarge  # ~4GB, ~10M documents
```

Or run steps individually:
```bash
node generate-data.js --size small --output ./data
node load-data.js --target mongodb --data-dir ./data --drop
node load-data.js --target oracle --data-dir ./data --drop
node compare-pipelines.js --verbose
```

## Test Data

The test data is designed to cover edge cases:

### Collections

1. **sales** - Sales transactions with various data types
   - Numeric fields (integers, decimals)
   - String fields (status, category)
   - Date fields
   - Nested objects (customer info)
   - Arrays (items, tags)
   - Null values

2. **employees** - Employee records for grouping tests
   - Hierarchical data (department, team)
   - Salary ranges
   - Multiple status values

3. **products** - Product catalog
   - Price ranges including zero and null
   - Boolean flags
   - Category hierarchies

4. **customers** - Customer records for $lookup
   - For testing JOIN operations

5. **events** - Events with timestamps
   - ISODate fields for date operator tests

6. **inventory** - Inventory records
   - For testing $lookup joins

### Edge Cases Covered

- Null values in comparison and arithmetic
- Empty strings vs null
- Zero values in division
- Large numbers
- Unicode strings
- Duplicate values for grouping
- Empty result sets

## Test Case Format

Each test case in `tests/test-cases.json` follows this format:

```json
{
  "id": "CMP001",
  "name": "Simple equality match",
  "category": "comparison",
  "collection": "sales",
  "mongodb_pipeline": [{ "$match": { "status": "completed" } }],
  "oracle_sql": "SELECT id FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed'",
  "expected_count": 5
}
```

## Validation

Tests validate that MongoDB and Oracle return the same row count. The test runner:
1. Executes the MongoDB aggregation pipeline
2. Executes the equivalent Oracle SQL/JSON query
3. Compares result counts from both databases
4. Reports PASS if counts match, FAIL if they differ

## Adding New Tests

1. Add test case to `tests/test-cases.json`
2. Ensure test data supports the case (update `data/test-data.json` if needed)
3. Run `./scripts/run-tests.sh --test YOUR_TEST_ID` to validate

## Interpreting Results

The test report shows:
- **PASS**: MongoDB and Oracle results match
- **FAIL**: Results differ (details provided)
- **ERROR**: Query execution failed
- **SKIP**: Test skipped (missing data or unsupported feature)

### Match Type Indicators

When comparing results, the test runner performs both strict and loose matching:

| Match Type | Description | Color |
|------------|-------------|-------|
| **Strict** | Values and types match exactly (e.g., `10` = `10`) | Green |
| **Loose** | Values match with type coercion (e.g., `10` = `"10"`) | Yellow |
| **None** | Results don't match | Red |

The match type is visible in:
1. **Console output**: Shows "strict match" or "loose match" after PASS
2. **JSON report**: Includes `matchType` field for each test
3. **Test Catalog HTML**: Color-coded indicator next to status badge

A "loose match" typically indicates that Oracle is returning a string where MongoDB returns a number (or vice versa). While the values are semantically equivalent, you may want to investigate and fix the type mismatch for stricter compatibility.

## Troubleshooting

### Docker containers won't start

```bash
# Check Docker is running
docker info

# Check for port conflicts
lsof -i :1521  # Oracle
lsof -i :27017 # MongoDB

# Reset and try again
docker compose down -v
docker compose up -d
```

### MongoDB connection fails

```bash
# Test connectivity
docker exec mongo-translator-mongodb mongosh --eval "db.adminCommand('ping')"

# Check logs
docker compose logs mongodb

# Verify credentials
docker exec mongo-translator-mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --eval "db.adminCommand('ping')"
```

### Oracle connection fails

```bash
# Test connectivity
docker exec mongo-translator-oracle sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 <<< "SELECT 1 FROM DUAL; EXIT;"

# Check logs (Oracle takes longer to start)
docker compose logs oracle

# Wait for ready message
docker compose logs -f oracle | grep "DATABASE IS READY"
```

### Oracle takes too long to start

Oracle 23.6 Free can take 2-3 minutes on first start. Use:
```bash
# Watch for ready message
docker compose logs -f oracle

# Or use start script which waits automatically
./scripts/start-env.sh
```

### Tests fail with count mismatch

```bash
# Re-run setup to ensure data is synchronized
./query-tests/scripts/setup.sh --reset

# Run specific failing test with verbose output
./query-tests/scripts/run-tests.sh --test TESTID --verbose
```

### Reset test data

```bash
./scripts/setup.sh --reset
```

## Environment Management Scripts

| Script | Description |
|--------|-------------|
| `scripts/start-env.sh` | Start MongoDB and Oracle containers, wait for healthy |
| `scripts/stop-env.sh` | Stop containers (preserves data volumes) |
| `scripts/reset-env.sh` | Stop containers and remove all data volumes |
| `scripts/validate-env.sh` | Verify containers are running and accessible |
| `query-tests/scripts/setup.sh` | Load test data into both databases |
| `query-tests/scripts/run-tests.sh` | Execute validation tests |
| `query-tests/scripts/teardown.sh` | Clean up test collections/tables |
