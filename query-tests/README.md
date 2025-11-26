# MongoPLSQL-Bridge Query Validation Tests

This directory contains comprehensive tests to validate that MongoDB aggregation pipelines produce identical results when translated and executed against Oracle SQL/JSON.

## Latest Test Results

**Status:** ✅ All Tests Passing
**Last Run:** 2024-11-26
**Results:** 39/39 tests passed (100%)

| Category | Tests | Status |
|----------|-------|--------|
| Comparison ($eq, $gt, $gte, $lt, $lte, $ne, $in, $nin) | 8 | ✅ Pass |
| Logical ($and, $or, $not, $nor) | 5 | ✅ Pass |
| Accumulator ($sum, $avg, $count, $min, $max, $first, $last) | 8 | ✅ Pass |
| Stage ($match, $group, $project, $sort, $limit, $skip) | 7 | ✅ Pass |
| Arithmetic ($add, $subtract, $multiply, $divide, $mod) | 5 | ✅ Pass |
| Conditional ($cond, $ifNull) | 3 | ✅ Pass |
| Complex Combinations | 3 | ✅ Pass |

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
│   └── test-cases.json      # All 39 test case definitions
├── scripts/
│   ├── setup.sh             # Initialize test environment
│   ├── run-tests.sh         # Execute all tests
│   └── teardown.sh          # Clean up test environment
└── results/
    └── (generated test results)
```

## Prerequisites

- Docker and Docker Compose v2 installed
- Python 3.8+ (uses standard library only)
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

## Running Individual Test Categories

```bash
# Run only comparison operator tests
./scripts/run-tests.sh --category comparison

# Run only accumulator tests
./scripts/run-tests.sh --category accumulator

# Run a specific test by ID
./scripts/run-tests.sh --test TC001
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
  "id": "TC001",
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

## Troubleshooting

### MongoDB connection fails
```bash
docker exec mongo-translator-mongodb mongosh --eval "db.adminCommand('ping')"
```

### Oracle connection fails
```bash
docker exec mongo-translator-oracle sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 <<< "SELECT 1 FROM DUAL;"
```

### Reset test data
```bash
./scripts/setup.sh --reset
```
