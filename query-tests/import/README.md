# MongoDB Test Importer

This directory contains tools for importing and generating test cases from MongoDB's official aggregation tests.

## Tools

### 1. `mongodb-test-importer.js`

Parses MongoDB jstests JavaScript files and extracts aggregation pipelines into our JSON test format.

```bash
# List available operators
node mongodb-test-importer.js --list-operators

# Fetch and import tests from MongoDB repo
node mongodb-test-importer.js --fetch size,arrayElemAt,cond --output imported.json

# Import from local file
node mongodb-test-importer.js ./path/to/jstest.js --output tests.json

# Import from directory
node mongodb-test-importer.js ./jstests/ --output all-tests.json
```

**Features:**
- Fetches tests directly from MongoDB GitHub repository
- Parses JavaScript test patterns (assert.eq, assertResult, etc.)
- Extracts test data (insertMany/insertOne calls)
- Filters for supported operators only
- Generates our JSON test format

### 2. `curated-mongodb-tests.js`

Generates well-structured test cases based on patterns found in MongoDB's official jstests. These are hand-curated to ensure they work with our Oracle translation.

```bash
# Generate curated test cases (JSON)
node curated-mongodb-tests.js --output curated-tests.json

# Generate MongoDB data loader
node curated-mongodb-tests.js --mongo-loader > load-mongodb.js

# Generate Oracle SQL loader
node curated-mongodb-tests.js --oracle-loader > load-oracle.sql
```

**Test Categories:**
- **String operators**: `$concat`, `$toLower`, `$toUpper`, `$substr`, `$strLenCP`, `$trim`
- **Array operators**: `$size`, `$arrayElemAt`, `$first`, `$last`
- **Conditional operators**: `$cond`, `$ifNull`
- **Date operators**: `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dayOfWeek`
- **Arithmetic operators**: `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`
- **Accumulators**: `$sum`, `$avg`, `$min`, `$max`, `$push`, `$addToSet`, `$count`

**Edge Cases Covered:**
- Null and missing field handling
- Empty arrays and strings
- Negative array indices
- Out-of-bounds access
- Type coercion
- Nested conditions

## Generated Files

| File | Description |
|------|-------------|
| `curated-tests.json` | Test cases in our standard format |
| `curated-mongodb-loader.js` | MongoDB data loader script |
| `curated-oracle-loader.sql` | Oracle SQL data loader |
| `imported-tests.json` | Tests imported from MongoDB repo |

## Test Format

Generated tests follow this structure:

```json
{
  "id": "CUR001",
  "name": "$concat - basic string concatenation",
  "category": "string",
  "operator": "$concat",
  "description": "Curated test from MongoDB patterns",
  "collection": "string_tests",
  "mongodb_pipeline": [...],
  "oracle_sql": null,
  "expected_result": [...],
  "expected_count": 5,
  "sort_by": "_id"
}
```

## Integration

To integrate curated tests into the main test suite:

1. Generate the test files:
   ```bash
   node curated-mongodb-tests.js --output curated-tests.json
   ```

2. Merge into main test cases:
   ```bash
   # Manual merge or use jq
   jq -s '.[0].test_cases + .[1].test_cases' \
       ../tests/test-cases.json curated-tests.json > merged.json
   ```

3. Load test data:
   ```bash
   # MongoDB
   mongosh < curated-mongodb-loader.js

   # Oracle
   sqlplus user/pass@db @curated-oracle-loader.sql
   ```

## MongoDB Test Sources

Tests are derived from:
- https://github.com/mongodb/mongo/tree/master/jstests/aggregation/expressions/
- https://github.com/mongodb/mongo/tree/master/jstests/aggregation/accumulators/
- https://github.com/mongodb/mongo/tree/master/jstests/aggregation/sources/

## Adding New Tests

To add more curated tests, edit `curated-mongodb-tests.js` and add entries to the `CURATED_TESTS` object:

```javascript
const CURATED_TESTS = {
    newCategory: {
        collection: 'new_tests',
        test_data: [
            { _id: 1, field: 'value' },
            // ...
        ],
        tests: [
            {
                name: 'Test description',
                operator: '$operator',
                pipeline: [{ $project: { result: { $operator: '$field' } } }],
                expected: [{ result: 'expected' }]
            }
        ]
    }
};
```
