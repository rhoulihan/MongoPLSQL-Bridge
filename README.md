# MongoDB to Oracle SQL Translator

A Java library that translates MongoDB aggregation framework pipelines into equivalent Oracle SQL/JSON statements for execution on Oracle 23ai/26ai JSON Collections.

## Overview

This library provides a MongoDB-style `aggregate()` API while generating Oracle SQL under the hood, enabling applications to:

- Execute MongoDB aggregation pipelines against Oracle Database
- Leverage Oracle's JSON Collection features with familiar MongoDB syntax
- Gradually migrate from MongoDB to Oracle without rewriting application logic

## Current Status

**Phase 4 Complete** - All operators implemented with 90% test coverage.

### Implemented Operators

**Stage Operators:**
- `$match` - WHERE clause with JSON_VALUE/JSON_EXISTS
- `$group` - GROUP BY with aggregate functions
- `$project` - SELECT with field selection/computation
- `$sort` - ORDER BY clause (with Top-N optimization)
- `$limit` - FETCH FIRST n ROWS ONLY
- `$skip` - OFFSET n ROWS
- `$lookup` - LEFT OUTER JOIN
- `$unwind` - JSON_TABLE with NESTED PATH
- `$addFields`/`$set` - Computed columns

**Expression Operators:**
- Comparison: `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$in`, `$nin`
- Logical: `$and`, `$or`, `$not`, `$nor`
- Arithmetic: `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`
- Conditional: `$cond`, `$ifNull`
- String: `$concat`, `$toLower`, `$toUpper`, `$substr`, `$trim`, `$ltrim`, `$rtrim`, `$strLenCP`, `$split`, `$indexOfCP`, `$regexMatch`, `$regexFind`, `$replaceOne`, `$replaceAll`
- Date: `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dayOfWeek`, `$dayOfYear`
- Array: `$arrayElemAt`, `$size`, `$first`, `$last`, `$filter`, `$map`, `$reduce`, `$concatArrays`, `$slice`
- Type Conversion: `$type`, `$toInt`, `$toString`, `$toDouble`, `$toBool`, `$toDate`

**Accumulator Operators:**
- `$sum`, `$avg`, `$count`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`

**Pipeline Optimization:**
- Predicate pushdown (moves `$match` early)
- Sort-limit combination (Oracle Top-N optimization)
- Configurable optimization chain

### Validation Status

All 102 cross-database validation tests pass (MongoDB 8.0 ↔ Oracle 23.6). See [query-tests/](query-tests/) for details.

**Test Categories:** Comparison (8), Logical (5), Accumulator (8), Stage (7), Arithmetic (5), Conditional (3), String (11), Date (5), Array (10), Type Conversion (5), $lookup/$unwind (4), $addFields (2), Complex (5), Edge cases (3), $unionWith (3), $bucket (2), $bucketAuto (2), $facet (3), $setWindowFields (4), $redact (2), $sample (2), $count (3), $graphLookup (1)

### Test Coverage

| Package | Line Coverage | Branch Coverage |
|---------|---------------|-----------------|
| Overall | **90%** | **78%** |
| `api` | 97% | 100% |
| `generator` | 96% | 93% |
| `optimizer` | 92% | 86% |
| `ast.expression` | 91% | 71% |
| `parser` | 87% | 81% |
| `ast.stage` | 86% | 69% |
| `exception` | 100% | 100% |
| `generator.dialect` | 100% | n/a |

### Additional Stages Implemented

- `$unionWith` - UNION ALL
- `$bucket` - CASE expression grouping
- `$bucketAuto` - NTILE-based automatic bucketing
- `$facet` - Multiple parallel subqueries (JSON_OBJECT)
- `$graphLookup` - Recursive CTE for hierarchical queries (with restrictSearchWithMatch)
- `$setWindowFields` - Window functions (RANK, DENSE_RANK, ROW_NUMBER, SUM, AVG, etc.)
- `$redact` - Document-level filtering ($$PRUNE/$$KEEP/$$DESCEND)
- `$sample` - Random sampling (DBMS_RANDOM.VALUE)
- `$count` - Document count (JSON_OBJECT output)
- `$merge` - MERGE statement (stub)
- `$out` - INSERT statement (stub)

## Requirements

- Java 17 or higher
- Oracle Database 23ai or 26ai with JSON Collections
- Oracle JDBC Driver 23.3+

## Quick Start

### Installation

**Maven:**
```xml
<dependency>
    <groupId>com.oracle.mongodb</groupId>
    <artifactId>mongo-oracle-translator</artifactId>
    <version>1.0.0</version>
</dependency>
```

**Gradle:**
```kotlin
implementation("com.oracle.mongodb:mongo-oracle-translator:1.0.0")
```

### Basic Usage

```java
import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import org.bson.Document;

// Configure the translator
var config = OracleConfiguration.builder()
    .collectionName("orders")
    .build();

var translator = AggregationTranslator.create(config);

// Define a MongoDB aggregation pipeline (currently supports $limit and $skip)
var pipeline = List.of(
    Document.parse("{\"$skip\": 10}"),
    Document.parse("{\"$limit\": 5}")
);

// Translate to Oracle SQL
var result = translator.translate(pipeline);

System.out.println(result.sql());
// Output:
// SELECT data FROM orders OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY

// Execute against Oracle
try (PreparedStatement ps = connection.prepareStatement(result.sql())) {
    for (int i = 0; i < result.bindVariables().size(); i++) {
        ps.setObject(i + 1, result.bindVariables().get(i));
    }
    ResultSet rs = ps.executeQuery();
    // Process results...
}
```

## Supported Operators

### Stage Operators

| Operator | Status | Oracle Translation |
|----------|--------|-------------------|
| `$match` | ✅ Implemented | WHERE clause with JSON_VALUE/JSON_EXISTS |
| `$group` | ✅ Implemented | GROUP BY with aggregate functions |
| `$project` | ✅ Implemented | SELECT with field selection/computation |
| `$sort` | ✅ Implemented | ORDER BY clause (with Top-N optimization) |
| `$limit` | ✅ Implemented | FETCH FIRST n ROWS ONLY |
| `$skip` | ✅ Implemented | OFFSET n ROWS |
| `$lookup` | ✅ Implemented | LEFT OUTER JOIN |
| `$unwind` | ✅ Implemented | JSON_TABLE with NESTED PATH |
| `$addFields`/`$set` | ✅ Implemented | Computed columns |
| `$unionWith` | ✅ Implemented | UNION ALL |
| `$bucket` | ✅ Implemented | CASE expressions |
| `$bucketAuto` | ✅ Implemented | NTILE window function |
| `$facet` | ✅ Implemented | Multiple subqueries (JSON_OBJECT) |
| `$graphLookup` | ✅ Implemented | Recursive CTE |
| `$setWindowFields` | ✅ Implemented | Window functions |
| `$redact` | ✅ Implemented | Conditional WHERE clause |
| `$sample` | ✅ Implemented | ORDER BY DBMS_RANDOM.VALUE |
| `$count` | ✅ Implemented | SELECT JSON_OBJECT(... COUNT(*)) |
| `$merge` | ✅ Stub | MERGE statement |
| `$out` | ✅ Stub | INSERT statement |

### Expression Operators

| Category | Operators | Status |
|----------|-----------|--------|
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin` | ✅ Implemented |
| Logical | `$and`, `$or`, `$not`, `$nor` | ✅ Implemented |
| Arithmetic | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod` | ✅ Implemented |
| Conditional | `$cond`, `$ifNull` | ✅ Implemented |
| String | `$concat`, `$toLower`, `$toUpper`, `$substr`, `$trim`, `$ltrim`, `$rtrim`, `$strLenCP`, `$split`, `$indexOfCP`, `$regexMatch`, `$regexFind`, `$replaceOne`, `$replaceAll` | ✅ Implemented |
| Date | `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dayOfWeek`, `$dayOfYear` | ✅ Implemented |
| Array | `$arrayElemAt`, `$size`, `$first`, `$last`, `$filter`, `$map`, `$reduce`, `$concatArrays`, `$slice` | ✅ Implemented |
| Type Conversion | `$type`, `$toInt`, `$toString`, `$toDouble`, `$toBool`, `$toDate` | ✅ Implemented |

### Accumulator Operators

| Operator | Status | Oracle Equivalent |
|----------|--------|------------------|
| `$sum` | ✅ Implemented | `SUM()` |
| `$avg` | ✅ Implemented | `AVG()` |
| `$count` | ✅ Implemented | `COUNT(*)` |
| `$min` | ✅ Implemented | `MIN()` |
| `$max` | ✅ Implemented | `MAX()` |
| `$first` | ✅ Implemented | `FIRST_VALUE()` |
| `$last` | ✅ Implemented | `LAST_VALUE()` |
| `$push` | ✅ Implemented | `JSON_ARRAYAGG()` |
| `$addToSet` | ✅ Implemented | `JSON_ARRAYAGG(DISTINCT)` |

## Configuration Options

```java
var options = TranslationOptions.builder()
    .inlineBindVariables(false)  // Use bind variables (default)
    .prettyPrint(true)           // Format generated SQL
    .includeHints(true)          // Add Oracle optimizer hints
    .strictMode(false)           // Fail on unsupported operators
    .dataColumnName("data")      // Name of JSON column (default: "data")
    .build();

var translator = AggregationTranslator.create(config, options);
var result = translator.translate(pipeline);
```

## Building and Testing

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| JDK | 17+ | Required for building and running |
| Docker | 20.10+ | Required for test databases |
| Docker Compose | 2.0+ | Required for test environment |
| Git | 2.30+ | For cloning the repository |
| Node.js | 16+ | Optional, for large-scale tests only |

### Quick Start (Build Only)

If you just want to build the library without running integration tests:

```bash
git clone https://github.com/rhoulihan/MongoPLSQL-Bridge.git
cd MongoPLSQL-Bridge
./gradlew build -x test
```

The built JAR will be at `core/build/libs/core.jar`.

### Full Build with Unit Tests

```bash
git clone https://github.com/rhoulihan/MongoPLSQL-Bridge.git
cd MongoPLSQL-Bridge
./gradlew build
```

This runs all unit tests (749 tests) and produces the library JAR.

### Setting Up the Test Environment

The test environment uses Docker to run MongoDB 8.0 and Oracle 23.6 Free:

```bash
# Start both databases
docker compose up -d

# Wait for Oracle to be ready (takes 1-2 minutes on first run)
docker compose logs -f oracle
# Wait until you see "DATABASE IS READY TO USE!"

# Alternatively, use the validation script
./scripts/validate-env.sh
```

**Container Details:**

| Service | Container Name | Port | Credentials |
|---------|---------------|------|-------------|
| Oracle 23.6 | mongo-translator-oracle | 1521 | user: `translator`, pass: `translator123` |
| MongoDB 8.0 | mongo-translator-mongodb | 27017 | user: `admin`, pass: `admin123` |

**Environment Management Scripts:**

```bash
./scripts/start-env.sh    # Start databases
./scripts/stop-env.sh     # Stop databases (preserves data)
./scripts/reset-env.sh    # Stop and remove all data
./scripts/validate-env.sh # Check database health
```

### Running Tests

#### Unit Tests (No Docker Required)

```bash
./gradlew :core:test
```

Runs 749 unit tests covering all operators and parsers.

#### Integration Tests (Requires Docker)

```bash
# Start test environment first
docker compose up -d
./scripts/validate-env.sh

# Run integration tests
./gradlew :integration-tests:test -PrunIntegrationTests
```

Tests actual SQL execution against Oracle database.

#### Cross-Database Validation Tests (Requires Docker)

These tests execute the same aggregation pipelines against both MongoDB and Oracle, comparing results:

```bash
# Start test environment
docker compose up -d
./scripts/validate-env.sh

# Load test data and run validation
./query-tests/scripts/setup.sh
./query-tests/scripts/run-tests.sh
```

**Expected Output:**
```
Running 102 cross-database validation tests...
✅ PASS: CMP001 - Basic equality match
✅ PASS: CMP002 - Greater than comparison
...
============================================
Results: 102 passed, 0 failed, 0 errors
============================================
```

#### Large-Scale Comparison Tests (Optional)

Test with larger datasets (100MB - 4GB) and complex pipelines:

```bash
cd query-tests/large-scale

# Install Node.js dependencies
npm install

# Generate test data and run comparison
./run-comparison.sh --size small   # ~100MB, ~219K documents
./run-comparison.sh --size medium  # ~500MB, ~1M documents
./run-comparison.sh --size large   # ~2GB, ~4.5M documents
./run-comparison.sh --size xlarge  # ~4GB, ~10M documents
```

Or run steps individually:

```bash
node generate-data.js --size small --output ./data
node load-data.js --target mongodb --data-dir ./data --drop
node compare-pipelines.js --verbose
```

### Test Coverage Report

Generate an HTML coverage report:

```bash
./gradlew :core:jacocoTestReport
```

Report location: `core/build/reports/jacoco/test/html/index.html`

### Performance Benchmarks

Run JMH benchmarks to measure translation performance:

```bash
./gradlew :benchmarks:jmh

# Quick benchmark (less warmup)
./gradlew :benchmarks:benchmarkQuick
```

Results: `benchmarks/build/reports/jmh/results.json`

### Troubleshooting

**Oracle container won't start:**
```bash
# Check logs
docker compose logs oracle

# Ensure sufficient memory (Oracle needs ~2GB)
docker stats

# Reset and try again
docker compose down -v
docker compose up -d
```

**MongoDB authentication errors:**
```bash
# Verify MongoDB is running
docker exec mongo-translator-mongodb mongosh --eval "db.adminCommand('ping')"

# Check credentials in docker-compose.yml
# Default: admin:admin123
```

**Tests failing with connection errors:**
```bash
# Ensure both containers are healthy
docker compose ps

# Should show both as "healthy"
# If not, wait and check logs
```

### Pre-commit Hooks (Recommended)

```bash
pip install pre-commit
pre-commit install
```

This runs Checkstyle and SpotBugs before each commit.

## Project Structure

```
mongo-oracle-translator/
├── core/                    # Main translation library
│   └── src/
│       ├── main/java/com/oracle/mongodb/translator/
│       │   ├── api/         # Public API
│       │   ├── ast/         # Abstract Syntax Tree
│       │   ├── parser/      # Pipeline parsing
│       │   ├── optimizer/   # Query optimization
│       │   ├── generator/   # SQL generation
│       │   └── exception/   # Error handling
│       └── test/java/
├── integration-tests/       # Oracle integration tests
├── generator/               # Code generation from specs
├── specs/                   # Operator specifications
│   ├── operators.json
│   ├── type-mappings.json
│   └── test-cases/
├── query-tests/             # Cross-database validation tests
│   ├── data/                # Test data loaders
│   ├── tests/               # Test case definitions
│   ├── scripts/             # Test runner scripts
│   ├── large-scale/         # Large-scale comparison tests (~4GB)
│   └── results/             # Test output
├── benchmarks/              # JMH performance benchmarks
├── docs/                    # Documentation
├── scripts/                 # Environment management
└── docker-compose.yml       # MongoDB + Oracle setup
```

## Architecture

The library follows a pipeline architecture:

```
MongoDB Pipeline (BSON)
        │
        ▼
┌───────────────┐
│    Parser     │  Converts BSON to AST
└───────┬───────┘
        │
        ▼
┌───────────────┐
│   Optimizer   │  Predicate pushdown, merging
└───────┬───────┘
        │
        ▼
┌───────────────┐
│   Generator   │  Renders AST to SQL
└───────┬───────┘
        │
        ▼
   Oracle SQL
```

**Key Design Decisions:**
- **jOOQ-inspired context rendering**: AST nodes render themselves via `SqlGenerationContext`
- **Sealed interfaces**: Type-safe expression and stage hierarchies
- **Specification-first**: Operator definitions in JSON drive code generation

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Follow TDD: Write failing tests first
4. Implement the feature
5. Ensure all tests pass (`./gradlew check`)
6. Commit your changes (`git commit -m 'Add amazing feature'`)
7. Push to the branch (`git push origin feature/amazing-feature`)
8. Open a Pull Request

## License

This project is licensed under the Universal Permissive License (UPL), Version 1.0. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Oracle Database JSON features documentation
- MongoDB Aggregation Framework specification
- jOOQ library for SQL generation patterns

## Support

- [Documentation](docs/)
- [Issue Tracker](https://github.com/rhoulihan/MongoPLSQL-Bridge/issues)
- [Implementation Status](docs/IMPLEMENTATION_STATUS.md)
