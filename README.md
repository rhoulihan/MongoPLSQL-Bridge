# MongoDB to Oracle SQL Translator

A Java library that translates MongoDB aggregation framework pipelines into equivalent Oracle SQL/JSON statements for execution on Oracle 23ai/26ai JSON Collections.

## Overview

This library provides a MongoDB-style `aggregate()` API while generating Oracle SQL under the hood, enabling applications to:

- Execute MongoDB aggregation pipelines against Oracle Database
- Leverage Oracle's JSON Collection features with familiar MongoDB syntax
- Gradually migrate from MongoDB to Oracle without rewriting application logic

## Current Status

**Phase 4 Complete** - All operators implemented with 93% instruction coverage and 152 passing cross-database validation tests.

| Metric | Value |
|--------|-------|
| Unit Tests | 1,408 |
| Cross-DB Validation | 152 |
| Large-Scale Tests | 10 |
| Instruction Coverage | 93% |
| Branch Coverage | 83% |

## Supported Operators

### Pipeline Stages

| Operator | Oracle Translation |
|----------|-------------------|
| `$match` | WHERE clause with dot notation (`base.data.field`) |
| `$group` | GROUP BY with aggregate functions |
| `$project` | SELECT with field selection/computation |
| `$sort` | ORDER BY (with Top-N optimization) |
| `$limit` | FETCH FIRST n ROWS ONLY |
| `$skip` | OFFSET n ROWS |
| `$lookup` | LEFT OUTER JOIN (equality) / LATERAL join (pipeline form) |
| `$unwind` | JSON_TABLE with NESTED PATH |
| `$addFields`/`$set` | Computed columns |
| `$unionWith` | UNION ALL |
| `$bucket` | CASE expression grouping |
| `$bucketAuto` | NTILE-based automatic bucketing |
| `$facet` | Multiple parallel subqueries (JSON_OBJECT) |
| `$graphLookup` | Recursive CTE for hierarchical queries |
| `$setWindowFields` | Window functions (RANK, DENSE_RANK, etc.) |
| `$redact` | Document-level filtering ($$PRUNE/$$KEEP/$$DESCEND) |
| `$sample` | Random sampling (DBMS_RANDOM.VALUE) |
| `$count` | Document count (JSON_OBJECT output) |
| `$replaceRoot` | Root document replacement |
| `$merge` | MERGE statement (stub) |
| `$out` | INSERT statement (stub) |

### Expression Operators

| Category | Operators |
|----------|-----------|
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists` |
| Logical | `$and`, `$or`, `$not`, `$nor` |
| Arithmetic | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod`, `$abs`, `$ceil`, `$floor`, `$round` |
| Conditional | `$cond`, `$ifNull`, `$switch` |
| String | `$concat`, `$toLower`, `$toUpper`, `$substr`, `$trim`, `$ltrim`, `$rtrim`, `$strLenCP`, `$split`, `$indexOfCP`, `$regexMatch`, `$regexFind`, `$replaceOne`, `$replaceAll` |
| Date | `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dayOfWeek`, `$dayOfYear` |
| Array | `$arrayElemAt`, `$size`, `$first`, `$last`, `$filter`, `$map`, `$reduce`, `$concatArrays`, `$slice`, `$setUnion`, `$setIntersection`, `$reverseArray`, `$in` |
| Type | `$type`, `$toInt`, `$toString`, `$toDouble`, `$toBool`, `$toDate` |
| Object | `$mergeObjects`, `$objectToArray`, `$arrayToObject` |

### Accumulator Operators

| Operator | Oracle Equivalent |
|----------|------------------|
| `$sum` | `SUM()` |
| `$avg` | `AVG()` |
| `$count` | `COUNT(*)` |
| `$min` | `MIN()` |
| `$max` | `MAX()` |
| `$first` | `FIRST_VALUE()` |
| `$last` | `LAST_VALUE()` |
| `$push` | `JSON_ARRAYAGG()` |
| `$addToSet` | `JSON_ARRAYAGG(DISTINCT)` |

### Pipeline Optimization

- Predicate pushdown (moves `$match` early)
- Sort-limit combination (Oracle Top-N optimization)
- Configurable optimization chain

## Quick Start

### Build Only

```bash
git clone https://github.com/rhoulihan/MongoPLSQL-Bridge.git
cd MongoPLSQL-Bridge
./gradlew build -x test
```

The built JAR will be at `core/build/libs/core.jar`.

### Command-Line Interface

The translator includes a CLI for quick pipeline translation:

```bash
# Build the CLI (creates standalone JAR)
./gradlew :core:fatJar

# Translate a pipeline file
./mongo2sql pipeline.json

# With options
./mongo2sql --collection orders --pretty --inline pipeline.json
```

**CLI Options:**

| Option | Short | Description |
|--------|-------|-------------|
| `--collection <name>` | `-c` | Collection/table name (overrides file setting) |
| `--inline` | `-i` | Inline bind variables into SQL |
| `--pretty` | `-p` | Pretty-print the SQL output |
| `--no-hints` | | Disable Oracle optimizer hints |
| `--strict` | | Fail on unsupported operators |
| `--data-column <name>` | | JSON data column name (default: data) |
| `--output <file>` | `-o` | Write output to file instead of stdout |
| `--version` | `-v` | Show version information |
| `--help` | `-h` | Show help message |

**Input File Formats:**

1. **Raw pipeline array:**
   ```json
   [{"$match": {"status": "active"}}, {"$limit": 10}]
   ```

2. **Single pipeline with metadata:**
   ```json
   {
     "name": "Active Orders",
     "collection": "orders",
     "pipeline": [{"$match": {"status": "active"}}]
   }
   ```

3. **Multiple pipelines:**
   ```json
   {
     "pipelines": [
       {"name": "Pipeline 1", "collection": "orders", "pipeline": [...]},
       {"name": "Pipeline 2", "collection": "products", "pipeline": [...]}
     ]
   }
   ```

### Java API Usage

```java
import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import org.bson.Document;

var config = OracleConfiguration.builder()
    .collectionName("orders")
    .build();

var translator = AggregationTranslator.create(config);

var pipeline = List.of(
    Document.parse("{\"$match\": {\"status\": \"active\"}}"),
    Document.parse("{\"$group\": {\"_id\": \"$category\", \"total\": {\"$sum\": \"$amount\"}}}"),
    Document.parse("{\"$sort\": {\"total\": -1}}"),
    Document.parse("{\"$limit\": 10}")
);

var result = translator.translate(pipeline);
System.out.println(result.sql());
// SELECT base.data.category AS "_id", SUM(CAST(base.data.amount AS NUMBER)) AS "total"
// FROM orders base
// WHERE base.data.status = :1
// GROUP BY base.data.category
// ORDER BY "total" DESC
// FETCH FIRST 10 ROWS ONLY
```

## Testing

### Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| JDK | 17+ | Required for building and running |
| Docker | 20.10+ | Required for integration tests |
| Docker Compose | 2.0+ | Required for test environment |
| Node.js | 16+ | Optional, for large-scale tests only |

### Unit Tests

```bash
./gradlew :core:test
```

Runs 1,408 unit tests covering all operators, parsers, and pipeline scenarios.

### Integration Tests

```bash
# Start test environment
docker compose up -d
./scripts/validate-env.sh

# Run cross-database validation (152 tests)
./query-tests/scripts/setup.sh
./query-tests/scripts/run-tests.sh
```

### Large-Scale Tests

Test with larger datasets (100MB - 4GB) and complex pipelines:

```bash
./run-comparison.sh --size small   # ~100MB, ~219K documents
./run-comparison.sh --size medium  # ~500MB, ~1M documents
./run-comparison.sh --size large   # ~2GB, ~4.5M documents
```

### Test Documentation

Open [docs/test-catalog.html](docs/test-catalog.html) in a browser for an interactive test catalog with:
- Searchable/filterable test list
- Category tabs for quick navigation
- MongoDB pipeline and generated SQL for each test
- Side-by-side comparison of MongoDB vs Oracle results
- Match type summary KPIs (Strict Match, Loose Match, Mismatch)
- Live test execution (requires Docker containers running)

> **Note:** The test catalog data is automatically regenerated after each test run via `docs/test-catalog-data.json`.

### Test Coverage Report

```bash
./gradlew :core:jacocoTestReport
```

Report location: `core/build/reports/jacoco/test/html/index.html`

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
```

## Project Structure

```
mongo-oracle-translator/
├── core/                    # Main translation library
│   └── src/
│       ├── main/java/       # API, AST, parser, optimizer, generator, CLI
│       └── test/java/       # Unit tests (1,408)
├── integration-tests/       # Oracle integration tests
├── query-tests/             # Cross-database validation tests
│   ├── tests/               # Test case definitions (152)
│   ├── large-scale/         # Large-scale comparison tests (10)
│   └── scripts/             # Test runner scripts
├── benchmarks/              # JMH performance benchmarks
├── docs/                    # Documentation
│   ├── test-catalog.html    # Interactive test catalog (open in browser)
│   ├── test-catalog-data.json # Test data for HTML catalog
│   └── IMPL-*.md            # Implementation notes
├── scripts/                 # Environment management
├── mongo2sql                # CLI wrapper script
└── docker-compose.yml       # MongoDB + Oracle setup
```

## Architecture

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
- **Type-preserving output**: Uses `JSON_QUERY` and `JSON_ARRAYAGG(JSON_OBJECT(*))` to preserve MongoDB types (numbers, booleans, arrays)
- **Sealed interfaces**: Type-safe expression and stage hierarchies
- **Specification-first**: Operator definitions in JSON drive code generation

## Requirements

- Java 17 or higher
- Oracle Database 23ai or 26ai with JSON Collections
- Oracle JDBC Driver 23.3+

## Installation

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

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Follow TDD: Write failing tests first
4. Ensure all tests pass (`./gradlew check`)
5. Commit your changes
6. Open a Pull Request

## License

This project is licensed under the Universal Permissive License (UPL), Version 1.0. See the [LICENSE](LICENSE) file for details.

## Support

- [Documentation](docs/)
- [Test Catalog](docs/test-catalog.html) (open in browser)
- [Issue Tracker](https://github.com/rhoulihan/MongoPLSQL-Bridge/issues)
