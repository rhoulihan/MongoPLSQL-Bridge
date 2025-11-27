# MongoDB to Oracle SQL Translator

A Java library that translates MongoDB aggregation framework pipelines into equivalent Oracle SQL/JSON statements for execution on Oracle 23ai/26ai JSON Collections.

## Overview

This library provides a MongoDB-style `aggregate()` API while generating Oracle SQL under the hood, enabling applications to:

- Execute MongoDB aggregation pipelines against Oracle Database
- Leverage Oracle's JSON Collection features with familiar MongoDB syntax
- Gradually migrate from MongoDB to Oracle without rewriting application logic

## Current Status

**Phase 4 In Progress** - Tier 2-3 operators and optimization implemented.

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
- String: `$concat`, `$toLower`, `$toUpper`, `$substr`, `$trim`, `$ltrim`, `$rtrim`, `$strLenCP`
- Date: `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dayOfWeek`, `$dayOfYear`
- Array: `$arrayElemAt`, `$size`, `$first`, `$last`

**Accumulator Operators:**
- `$sum`, `$avg`, `$count`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet`

**Pipeline Optimization:**
- Predicate pushdown (moves `$match` early)
- Sort-limit combination (Oracle Top-N optimization)
- Configurable optimization chain

### Validation Status

All 65 cross-database validation tests pass (MongoDB 8.0 ↔ Oracle 23.6). See [query-tests/](query-tests/) for details.

**Test Categories:** Comparison (8), Logical (5), Accumulator (8), Stage (7), Arithmetic (5), Conditional (3), String (6), Date (5), Array (4), $lookup/$unwind (4), $addFields (2), Complex (5), Edge cases (3)

### Next Phase (Tier 4)

- `$facet`, `$bucket`, `$bucketAuto` stages
- `$merge`, `$out`, `$unionWith` stages
- `$graphLookup`, `$setWindowFields` (stubs)

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
| `$facet` | ⏳ Planned | Multiple subqueries |
| `$bucket` | ⏳ Planned | CASE expressions |

### Expression Operators

| Category | Operators | Status |
|----------|-----------|--------|
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin` | ✅ Implemented |
| Logical | `$and`, `$or`, `$not`, `$nor` | ✅ Implemented |
| Arithmetic | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod` | ✅ Implemented |
| Conditional | `$cond`, `$ifNull` | ✅ Implemented |
| String | `$concat`, `$toLower`, `$toUpper`, `$substr`, `$trim`, `$ltrim`, `$rtrim`, `$strLenCP` | ✅ Implemented |
| Date | `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dayOfWeek`, `$dayOfYear` | ✅ Implemented |
| Array | `$arrayElemAt`, `$size`, `$first`, `$last` | ✅ Implemented |

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

## Development Setup

### Prerequisites

- JDK 17+
- Docker and Docker Compose
- Git

### Local Development

1. **Clone the repository:**
   ```bash
   git clone https://github.com/rhoulihan/MongoPLSQL-Bridge.git
   cd MongoPLSQL-Bridge
   ```

2. **Start test environment (MongoDB + Oracle):**
   ```bash
   ./scripts/start-env.sh
   ```

3. **Wait for databases to be ready:**
   ```bash
   ./scripts/validate-env.sh
   ```

4. **Build the project:**
   ```bash
   ./gradlew build
   ```

5. **Run tests:**
   ```bash
   ./gradlew test                                           # Unit tests
   ./gradlew :integration-tests:test -PrunIntegrationTests  # Integration tests (requires Docker)
   ./query-tests/scripts/setup.sh && ./query-tests/scripts/run-tests.sh  # Cross-database validation
   ```

### Pre-commit Hooks (Recommended)

```bash
pip install pre-commit
pre-commit install
```

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
│   └── results/             # Test output
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
