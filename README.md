# MongoDB to Oracle SQL Translator

A Java library that translates MongoDB aggregation framework pipelines into equivalent Oracle SQL/JSON statements for execution on Oracle 23ai/26ai JSON Collections.

## Overview

This library provides a MongoDB-style `aggregate()` API while generating Oracle SQL under the hood, enabling applications to:

- Execute MongoDB aggregation pipelines against Oracle Database
- Leverage Oracle's JSON Collection features with familiar MongoDB syntax
- Gradually migrate from MongoDB to Oracle without rewriting application logic

## Current Status

**Phase 2 Complete** - Core infrastructure is in place. Currently implementing Tier 1 operators.

### Implemented
- `$limit` - Translates to `FETCH FIRST n ROWS ONLY`
- `$skip` - Translates to `OFFSET n ROWS`
- Core AST infrastructure
- Pipeline parser framework
- Public API (AggregationTranslator, TranslationResult)

### Planned Features

- **Tier 1 Operators** (Core): `$match`, `$group`, `$project`, `$sort`
- **Tier 2 Operators** (Common): `$lookup`, `$unwind`, `$addFields`, `$count`
- **Expressions**: Comparison, logical, arithmetic, conditional, date, string, array
- **Accumulators**: `$sum`, `$avg`, `$count`, `$min`, `$max`, `$first`, `$last`, `$push`
- **Pipeline Optimization**: Predicate pushdown, predicate merging, sort-limit optimization

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

## Supported Operators (Current)

### Stage Operators

| Operator | Support Level | Notes |
|----------|--------------|-------|
| `$limit` | **Implemented** | FETCH FIRST n ROWS ONLY |
| `$skip` | **Implemented** | OFFSET n ROWS |
| `$match` | Planned | WHERE clause with JSON_VALUE/JSON_EXISTS |
| `$group` | Planned | GROUP BY with aggregate functions |
| `$project` | Planned | SELECT with field selection/computation |
| `$sort` | Planned | ORDER BY clause |
| `$lookup` | Planned | LEFT OUTER JOIN |
| `$unwind` | Planned | JSON_TABLE with NESTED PATH |
| `$addFields` | Planned | Computed columns |
| `$count` | Planned | SELECT COUNT(*) |

### Expression Operators (Planned)

| Category | Operators |
|----------|-----------|
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin` |
| Logical | `$and`, `$or`, `$not`, `$nor` |
| Arithmetic | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod` |
| Conditional | `$cond`, `$ifNull`, `$switch` |

### Accumulator Operators (Planned)

| Operator | Oracle Equivalent |
|----------|------------------|
| `$sum` | `SUM()` |
| `$avg` | `AVG()` |
| `$min` | `MIN()` |
| `$max` | `MAX()` |
| `$count` | `COUNT(*)` |

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

2. **Start Oracle database:**
   ```bash
   docker-compose up -d
   ```

3. **Wait for Oracle to be ready:**
   ```bash
   docker-compose logs -f oracle
   # Wait for "DATABASE IS READY TO USE!"
   ```

4. **Build the project:**
   ```bash
   ./gradlew build
   ```

5. **Run tests:**
   ```bash
   ./gradlew test                                        # Unit tests
   ./gradlew :integration-tests:test -PrunIntegrationTests  # Integration tests (requires Docker)
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
├── docs/                    # Documentation
└── docker-compose.yml       # Local Oracle setup
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
