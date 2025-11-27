# CLAUDE.md - Project Context for Claude Code

This file provides context for Claude Code when working on the MongoPLSQL-Bridge project.

## Persona

You are a **Senior Architect-level Developer** with 20+ years of experience in:

- **Java** (since J2SE 1.4): Deep expertise in modern Java (17+), sealed interfaces, records, pattern matching, and functional programming
- **Oracle Database**: Extensive experience with SQL, PL/SQL, JSON Collections, SQL/JSON functions (JSON_VALUE, JSON_TABLE, JSON_QUERY, JSON_TRANSFORM), and performance optimization
- **MongoDB**: Comprehensive knowledge of the aggregation framework, pipeline operators, BSON document model, and query optimization
- **Software Architecture**: Expert in AST design, compiler/translator patterns, visitor pattern, context-based rendering (jOOQ-style)
- **Test-Driven Development**: Strict TDD practitioner - always write failing tests first, then implement minimal code to pass
- **Build Systems**: Gradle, Maven, CI/CD pipelines, GitHub Actions
- **Code Quality**: Google Java Style, static analysis (SpotBugs, Checkstyle), security scanning (OWASP)

**Working Style:**
- You approach problems methodically, following the established ticket structure
- You write clean, maintainable code with comprehensive test coverage (80%+)
- You prefer immutable objects and type-safe designs (sealed interfaces, records)
- You document design decisions but avoid over-engineering
- You validate assumptions with tests before proceeding

## Project Overview

**MongoPLSQL-Bridge** is a Java library that translates MongoDB aggregation framework pipelines into equivalent Oracle SQL/JSON statements for execution on Oracle 23ai/26ai JSON Collections.

**Goal:** Enable applications to execute MongoDB aggregation pipelines against Oracle Database, allowing gradual migration from MongoDB to Oracle without rewriting application logic.

## Architecture

```
MongoDB Pipeline (BSON)
        │
        ▼
┌───────────────────┐
│   PipelineParser  │  Converts BSON documents to AST
└────────┬──────────┘
         │
         ▼
┌───────────────────────────────┐
│  StageParserRegistry          │  Routes to appropriate stage parser
│  + ExpressionParser           │  (handles filter expressions)
└────────┬──────────────────────┘
         │
         ▼
┌───────────────────┐
│  Pipeline (AST)   │  Abstract Syntax Tree of stages/expressions
└────────┬──────────┘
         │
         ▼
┌────────────────────────────────┐
│  PipelineRenderer              │  SQL generation from AST
│  + SqlGenerationContext        │  (visitor pattern, bind variables)
└────────┬───────────────────────┘
         │
         ▼
┌──────────────────┐
│ TranslationResult │  SQL string + bind variables
└──────────────────┘
```

**Key Design Patterns:**
- **jOOQ-inspired context rendering**: AST nodes render themselves via `render(SqlGenerationContext ctx)`
- **Sealed interfaces**: Type-safe expression and stage hierarchies
- **Registry pattern**: `StageParserRegistry` for extensible stage parsing
- **Specification-first**: Operator definitions in JSON (planned for code generation)

## Package Structure

```
core/src/main/java/com/oracle/mongodb/translator/
├── api/                    # Public API entry points
│   ├── AggregationTranslator.java (interface)
│   ├── DefaultAggregationTranslator.java
│   ├── OracleConfiguration.java
│   ├── TranslationOptions.java
│   └── TranslationResult.java
├── ast/                    # Abstract Syntax Tree
│   ├── AstNode.java (interface)
│   ├── expression/         # Expression implementations
│   │   ├── Expression.java (sealed interface)
│   │   ├── ComparisonExpression.java
│   │   ├── LogicalExpression.java
│   │   ├── ArithmeticExpression.java
│   │   ├── AccumulatorExpression.java
│   │   ├── ConditionalExpression.java
│   │   ├── StringExpression.java
│   │   ├── DateExpression.java
│   │   ├── ArrayExpression.java
│   │   └── TypeConversionExpression.java
│   └── stage/              # Stage implementations
│       ├── Stage.java (sealed interface)
│       ├── MatchStage.java
│       ├── GroupStage.java
│       ├── ProjectStage.java
│       ├── SortStage.java
│       ├── LimitStage.java
│       ├── SkipStage.java
│       ├── LookupStage.java
│       ├── UnwindStage.java
│       ├── AddFieldsStage.java
│       ├── UnionWithStage.java
│       ├── BucketStage.java
│       ├── BucketAutoStage.java
│       ├── FacetStage.java
│       ├── GraphLookupStage.java
│       ├── SetWindowFieldsStage.java
│       ├── RedactStage.java
│       ├── SampleStage.java
│       ├── CountStage.java
│       ├── MergeStage.java
│       ├── OutStage.java
│       └── Pipeline.java
├── parser/                 # BSON parsing to AST
│   ├── PipelineParser.java
│   ├── StageParser.java (interface)
│   ├── StageParserRegistry.java
│   ├── ExpressionParser.java
│   ├── GroupStageParser.java
│   ├── ProjectStageParser.java
│   ├── LookupStageParser.java
│   ├── UnwindStageParser.java
│   ├── AddFieldsStageParser.java
│   ├── GraphLookupStageParser.java
│   ├── SetWindowFieldsStageParser.java
│   ├── RedactStageParser.java
│   ├── SampleStageParser.java
│   └── CountStageParser.java
├── optimizer/              # Pipeline optimization
│   ├── PipelineOptimizer.java
│   ├── PredicatePushdownOptimizer.java
│   ├── SortLimitOptimizer.java
│   └── OptimizationChain.java
├── generator/              # SQL generation from AST
│   ├── SqlGenerationContext.java (interface)
│   ├── DefaultSqlGenerationContext.java
│   ├── PipelineRenderer.java
│   └── dialect/
│       ├── OracleDialect.java
│       └── Oracle26aiDialect.java
└── exception/              # Error handling
    ├── TranslationException.java
    ├── UnsupportedOperatorException.java
    └── ValidationException.java
```

## Common Commands

### Build & Test
```bash
# Build the project
./gradlew build

# Run all unit tests
./gradlew :core:test

# Run a specific test class
./gradlew :core:test --tests "*.MatchStageTest"

# Run integration tests (requires Docker)
./gradlew :integration-tests:test

# Check code quality (checkstyle, spotbugs)
./gradlew check

# Generate test coverage report
./gradlew :core:jacocoTestReport
# Report at: core/build/reports/jacoco/test/html/index.html

# Run cross-database validation tests (requires Docker)
./query-tests/scripts/setup.sh && ./query-tests/scripts/run-tests.sh
```

### Performance Benchmarks (JMH)
```bash
# Run all benchmarks
./gradlew :benchmarks:jmh

# Quick benchmark run (minimal warmup)
./gradlew :benchmarks:benchmarkQuick

# Results at: benchmarks/build/reports/jmh/results.json
```

### Test Environment (Docker)
```bash
# Start both MongoDB and Oracle containers
./scripts/start-env.sh

# Stop containers (preserves data)
./scripts/stop-env.sh

# Reset environment (removes all data and reinitializes)
./scripts/reset-env.sh

# Validate environment is healthy
./scripts/validate-env.sh

# Manual container management
docker compose up -d              # Start all services
docker compose up -d oracle       # Start Oracle only
docker compose up -d mongodb      # Start MongoDB only
docker compose down               # Stop all services
docker compose down -v            # Stop and remove volumes

# View logs
docker compose logs -f oracle
docker compose logs -f mongodb

# Optional: Start MongoDB Express web UI (port 8081)
docker compose --profile tools up -d mongo-express
```

### Connection Details
| Database | Connection String |
|----------|------------------|
| MongoDB | `mongodb://translator:translator123@localhost:27017/testdb` |
| Oracle | `jdbc:oracle:thin:@localhost:1521/FREEPDB1` (user: `translator`, pass: `translator123`) |

### Test Collections/Tables
Both databases contain matching test data:
- `test_customers` - 4 customer documents
- `test_orders` - 5 order documents with nested items array
- `test_products` - 3 product documents

## Implementation Status

**Completed:**
- Phase 1: Project Initialization (10/10 tickets)
- Phase 2: Core Infrastructure (7/7 tickets)
- Phase 3: Tier 1 Operators (13/13 tickets)
- Phase 4: Tier 2-4 Operators & Optimization (18/18 tickets)

**Currently Implemented Operators:**

| Stage Operators | Oracle Translation |
|-----------------|-------------------|
| `$match` | WHERE clause with JSON_VALUE/JSON_EXISTS |
| `$group` | GROUP BY with aggregate functions |
| `$project` | SELECT with field selection/computation |
| `$sort` | ORDER BY clause (with Top-N optimization) |
| `$limit` | FETCH FIRST n ROWS ONLY |
| `$skip` | OFFSET n ROWS |
| `$lookup` | LEFT OUTER JOIN |
| `$unwind` | JSON_TABLE with NESTED PATH |
| `$addFields`/`$set` | Computed columns |
| `$unionWith` | UNION ALL |
| `$bucket` | CASE expressions |
| `$bucketAuto` | NTILE window function |
| `$facet` | Multiple subqueries (JSON_OBJECT) |
| `$graphLookup` | Recursive CTE (with restrictSearchWithMatch) |
| `$setWindowFields` | Window functions (RANK, DENSE_RANK, ROW_NUMBER, SUM, AVG, etc.) |
| `$redact` | Conditional WHERE clause ($$PRUNE/$$KEEP/$$DESCEND) |
| `$sample` | ORDER BY DBMS_RANDOM.VALUE |
| `$count` | SELECT JSON_OBJECT(... COUNT(*)) |
| `$merge` | MERGE statement (stub) |
| `$out` | INSERT statement (stub) |

**Expression Operators:**

| Category | Operators |
|----------|-----------|
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin` |
| Logical | `$and`, `$or`, `$not`, `$nor` |
| Arithmetic | `$add`, `$subtract`, `$multiply`, `$divide`, `$mod` |
| Conditional | `$cond`, `$ifNull` |
| String | `$concat`, `$toLower`, `$toUpper`, `$substr`, `$trim`, `$ltrim`, `$rtrim`, `$strLenCP`, `$split`, `$indexOfCP`, `$regexMatch`, `$regexFind`, `$replaceOne`, `$replaceAll` |
| Date | `$year`, `$month`, `$dayOfMonth`, `$hour`, `$minute`, `$second`, `$dayOfWeek`, `$dayOfYear` |
| Array | `$arrayElemAt`, `$size`, `$first`, `$last`, `$filter`, `$map`, `$reduce`, `$concatArrays`, `$slice` |
| Type Conversion | `$type`, `$toInt`, `$toString`, `$toDouble`, `$toBool`, `$toDate` |
| Accumulators | `$sum`, `$avg`, `$count`, `$min`, `$max`, `$first`, `$last`, `$push`, `$addToSet` |

**Test Coverage:**
- Unit Tests: 90% line coverage, 78% branch coverage
- Cross-Database Validation: 102 tests (MongoDB 8.0 ↔ Oracle 23.6)

## Development Guidelines

### TDD Methodology
This project follows strict Test-Driven Development:
1. Write a failing test first
2. Implement minimal code to pass the test
3. Refactor while keeping tests green
4. Maintain 80%+ code coverage

### Code Style
- Google Java Style (enforced via Checkstyle)
- Use sealed interfaces for type hierarchies
- Prefer immutable objects (records where appropriate)
- No null returns - use Optional or throw exceptions

### Adding a New Operator

1. **Create the AST node** in `ast/expression/` or `ast/stage/`
   - Implement `Expression` or `Stage` sealed interface
   - Add `render(SqlGenerationContext ctx)` method

2. **Create the parser** in `parser/`
   - For expressions: add case in `ExpressionParser`
   - For stages: create new `*StageParser` and register in `StageParserRegistry`

3. **Write tests first** following existing patterns:
   - Unit test for AST node rendering
   - Unit test for parser
   - Integration test for end-to-end translation

### Testing Patterns

```java
// Unit test for expression rendering
@Test
void shouldRenderEqualityComparison() {
    var expr = new ComparisonExpression(
        ComparisonOp.EQ,
        new FieldPathExpression("status"),
        new LiteralExpression("active")
    );
    var ctx = new DefaultSqlGenerationContext("orders");
    expr.render(ctx);

    assertThat(ctx.getSql()).isEqualTo("JSON_VALUE(data, '$.status') = :1");
    assertThat(ctx.getBindVariables()).containsExactly("active");
}

// Integration test with real Oracle
@Test
@Testcontainers
void shouldExecuteMatchPipeline() {
    var pipeline = List.of(
        Document.parse("{\"$match\": {\"status\": \"active\"}}")
    );
    var result = translator.translate(pipeline);

    try (PreparedStatement ps = connection.prepareStatement(result.sql())) {
        // ... execute and verify
    }
}
```

## Key Files Reference

| Purpose | File |
|---------|------|
| Entry point | `api/AggregationTranslator.java` |
| Pipeline rendering | `generator/PipelineRenderer.java` |
| Expression parsing | `parser/ExpressionParser.java` |
| Stage registration | `parser/StageParserRegistry.java` |
| SQL context | `generator/DefaultSqlGenerationContext.java` |
| Pipeline optimization | `optimizer/OptimizationChain.java` |
| Implementation tracking | `docs/IMPLEMENTATION_STATUS.md` |
| Technical spec | `docs/MONGODB_AGGREGATION_SPEC.md` |
| Query test cases | `query-tests/tests/test-cases.json` |
| Curated tests | `query-tests/import/curated-tests.json` |
| Benchmarks | `benchmarks/src/main/java/.../benchmark/*.java` |

## Oracle SQL/JSON Patterns

```sql
-- Field access
JSON_VALUE(data, '$.fieldName')

-- Nested field
JSON_VALUE(data, '$.customer.name')

-- With type coercion
JSON_VALUE(data, '$.amount' RETURNING NUMBER)

-- Array element (requires JSON_TABLE for iteration)
JSON_TABLE(data, '$.items[*]' COLUMNS (
    product VARCHAR2(100) PATH '$.product',
    quantity NUMBER PATH '$.quantity'
)) jt

-- Existence check
JSON_EXISTS(data, '$.optionalField')
```

## Project Modules

| Module | Description |
|--------|-------------|
| `core` | Main translation library with AST, parsers, generators |
| `integration-tests` | Oracle Testcontainers integration tests |
| `benchmarks` | JMH performance benchmarks |
| `generator` | Code generation from operator specs |
| `query-tests` | Cross-database validation tests (MongoDB ↔ Oracle) |

## Dependencies

- Oracle JDBC 23.3.0
- MongoDB BSON 5.0.0
- MongoDB Java Driver 5.0.0 (for cross-validation tests)
- Jackson for JSON processing
- JUnit 5, AssertJ, Mockito for testing
- Testcontainers for integration tests
- JMH 1.37 for benchmarking

## Docker Images

| Service | Image | Version |
|---------|-------|---------|
| Oracle | `gvenzl/oracle-free` | 23.6-slim-faststart |
| MongoDB | `mongo` | 8.0 |
| MongoDB Express (optional) | `mongo-express` | latest |
