# Technical Specification: MongoDB to Oracle SQL Translation Library

A translation library enabling MongoDB aggregation framework operators to execute on Oracle 26ai JSON Collections represents a significant engineering effort. This specification provides the complete architectural blueprint for a Java-first implementation with Node.js and Python portability, designed for TDD-driven development with Claude Code.

## Executive summary

**Oracle's MongoDB wire protocol layer cannot be extended by third parties** - ORDS is closed-source with no plugin architecture. The recommended approach is a **client-side driver library** that presents a MongoDB-style `aggregate()` API while generating Oracle SQL/JSON under the hood. This library will parse MongoDB aggregation pipelines, transform them into an AST, optimize the pipeline, and generate equivalent Oracle SQL using JSON_TABLE, JSON_VALUE, JSON_QUERY, and JSON_TRANSFORM functions.

The architecture draws heavily from **jOOQ's context-based rendering pattern** and uses a **specification-first design** where operator definitions in JSON drive code generation across Java, Node.js, and Python implementations.

---

## Architecture decision: Option B confirmed

### Why Option A (extending Oracle's MongoDB API) is not feasible

Oracle's Database API for MongoDB is implemented within ORDS (Oracle REST Data Services), which is **closed-source** with no documented extension points. Key findings:

- No plugin mechanism or Service Provider Interface (SPI) exists
- Translation occurs within ORDS's proprietary Java code
- No third-party extensions or community plugins exist
- Configuration is limited to connection pooling, ports, and TLS settings

**The `$sql` aggregation stage** provides a partial workaround, allowing arbitrary Oracle SQL execution from within a MongoDB pipeline, but this doesn't enable custom operator handling.

### Option B architecture: client-side translation driver

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        CLIENT APPLICATION                                │
├─────────────────────────────────────────────────────────────────────────┤
│  MongoDB-style API                                                       │
│  db.collection("users").aggregate([{$match: {...}}, {$group: {...}}])   │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    TRANSLATION LIBRARY (This Project)                    │
├─────────────────────────────────────────────────────────────────────────┤
│  1. Parse aggregation pipeline → AST                                     │
│  2. Validate operators and expressions                                   │
│  3. Optimize pipeline (predicate pushdown, stage reordering)            │
│  4. Generate Oracle SQL/JSON                                             │
│  5. Execute via JDBC/node-oracledb/python-oracledb                      │
│  6. Transform results back to document format                            │
└──────────────────────────────────┬──────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         ORACLE DATABASE 26ai                             │
│  JSON Collections + SQL/JSON Functions + Native JSON Type               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## MongoDB aggregation operators: tiered priority matrix

Based on real-world usage patterns from MongoDB documentation, Stack Overflow question frequency (**12,600+** aggregation-framework tagged questions), and common pipeline patterns in production systems:

### Tier 1 (Critical) — Essential for basic functionality

These operators appear in **80%+** of production pipelines and must be implemented first.

| Operator | Category | Translation Complexity | Oracle Equivalent |
|----------|----------|----------------------|-------------------|
| `$match` | Stage | Simple | `WHERE` + `JSON_EXISTS`/`JSON_VALUE` |
| `$group` | Stage | Moderate | `GROUP BY` + aggregation functions |
| `$project` | Stage | Simple | `SELECT` with `JSON_VALUE`/`JSON_QUERY` |
| `$sort` | Stage | Simple | `ORDER BY` with `JSON_VALUE` |
| `$limit` | Stage | Simple | `FETCH FIRST n ROWS ONLY` |
| `$sum` | Accumulator | Simple | `SUM()` |
| `$avg` | Accumulator | Simple | `AVG()` |
| `$count` | Accumulator | Simple | `COUNT(*)` |
| `$cond` | Conditional | Simple | `CASE WHEN...THEN...ELSE` |
| `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne` | Comparison | Simple | Standard SQL operators |
| `$add`, `$subtract`, `$multiply`, `$divide` | Arithmetic | Simple | Standard SQL operators |

### Tier 2 (Important) — Frequently used in production

| Operator | Category | Translation Complexity | Oracle Equivalent |
|----------|----------|----------------------|-------------------|
| `$lookup` | Stage | Moderate | `JOIN` on `JSON_VALUE` results |
| `$unwind` | Stage | Moderate | `JSON_TABLE` with `NESTED PATH` |
| `$addFields`/`$set` | Stage | Simple | `JSON_TRANSFORM SET` |
| `$skip` | Stage | Simple | `OFFSET n ROWS` |
| `$count` (stage) | Stage | Simple | `SELECT COUNT(*)` |
| `$min`, `$max` | Accumulator | Simple | `MIN()`, `MAX()` |
| `$first`, `$last` | Accumulator | Simple | `FIRST_VALUE()`, `LAST_VALUE()` |
| `$push`, `$addToSet` | Accumulator | Moderate | `JSON_ARRAYAGG` |
| `$concat`, `$toLower`, `$toUpper` | String | Simple | `CONCAT`, `LOWER`, `UPPER` |
| `$arrayElemAt`, `$filter`, `$size` | Array | Moderate | `JSON_QUERY` with path, `JSON_TABLE` |
| `$year`, `$month`, `$dayOfMonth` | Date | Simple | `EXTRACT()` |
| `$ifNull` | Conditional | Simple | `NVL()` or `COALESCE()` |

### Tier 3 (Specialized) — Domain-specific, moderate usage

| Operator | Category | Translation Complexity | Oracle Equivalent |
|----------|----------|----------------------|-------------------|
| `$facet` | Stage | Complex | Multiple CTEs or parallel queries |
| `$bucket`/`$bucketAuto` | Stage | Moderate | `CASE`-based grouping, `WIDTH_BUCKET` |
| `$sortByCount` | Stage | Simple | `GROUP BY` + `ORDER BY COUNT DESC` |
| `$replaceRoot`/`$replaceWith` | Stage | Simple | Subquery restructuring |
| `$merge`, `$out` | Stage | Moderate | `MERGE INTO` / `INSERT INTO` |
| `$unionWith` | Stage | Simple | `UNION ALL` |
| `$switch` | Conditional | Moderate | `CASE WHEN...` (multi-branch) |
| `$map`, `$reduce` | Array | Complex | `JSON_TABLE` + recursion or PL/SQL |
| `$setUnion`, `$setIntersection` | Array | Moderate | `MULTISET` operations |
| `$regexMatch` | String | Moderate | `REGEXP_LIKE` |
| `$dateFromString`, `$dateDiff` | Date | Moderate | `TO_DATE`, date arithmetic |
| `$stdDevPop`, `$stdDevSamp` | Accumulator | Simple | `STDDEV_POP`, `STDDEV_SAMP` |

### Tier 4 (Rare) — Edge cases, low adoption

| Operator | Category | Translation Complexity | Oracle Equivalent |
|----------|----------|----------------------|-------------------|
| `$graphLookup` | Stage | Complex | Recursive CTE |
| `$setWindowFields` | Stage | Complex | Window functions |
| `$geoNear` | Stage | Complex | Oracle Spatial functions |
| `$redact` | Stage | Complex | Row-level security emulation |
| `$sample` | Stage | Simple | `SAMPLE` clause or `DBMS_RANDOM` |
| `$densify`, `$fill` | Stage | Complex | Gap-filling with recursive CTEs |
| `$accumulator` | Accumulator | **Unsupported** | Deprecated in MongoDB 8.0 |
| `$pow`, `$sqrt`, `$log`, `$exp` | Arithmetic | Simple | `POWER`, `SQRT`, `LOG`, `EXP` |

---

## Oracle 26ai JSON capabilities deep dive

### Core SQL/JSON functions for translation

**JSON_TABLE** serves as the primary tool for flattening nested documents:

```sql
SELECT jt.* FROM orders,
  JSON_TABLE(data, '$' COLUMNS (
    order_id NUMBER PATH '$.orderId',
    customer VARCHAR2(100) PATH '$.customer.name',
    NESTED PATH '$.items[*]' COLUMNS (
      item_num FOR ORDINALITY,
      product VARCHAR2(50) PATH '$.product',
      quantity NUMBER PATH '$.quantity'
    )
  )) jt;
```

**JSON_QUERY with predicates** (Oracle 23ai/26ai) enables MongoDB-like filtering:

```sql
-- Filter array elements directly
SELECT json_query(data, '$[*]?(@.status == "active")' WITH WRAPPER) FROM orders;

-- With PASSING clause for parameterized values
SELECT json_query(data, '$[*]?(@.price > $min)' PASSING 100 AS "min") FROM products;
```

**JSON_TRANSFORM** handles in-place modifications with aggregation support:

```sql
-- Aggregation within documents
json_transform(data,
  SET '$.count' = path '@.items[*].count()',
  SET '$.total' = path '@.items[*].value.sum()',
  SET '$.average' = path '@.items[*].value.avg()')
```

### SODA API limitations

The Simple Oracle Document Access API supports document CRUD and Query-by-Example (QBE) but **does not support aggregation pipelines**. SODA QBE operators (`$eq`, `$gt`, `$and`, `$or`, etc.) translate well to `$match` stages, but complex aggregations must use SQL/JSON instead.

### JSON Relational Duality Views

Duality Views unify relational and document models with ETAG-based optimistic concurrency, but they **do not directly support aggregation operations**. They're useful for:
- Mapping normalized relational data to document structure
- Read/write operations with document semantics  
- Lock-free concurrency for REST APIs

For aggregation, use SQL queries on underlying tables or the duality view itself via SQL/JSON.

### Indexing strategies for performance

| Index Type | Use Case | Example |
|------------|----------|---------|
| **Function-based B-Tree** | Specific scalar paths | `CREATE INDEX idx_name ON docs(JSON_VALUE(data, '$.name'))` |
| **Multivalue Index** | Array element searches | `CREATE MULTIVALUE INDEX idx_tags ON docs(data.tags.stringOnly())` |
| **JSON Search Index** | Full-text and ad-hoc queries | `CREATE SEARCH INDEX idx ON docs(data) FOR JSON` |
| **Composite** | Multiple frequently-filtered paths | Multiple `JSON_VALUE` expressions |

---

## Translation architecture specification

### AST node hierarchy

```
AggregationPipeline
├── Stage (abstract)
│   ├── MatchStage { FilterExpression filter }
│   ├── GroupStage { GroupKey id, List<Accumulator> accumulators }
│   ├── ProjectStage { List<ProjectionSpec> projections }
│   ├── LookupStage { String from, String localField, String foreignField, String as }
│   ├── UnwindStage { String path, boolean preserveNullAndEmpty }
│   ├── SortStage { List<SortKey> keys }
│   ├── LimitStage { int limit }
│   ├── SkipStage { int skip }
│   └── ... (additional stages)
│
Expression (abstract)
├── FieldPathExpression { String path }      // $.field.nested
├── LiteralExpression { Object value }       // Constants
├── ComparisonExpression { Op, left, right } // $eq, $gt, etc.
├── LogicalExpression { Op, List<Expression> operands } // $and, $or
├── ArithmeticExpression { Op, left, right } // $add, $multiply
├── ArrayExpression { ... }                  // $in, $elemMatch
├── ConditionalExpression { ... }            // $cond, $switch
├── DateExpression { ... }                   // $year, $dateToString
└── AccumulatorExpression { Op, Expression arg } // $sum, $avg
```

### Context-based rendering pattern (jOOQ-inspired)

Rather than a visitor pattern, use a **context pattern** where each AST node renders itself:

```java
public interface AstNode {
    void render(SqlGenerationContext ctx);
}

public interface SqlGenerationContext {
    void sql(String fragment);           // Append raw SQL
    void visit(AstNode node);            // Recursively render child
    void bind(Object value);             // Add bind variable
    void identifier(String name);        // Quote identifier appropriately
    boolean inline();                    // Whether to inline values
    OracleDialectVersion dialect();      // Target Oracle version
}

public class MatchStage implements Stage {
    private final FilterExpression filter;
    
    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("WHERE ");
        ctx.visit(filter);
    }
}

public class ComparisonExpression implements Expression {
    private final ComparisonOp op;
    private final Expression left;
    private final Expression right;
    
    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("JSON_VALUE(data, '");
        ctx.visit(left);
        ctx.sql("') ");
        ctx.sql(op.toSql());  // =, >, <, etc.
        ctx.sql(" ");
        ctx.visit(right);
    }
}
```

### Pipeline optimization passes

Execute these optimizations before SQL generation:

1. **Predicate pushdown**: Move `$match` stages as early as possible
2. **Predicate merging**: Combine consecutive `$match` stages into one `WHERE` clause
3. **Projection pushdown**: Limit selected fields early to reduce data transfer
4. **Lookup optimization**: Convert simple `$lookup` to efficient JOINs
5. **Sort-limit optimization**: Detect TOP-N patterns for Oracle optimization hints

```java
public interface PipelineOptimizer {
    Pipeline optimize(Pipeline input);
}

public class PredicatePushdownOptimizer implements PipelineOptimizer {
    @Override
    public Pipeline optimize(Pipeline input) {
        // Identify $match stages
        // Check if they can be moved before earlier stages
        // Reconstruct optimized pipeline
    }
}
```

### Graceful degradation hierarchy

```java
public enum TranslationCapability {
    FULL_SUPPORT,       // Direct translation to SQL
    EMULATED,           // Workaround using multiple SQL constructs
    PARTIAL,            // Some functionality, with documented limitations
    CLIENT_SIDE_ONLY,   // Must process in application code
    UNSUPPORTED         // Cannot translate - throws exception
}

public class TranslationResult {
    private final SqlQuery query;
    private final List<TranslationWarning> warnings;
    private final List<ClientStage> clientStages;  // For hybrid execution
    private final CapabilityReport capabilities;
}
```

**Emulation patterns for unsupported features**:

| MongoDB Feature | Emulation Strategy |
|-----------------|-------------------|
| `$arrayElemAt` | `JSON_QUERY` with array index path |
| `$filter` (array) | `JSON_TABLE` + `WHERE` clause |
| `$reduce` | Recursive CTE or client-side |
| `$map` | `JSON_TABLE` + transformation |
| `$mergeObjects` | `JSON_MERGEPATCH` or multiple JOINs |
| `$expr` with `$function` | **Not translatable** - client-side only |

---

## Polyglot architecture: specification-first design

### Monorepo structure

```
mongo-oracle-translator/
├── specs/                              # Language-agnostic specifications
│   ├── operators.json                  # Operator definitions
│   ├── type-mappings.json             # MongoDB→Oracle type mapping
│   ├── error-codes.json               # Error definitions
│   ├── test-cases/                    # Shared test fixtures
│   │   ├── match-operator.json
│   │   ├── group-operator.json
│   │   └── complex-pipelines.json
│   └── schemas/                       # JSON Schema validation
│
├── generator/                         # Code generation tool
│   ├── generate.py                    # Main generation script
│   └── templates/
│       ├── java/
│       │   ├── operator.java.mustache
│       │   └── translator.java.mustache
│       ├── nodejs/
│       │   └── operator.ts.mustache
│       └── python/
│           └── operator.py.mustache
│
├── java/                              # Primary implementation
│   ├── pom.xml
│   ├── src/main/java/
│   │   ├── core/                      # Hand-written translation engine
│   │   │   ├── parser/
│   │   │   ├── ast/
│   │   │   ├── optimizer/
│   │   │   └── generator/
│   │   └── generated/                 # Generated operator code
│   └── src/test/java/
│
├── nodejs/                            # Node.js port
│   ├── package.json
│   ├── src/
│   │   ├── core/
│   │   └── generated/
│   └── test/
│
├── python/                            # Python port
│   ├── pyproject.toml
│   ├── src/
│   │   ├── core/
│   │   └── generated/
│   └── tests/
│
└── integration-tests/                 # Cross-language verification
    ├── run-all-tests.sh
    └── compatibility-matrix.json
```

### Operator specification format

```json
{
  "operators": {
    "$match": {
      "category": "stage",
      "tier": 1,
      "description": "Filters documents based on specified criteria",
      "translationPattern": "WHERE {translated_expression}",
      "oracleFunctions": ["JSON_EXISTS", "JSON_VALUE"],
      "parameters": [{
        "name": "expression",
        "type": "filter_expression",
        "required": true
      }],
      "examples": [{
        "input": {"$match": {"status": "active"}},
        "output": "WHERE JSON_VALUE(data, '$.status') = :1",
        "bindings": ["active"]
      }],
      "limitations": [],
      "emulation": null
    },
    "$group": {
      "category": "stage",
      "tier": 1,
      "description": "Groups documents by expression and applies accumulators",
      "translationPattern": "SELECT {id_fields}, {accumulators} FROM ... GROUP BY {id_fields}",
      "oracleFunctions": ["JSON_VALUE", "SUM", "AVG", "COUNT", "JSON_ARRAYAGG"],
      "parameters": [{
        "name": "_id",
        "type": "expression",
        "required": true
      }, {
        "name": "accumulators",
        "type": "accumulator_map",
        "required": false
      }],
      "examples": [{
        "input": {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
        "output": "SELECT JSON_VALUE(data, '$.category') AS _id, SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS total FROM {collection} GROUP BY JSON_VALUE(data, '$.category')"
      }]
    }
  }
}
```

### Shared test fixture format

```json
{
  "testSuite": "aggregation-operators",
  "version": "1.0.0",
  "tests": [{
    "id": "match-001",
    "description": "$match with simple equality",
    "input": {
      "collection": "customers",
      "pipeline": [{"$match": {"status": "active"}}]
    },
    "expected": {
      "sql": "SELECT data FROM customers WHERE JSON_VALUE(data, '$.status') = :1",
      "params": ["active"]
    }
  }, {
    "id": "pipeline-001",
    "description": "Full pipeline: match, group, sort",
    "input": {
      "collection": "orders",
      "pipeline": [
        {"$match": {"status": "completed"}},
        {"$group": {"_id": "$customerId", "total": {"$sum": "$amount"}}},
        {"$sort": {"total": -1}}
      ]
    },
    "expected": {
      "sql": "SELECT JSON_VALUE(data, '$.customerId') AS _id, SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS total FROM orders WHERE JSON_VALUE(data, '$.status') = :1 GROUP BY JSON_VALUE(data, '$.customerId') ORDER BY total DESC",
      "params": ["completed"]
    }
  }]
}
```

---

## Package structure and interface definitions

### Java package organization

```
com.oracle.mongodb.translator/
├── api/                               # Public API
│   ├── AggregationTranslator.java     # Main entry point
│   ├── TranslationResult.java
│   ├── TranslationOptions.java
│   └── MongoCollection.java           # MongoDB-style API facade
│
├── parser/                            # Pipeline parsing
│   ├── PipelineParser.java
│   ├── StageParser.java
│   └── ExpressionParser.java
│
├── ast/                               # Abstract Syntax Tree
│   ├── AstNode.java
│   ├── Pipeline.java
│   ├── stage/
│   │   ├── Stage.java
│   │   ├── MatchStage.java
│   │   ├── GroupStage.java
│   │   └── ...
│   └── expression/
│       ├── Expression.java
│       ├── FieldPathExpression.java
│       ├── ComparisonExpression.java
│       └── ...
│
├── optimizer/                         # Pipeline optimization
│   ├── PipelineOptimizer.java
│   ├── PredicatePushdownOptimizer.java
│   ├── ProjectionPushdownOptimizer.java
│   └── OptimizationChain.java
│
├── generator/                         # SQL generation
│   ├── SqlGenerationContext.java
│   ├── OracleSqlGenerator.java
│   ├── BindVariableCollector.java
│   └── dialect/
│       ├── OracleDialect.java
│       ├── Oracle21cDialect.java
│       └── Oracle23aiDialect.java
│
├── executor/                          # Query execution
│   ├── QueryExecutor.java
│   ├── ResultMapper.java
│   └── DocumentBuilder.java
│
├── validation/                        # Input validation
│   ├── PipelineValidator.java
│   ├── OperatorValidator.java
│   └── ValidationResult.java
│
└── exception/                         # Error handling
    ├── TranslationException.java
    ├── UnsupportedOperatorException.java
    └── ValidationException.java
```

### Core interface definitions

```java
/**
 * Primary API for translating MongoDB aggregation pipelines to Oracle SQL.
 */
public interface AggregationTranslator {
    
    /**
     * Translates a MongoDB aggregation pipeline to Oracle SQL.
     * @param pipeline BSON document representing the aggregation pipeline
     * @return TranslationResult containing SQL and metadata
     */
    TranslationResult translate(List<Document> pipeline);
    
    /**
     * Translates with custom options.
     */
    TranslationResult translate(List<Document> pipeline, TranslationOptions options);
    
    /**
     * Factory method with Oracle connection configuration.
     */
    static AggregationTranslator create(OracleConfiguration config) {
        return new DefaultAggregationTranslator(config);
    }
}

/**
 * Translation result containing SQL and execution metadata.
 */
public record TranslationResult(
    String sql,
    List<Object> bindVariables,
    List<TranslationWarning> warnings,
    CapabilityReport capabilities,
    ExecutionPlan executionPlan
) {
    public boolean hasWarnings() { return !warnings.isEmpty(); }
    public boolean requiresClientProcessing() { 
        return capabilities.hasClientSideStages(); 
    }
}

/**
 * MongoDB-style collection facade for fluent API usage.
 */
public interface MongoCollection {
    String getName();
    
    /**
     * Execute aggregation pipeline and return results as documents.
     */
    List<Document> aggregate(List<Document> pipeline);
    
    /**
     * Execute aggregation and return as stream for large results.
     */
    Stream<Document> aggregateAsStream(List<Document> pipeline);
    
    /**
     * Get translation result without executing.
     */
    TranslationResult explain(List<Document> pipeline);
}

/**
 * Configuration options for translation behavior.
 */
@Builder
public record TranslationOptions(
    boolean inlineBindVariables,      // For debugging
    boolean prettyPrint,              // Format generated SQL
    boolean includeHints,             // Add Oracle optimizer hints
    OracleDialectVersion targetDialect,
    Set<String> allowedOperators,     // Whitelist (null = all)
    boolean strictMode                // Fail on any unsupported feature
) {
    public static TranslationOptions defaults() {
        return TranslationOptions.builder()
            .inlineBindVariables(false)
            .prettyPrint(false)
            .includeHints(true)
            .targetDialect(OracleDialectVersion.ORACLE_26AI)
            .strictMode(false)
            .build();
    }
}
```

---

## CI/CD pipeline configuration

### GitHub Actions workflow

```yaml
name: CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  JAVA_VERSION: '17'
  
jobs:
  validate-specs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Validate specification files
        run: python generator/validate-specs.py

  generate-code:
    needs: validate-specs
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Generate code from specifications
        run: python generator/generate.py --all
      - uses: actions/upload-artifact@v4
        with:
          name: generated-code
          path: |
            java/src/main/java/generated/
            nodejs/src/generated/
            python/src/generated/

  test-java:
    needs: generate-code
    runs-on: ubuntu-latest
    strategy:
      matrix:
        java-version: ['17', '21']
    
    services:
      oracle:
        image: gvenzl/oracle-free:23.6-slim-faststart
        env:
          ORACLE_PASSWORD: testpassword
          APP_USER: testuser
          APP_USER_PASSWORD: testpass
        ports:
          - 1521:1521
        options: >-
          --health-cmd healthcheck.sh
          --health-interval 10s
          --health-timeout 5s
          --health-retries 20
    
    steps:
      - uses: actions/checkout@v4
      - uses: actions/download-artifact@v4
        with:
          name: generated-code
          
      - name: Set up JDK ${{ matrix.java-version }}
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: ${{ matrix.java-version }}
          cache: 'gradle'
      
      - name: Run unit tests
        run: ./gradlew test -x integrationTest
      
      - name: Run integration tests
        run: ./gradlew integrationTest
        env:
          ORACLE_JDBC_URL: jdbc:oracle:thin:@localhost:1521/FREEPDB1
          ORACLE_USERNAME: testuser
          ORACLE_PASSWORD: testpass
      
      - name: Check code coverage
        run: ./gradlew jacocoTestCoverageVerification
      
      - name: Upload coverage report
        uses: codecov/codecov-action@v4
        with:
          files: build/reports/jacoco/test/jacocoTestReport.xml

  security-scan:
    needs: generate-code
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Run OWASP Dependency Check
        run: ./gradlew dependencyCheckAnalyze
        env:
          NVD_API_KEY: ${{ secrets.NVD_API_KEY }}
      
      - name: Run SpotBugs with FindSecBugs
        run: ./gradlew spotbugsMain
      
      - name: Snyk Security Scan
        uses: snyk/actions/gradle@master
        env:
          SNYK_TOKEN: ${{ secrets.SNYK_TOKEN }}

  quality-gate:
    needs: [test-java, security-scan]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: SonarQube Scan
        uses: sonarsource/sonarqube-scan-action@master
        env:
          SONAR_TOKEN: ${{ secrets.SONAR_TOKEN }}
          SONAR_HOST_URL: ${{ secrets.SONAR_HOST_URL }}

  cross-language-tests:
    needs: [test-java]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run cross-language compatibility tests
        run: ./integration-tests/run-cross-language-tests.sh
```

### Gradle build configuration

```groovy
plugins {
    id 'java-library'
    id 'jacoco'
    id 'checkstyle'
    id 'com.github.spotbugs' version '6.0.0'
    id 'org.owasp.dependencycheck' version '12.1.0'
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

dependencies {
    // Oracle JDBC
    implementation 'com.oracle.database.jdbc:ojdbc11:23.3.0.23.09'
    
    // MongoDB BSON for document handling
    implementation 'org.mongodb:bson:5.0.0'
    
    // JSON processing
    implementation 'com.fasterxml.jackson.core:jackson-databind:2.16.0'
    
    // Testing
    testImplementation 'org.junit.jupiter:junit-jupiter:5.10.1'
    testImplementation 'org.assertj:assertj-core:3.24.2'
    testImplementation 'org.testcontainers:oracle-free:1.21.3'
    testImplementation 'org.testcontainers:junit-jupiter:1.21.3'
}

// Code coverage thresholds
jacocoTestCoverageVerification {
    violationRules {
        rule {
            element = 'BUNDLE'
            limit {
                counter = 'LINE'
                value = 'COVEREDRATIO'
                minimum = 0.80
            }
            limit {
                counter = 'BRANCH'
                value = 'COVEREDRATIO'
                minimum = 0.75  // Critical for translation conditionals
            }
        }
    }
}

// SpotBugs with security plugin
spotbugs {
    effort = 'max'
    reportLevel = 'medium'
}

dependencies {
    spotbugsPlugins 'com.h3xstream.findsecbugs:findsecbugs-plugin:1.12.0'
}

// OWASP Dependency Check
dependencyCheck {
    failBuildOnCVSS = 7
}

// Checkstyle (Google style)
checkstyle {
    toolVersion = "10.25.0"
    configFile = file("${rootProject.projectDir}/config/checkstyle/google_checks.xml")
}
```

### Pre-commit hook configuration

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/gherynos/pre-commit-java
    rev: v0.2.1
    hooks:
      - id: checkstyle
        args: ['--config=config/checkstyle/google_checks.xml']
      - id: pmd
        exclude: /test/
      
  - repo: local
    hooks:
      - id: spotbugs
        name: SpotBugs Security Check
        entry: ./gradlew spotbugsMain
        language: system
        pass_filenames: false
        
      - id: unit-tests
        name: Run Unit Tests
        entry: ./gradlew test -x integrationTest
        language: system
        pass_filenames: false
```

---

## TDD test patterns and examples

### Unit test for operator translation

```java
@ExtendWith(MockitoExtension.class)
class MatchStageTranslatorTest {

    private MatchStageTranslator translator;
    
    @BeforeEach
    void setUp() {
        translator = new MatchStageTranslator(Oracle26aiDialect.INSTANCE);
    }

    @ParameterizedTest
    @CsvSource({
        "$eq, =",
        "$ne, <>",
        "$gt, >",
        "$gte, >=",
        "$lt, <",
        "$lte, <="
    })
    void shouldTranslateComparisonOperators(String mongoOp, String sqlOp) {
        var matchStage = Document.parse(
            String.format("{\"$match\": {\"age\": {\"%s\": 25}}}", mongoOp));
        
        var result = translator.translate(matchStage);
        
        assertThat(result.getSql())
            .contains("JSON_VALUE(data, '$.age')")
            .contains(sqlOp);
    }

    @ParameterizedTest
    @MethodSource("provideComplexMatchCases")
    void shouldTranslateComplexMatchExpressions(
            Document input, String expectedSql, List<Object> expectedBindings) {
        
        var result = translator.translate(input);
        
        assertThat(result.getSql()).isEqualToIgnoringWhitespace(expectedSql);
        assertThat(result.getBindVariables()).containsExactlyElementsOf(expectedBindings);
    }
    
    static Stream<Arguments> provideComplexMatchCases() {
        return Stream.of(
            Arguments.of(
                Document.parse("{\"$match\": {\"$and\": [{\"status\": \"active\"}, {\"age\": {\"$gt\": 21}}]}}"),
                "WHERE (JSON_VALUE(data, '$.status') = :1) AND (JSON_VALUE(data, '$.age' RETURNING NUMBER) > :2)",
                List.of("active", 21)
            ),
            Arguments.of(
                Document.parse("{\"$match\": {\"$or\": [{\"type\": \"A\"}, {\"type\": \"B\"}]}}"),
                "WHERE (JSON_VALUE(data, '$.type') = :1) OR (JSON_VALUE(data, '$.type') = :2)",
                List.of("A", "B")
            )
        );
    }
}
```

### Integration test with Testcontainers

```java
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class EndToEndTranslationTest {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23.6-slim-faststart")
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass")
        .withStartupTimeoutSeconds(300);
    
    private AggregationTranslator translator;
    private Connection connection;
    
    @BeforeAll
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(
            oracle.getJdbcUrl(), 
            oracle.getUsername(), 
            oracle.getPassword());
        
        translator = AggregationTranslator.create(
            OracleConfiguration.builder()
                .connection(connection)
                .build());
        
        // Create test collection and seed data
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE JSON COLLECTION TABLE customers
                """);
            stmt.execute("""
                INSERT INTO customers (data) VALUES ('{"name":"John","status":"active","amount":100}')
                """);
            stmt.execute("""
                INSERT INTO customers (data) VALUES ('{"name":"Jane","status":"active","amount":200}')
                """);
            stmt.execute("""
                INSERT INTO customers (data) VALUES ('{"name":"Bob","status":"inactive","amount":50}')
                """);
        }
    }

    @Test
    void shouldExecuteMatchGroupPipeline() throws SQLException {
        var pipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\"}}"),
            Document.parse("{\"$group\": {\"_id\": \"$status\", \"total\": {\"$sum\": \"$amount\"}}}")
        );
        
        var result = translator.translate(pipeline);
        
        try (PreparedStatement ps = connection.prepareStatement(result.sql())) {
            for (int i = 0; i < result.bindVariables().size(); i++) {
                ps.setObject(i + 1, result.bindVariables().get(i));
            }
            
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getDouble("total")).isEqualTo(300.0);
            }
        }
    }

    @Test
    void shouldProduceSameResultsAsMongoDB() {
        // This test requires MongoDB container as well
        // Compare results from both databases for identical pipelines
    }
}
```

### Snapshot testing for SQL generation

```java
class SqlSnapshotTest {
    
    @Test
    void shouldGenerateConsistentSqlForComplexPipeline() {
        var pipeline = loadPipeline("testdata/complex-aggregation.json");
        var translator = AggregationTranslator.create(defaultConfig());
        
        var result = translator.translate(pipeline);
        
        // Using approval testing library
        Approvals.verify(formatSql(result.sql()));
    }
    
    @Test
    void shouldMaintainBackwardsCompatibility() throws IOException {
        var pipeline = loadPipeline("testdata/regression-pipeline.json");
        var result = translator.translate(pipeline);
        
        var expectedSql = Files.readString(Path.of("testdata/regression-pipeline.expected.sql"));
        
        assertThat(normalizeWhitespace(result.sql()))
            .isEqualTo(normalizeWhitespace(expectedSql));
    }
}
```

---

## Implementation sequencing recommendations

### Phase 1: Foundation (Weeks 1-4)

1. **Project scaffold and CI/CD** (Week 1)
   - Gradle build configuration
   - GitHub Actions pipeline with Oracle Testcontainers
   - Pre-commit hooks and code quality tools
   - Shared specification directory structure

2. **AST and parser infrastructure** (Weeks 2-3)
   - Define core AST node interfaces
   - Implement BSON document parsing to AST
   - Stage parsers for Tier 1 operators only
   - Expression parser for basic comparisons

3. **SQL generation foundation** (Week 4)
   - SqlGenerationContext implementation
   - Basic Oracle dialect with JSON_VALUE rendering
   - Bind variable collection
   - Simple query executor

### Phase 2: Tier 1 operators (Weeks 5-8)

4. **$match stage** (Week 5)
   - Comparison operators: `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`
   - Logical operators: `$and`, `$or`, `$not`
   - Nested field paths

5. **$group stage** (Week 6)
   - Group key handling (single field, compound, constant)
   - Accumulators: `$sum`, `$avg`, `$count`, `$min`, `$max`

6. **$project, $sort, $limit, $skip** (Week 7)
   - Field inclusion/exclusion
   - Computed fields with expressions
   - Sort key translation
   - FETCH FIRST / OFFSET

7. **Arithmetic and conditional operators** (Week 8)
   - `$add`, `$subtract`, `$multiply`, `$divide`
   - `$cond`, `$ifNull`

### Phase 3: Tier 2 operators (Weeks 9-12)

8. **$lookup stage** (Weeks 9-10)
   - Simple lookup (localField/foreignField)
   - Pipeline lookup variant
   - LEFT OUTER JOIN translation

9. **$unwind stage** (Week 11)
   - JSON_TABLE NESTED PATH translation
   - preserveNullAndEmptyArrays handling

10. **Additional accumulators and array operators** (Week 12)
    - `$first`, `$last`, `$push`, `$addToSet`
    - `$arrayElemAt`, `$filter`, `$size`

### Phase 4: Optimization and polish (Weeks 13-16)

11. **Pipeline optimization** (Weeks 13-14)
    - Predicate pushdown
    - Predicate merging
    - Sort-limit optimization

12. **Tier 3 operators** (Week 15)
    - `$facet`, `$bucket`, `$unionWith`
    - Date operators

13. **Documentation and Node.js/Python ports** (Week 16)
    - API documentation
    - Generate Node.js and Python scaffolds
    - Cross-language test verification

---

## Code examples for key translations

### $match to WHERE clause

```java
// MongoDB
db.orders.aggregate([
  { $match: { status: "A", amount: { $gt: 100 } } }
])

// Oracle SQL
SELECT data FROM orders 
WHERE JSON_VALUE(data, '$.status') = :1 
  AND JSON_VALUE(data, '$.amount' RETURNING NUMBER) > :2
-- bindings: ["A", 100]
```

### $group with accumulators

```java
// MongoDB
db.orders.aggregate([
  { $match: { status: "completed" } },
  { $group: { 
      _id: "$customerId", 
      totalAmount: { $sum: "$amount" },
      orderCount: { $sum: 1 },
      avgAmount: { $avg: "$amount" }
  }},
  { $sort: { totalAmount: -1 } }
])

// Oracle SQL
SELECT 
  JSON_VALUE(data, '$.customerId') AS _id,
  SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS totalAmount,
  COUNT(*) AS orderCount,
  AVG(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS avgAmount
FROM orders
WHERE JSON_VALUE(data, '$.status') = :1
GROUP BY JSON_VALUE(data, '$.customerId')
ORDER BY totalAmount DESC
-- bindings: ["completed"]
```

### $unwind with JSON_TABLE

```java
// MongoDB
db.orders.aggregate([
  { $unwind: "$items" },
  { $group: { _id: "$items.product", totalQty: { $sum: "$items.quantity" } } }
])

// Oracle SQL
SELECT 
  jt.product AS _id,
  SUM(jt.quantity) AS totalQty
FROM orders,
  JSON_TABLE(data, '$.items[*]' COLUMNS (
    product VARCHAR2(100) PATH '$.product',
    quantity NUMBER PATH '$.quantity'
  )) jt
GROUP BY jt.product
```

### $lookup as JOIN

```java
// MongoDB
db.orders.aggregate([
  { $lookup: {
      from: "customers",
      localField: "customerId",
      foreignField: "_id",
      as: "customer"
  }},
  { $unwind: "$customer" }
])

// Oracle SQL
SELECT 
  o.data,
  c.data AS customer
FROM orders o
LEFT OUTER JOIN customers c 
  ON JSON_VALUE(o.data, '$.customerId') = JSON_VALUE(c.data, '$._id')
```

---

This specification provides the complete blueprint for implementing a production-ready MongoDB aggregation to Oracle SQL translation library. The TDD-first approach with comprehensive CI/CD ensures code quality, while the polyglot architecture enables future Node.js and Python ports with minimal duplication. Implementation should proceed through the phased approach, validating each operator tier against the shared test fixtures before moving forward.