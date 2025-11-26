# Phase 3: Tier 1 Operators

## Entry Criteria
- Phase 2 complete (core infrastructure working)
- Integration tests passing
- Pipeline parser operational

## Exit Criteria
- All Tier 1 operators fully implemented
- 80%+ code coverage
- All operators tested against Oracle

---

## IMPL-018: Comparison Expression Implementation

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-013, IMPL-016

### Description
Implement the ComparisonExpression for $eq, $gt, $gte, $lt, $lte, $ne operators.

### Acceptance Criteria
- [ ] Test: $eq renders to `=`
- [ ] Test: $gt renders to `>`
- [ ] Test: $gte renders to `>=`
- [ ] Test: $lt renders to `<`
- [ ] Test: $lte renders to `<=`
- [ ] Test: $ne renders to `<>`
- [ ] Test: Field path on left, literal on right
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/ComparisonExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.*;

class ComparisonExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @ParameterizedTest
    @CsvSource({
        "EQ, =",
        "NE, <>",
        "GT, >",
        "GTE, >=",
        "LT, <",
        "LTE, <="
    })
    void shouldRenderComparisonOperator(ComparisonOp op, String expectedSql) {
        var expr = new ComparisonExpression(
            op,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );

        expr.render(context);

        assertThat(context.toSql()).contains(expectedSql);
    }

    @Test
    void shouldRenderFieldPathEquality() {
        var expr = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.status') = :1");
        assertThat(context.getBindVariables()).containsExactly("active");
    }

    @Test
    void shouldRenderNumericComparison() {
        var expr = new ComparisonExpression(
            ComparisonOp.GT,
            FieldPathExpression.of("age", JsonReturnType.NUMBER),
            LiteralExpression.of(21)
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.age' RETURNING NUMBER) > :1");
        assertThat(context.getBindVariables()).containsExactly(21);
    }

    @Test
    void shouldRenderNestedFieldComparison() {
        var expr = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("customer.address.city"),
            LiteralExpression.of("New York")
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.customer.address.city') = :1");
    }

    @Test
    void shouldHandleNullComparison() {
        var expr = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("deletedAt"),
            LiteralExpression.ofNull()
        );

        expr.render(context);

        // NULL comparisons use IS NULL
        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.deletedAt') IS NULL");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/expression/ComparisonOp.java
package com.oracle.mongodb.translator.ast.expression;

/**
 * Comparison operators.
 */
public enum ComparisonOp {
    EQ("=", "$eq"),
    NE("<>", "$ne"),
    GT(">", "$gt"),
    GTE(">=", "$gte"),
    LT("<", "$lt"),
    LTE("<=", "$lte"),
    IN("IN", "$in"),
    NIN("NOT IN", "$nin");

    private final String sqlOperator;
    private final String mongoOperator;

    ComparisonOp(String sqlOperator, String mongoOperator) {
        this.sqlOperator = sqlOperator;
        this.mongoOperator = mongoOperator;
    }

    public String getSqlOperator() {
        return sqlOperator;
    }

    public String getMongoOperator() {
        return mongoOperator;
    }

    public static ComparisonOp fromMongo(String mongoOp) {
        for (ComparisonOp op : values()) {
            if (op.mongoOperator.equals(mongoOp)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown comparison operator: " + mongoOp);
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/ComparisonExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a comparison expression (e.g., $eq, $gt, $lt).
 */
public final class ComparisonExpression implements Expression {

    private final ComparisonOp op;
    private final Expression left;
    private final Expression right;

    public ComparisonExpression(ComparisonOp op, Expression left, Expression right) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.right = Objects.requireNonNull(right, "right must not be null");
    }

    public ComparisonOp getOp() {
        return op;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Handle NULL comparisons specially
        if (right instanceof LiteralExpression lit && lit.isNull()) {
            ctx.visit(left);
            if (op == ComparisonOp.EQ) {
                ctx.sql(" IS NULL");
            } else if (op == ComparisonOp.NE) {
                ctx.sql(" IS NOT NULL");
            } else {
                throw new IllegalStateException("Invalid NULL comparison with operator: " + op);
            }
            return;
        }

        ctx.visit(left);
        ctx.sql(" ");
        ctx.sql(op.getSqlOperator());
        ctx.sql(" ");
        ctx.visit(right);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComparisonExpression that = (ComparisonExpression) o;
        return op == that.op && Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, left, right);
    }

    @Override
    public String toString() {
        return "Comparison(" + left + " " + op + " " + right + ")";
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ComparisonOp.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ComparisonExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/ComparisonExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-019: Logical Expression Implementation

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-018

### Description
Implement LogicalExpression for $and, $or, $not operators.

### Acceptance Criteria
- [ ] Test: $and renders with `AND` between operands
- [ ] Test: $or renders with `OR` between operands
- [ ] Test: $not renders as `NOT (...)`
- [ ] Test: Nested logical expressions work
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/LogicalExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class LogicalExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderAndExpression() {
        var expr = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("status"), LiteralExpression.of("active")),
                new ComparisonExpression(ComparisonOp.GT,
                    FieldPathExpression.of("age", JsonReturnType.NUMBER), LiteralExpression.of(21))
            )
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(
            "(JSON_VALUE(data, '$.status') = :1) AND (JSON_VALUE(data, '$.age' RETURNING NUMBER) > :2)");
    }

    @Test
    void shouldRenderOrExpression() {
        var expr = new LogicalExpression(
            LogicalOp.OR,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("type"), LiteralExpression.of("A")),
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("type"), LiteralExpression.of("B"))
            )
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(
            "(JSON_VALUE(data, '$.type') = :1) OR (JSON_VALUE(data, '$.type') = :2)");
    }

    @Test
    void shouldRenderNotExpression() {
        var expr = new LogicalExpression(
            LogicalOp.NOT,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("deleted"), LiteralExpression.of(true))
            )
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(
            "NOT (JSON_VALUE(data, '$.deleted') = :1)");
    }

    @Test
    void shouldRenderNestedLogicalExpression() {
        var expr = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("active"), LiteralExpression.of(true)),
                new LogicalExpression(
                    LogicalOp.OR,
                    List.of(
                        new ComparisonExpression(ComparisonOp.EQ,
                            FieldPathExpression.of("role"), LiteralExpression.of("admin")),
                        new ComparisonExpression(ComparisonOp.EQ,
                            FieldPathExpression.of("role"), LiteralExpression.of("moderator"))
                    )
                )
            )
        );

        expr.render(context);

        assertThat(context.toSql()).contains("AND");
        assertThat(context.toSql()).contains("OR");
    }

    @Test
    void shouldHandleSingleOperandAnd() {
        var expr = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("status"), LiteralExpression.of("active"))
            )
        );

        expr.render(context);

        // Single operand should render without AND
        assertThat(context.toSql()).doesNotContain("AND");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/expression/LogicalOp.java
package com.oracle.mongodb.translator.ast.expression;

/**
 * Logical operators.
 */
public enum LogicalOp {
    AND("AND", "$and"),
    OR("OR", "$or"),
    NOT("NOT", "$not"),
    NOR("NOR", "$nor");

    private final String sqlOperator;
    private final String mongoOperator;

    LogicalOp(String sqlOperator, String mongoOperator) {
        this.sqlOperator = sqlOperator;
        this.mongoOperator = mongoOperator;
    }

    public String getSqlOperator() {
        return sqlOperator;
    }

    public String getMongoOperator() {
        return mongoOperator;
    }

    public static LogicalOp fromMongo(String mongoOp) {
        for (LogicalOp op : values()) {
            if (op.mongoOperator.equals(mongoOp)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown logical operator: " + mongoOp);
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/LogicalExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.List;
import java.util.Objects;

/**
 * Represents a logical expression ($and, $or, $not).
 */
public final class LogicalExpression implements Expression {

    private final LogicalOp op;
    private final List<Expression> operands;

    public LogicalExpression(LogicalOp op, List<Expression> operands) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.operands = List.copyOf(Objects.requireNonNull(operands, "operands must not be null"));

        if (operands.isEmpty()) {
            throw new IllegalArgumentException("Logical expression must have at least one operand");
        }
        if (op == LogicalOp.NOT && operands.size() != 1) {
            throw new IllegalArgumentException("NOT expression must have exactly one operand");
        }
    }

    public LogicalOp getOp() {
        return op;
    }

    public List<Expression> getOperands() {
        return operands;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        if (op == LogicalOp.NOT) {
            renderNot(ctx);
        } else if (op == LogicalOp.NOR) {
            renderNor(ctx);
        } else {
            renderBinaryLogical(ctx);
        }
    }

    private void renderNot(SqlGenerationContext ctx) {
        ctx.sql("NOT (");
        ctx.visit(operands.get(0));
        ctx.sql(")");
    }

    private void renderNor(SqlGenerationContext ctx) {
        // NOR is NOT (a OR b OR c)
        ctx.sql("NOT (");
        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                ctx.sql(" OR ");
            }
            ctx.sql("(");
            ctx.visit(operands.get(i));
            ctx.sql(")");
        }
        ctx.sql(")");
    }

    private void renderBinaryLogical(SqlGenerationContext ctx) {
        if (operands.size() == 1) {
            // Single operand - just render it without operator
            ctx.visit(operands.get(0));
            return;
        }

        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                ctx.sql(" ");
                ctx.sql(op.getSqlOperator());
                ctx.sql(" ");
            }
            ctx.sql("(");
            ctx.visit(operands.get(i));
            ctx.sql(")");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogicalExpression that = (LogicalExpression) o;
        return op == that.op && Objects.equals(operands, that.operands);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, operands);
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/LogicalOp.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/LogicalExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/LogicalExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-020: Expression Parser

**Phase:** 3
**Complexity:** L
**Dependencies:** IMPL-018, IMPL-019

### Description
Create ExpressionParser to convert BSON filter documents to Expression AST nodes.

### Acceptance Criteria
- [ ] Test: Parse simple equality `{"status": "active"}`
- [ ] Test: Parse comparison `{"age": {"$gt": 21}}`
- [ ] Test: Parse $and expression
- [ ] Test: Parse $or expression
- [ ] Test: Parse nested expressions
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/parser/ExpressionParserTest.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.*;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ExpressionParserTest {

    private ExpressionParser parser;
    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        parser = new ExpressionParser();
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldParseSimpleEquality() {
        var doc = Document.parse("{\"status\": \"active\"}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.status') = :1");
        assertThat(context.getBindVariables()).containsExactly("active");
    }

    @Test
    void shouldParseComparisonOperator() {
        var doc = Document.parse("{\"age\": {\"$gt\": 21}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("JSON_VALUE(data, '$.age'");
        assertThat(context.toSql()).contains("> :1");
    }

    @Test
    void shouldParseMultipleFieldsAsAnd() {
        var doc = Document.parse("{\"status\": \"active\", \"age\": {\"$gte\": 18}}");

        var expr = parser.parse(doc);

        assertThat(expr).isInstanceOf(LogicalExpression.class);
        var logical = (LogicalExpression) expr;
        assertThat(logical.getOp()).isEqualTo(LogicalOp.AND);
    }

    @Test
    void shouldParseExplicitAnd() {
        var doc = Document.parse("{\"$and\": [{\"status\": \"active\"}, {\"age\": {\"$gt\": 21}}]}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("AND");
    }

    @Test
    void shouldParseOr() {
        var doc = Document.parse("{\"$or\": [{\"type\": \"A\"}, {\"type\": \"B\"}]}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("OR");
    }

    @Test
    void shouldParseNestedPath() {
        var doc = Document.parse("{\"customer.address.city\": \"New York\"}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("$.customer.address.city");
    }

    @Test
    void shouldParseNot() {
        var doc = Document.parse("{\"status\": {\"$not\": {\"$eq\": \"deleted\"}}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("NOT");
    }

    @Test
    void shouldParseIn() {
        var doc = Document.parse("{\"status\": {\"$in\": [\"active\", \"pending\"]}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("IN");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/parser/ExpressionParser.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.*;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses MongoDB filter expressions into AST Expression nodes.
 */
public class ExpressionParser {

    private static final Set<String> COMPARISON_OPS = Set.of(
        "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin"
    );

    private static final Set<String> LOGICAL_OPS = Set.of(
        "$and", "$or", "$not", "$nor"
    );

    /**
     * Parses a filter document into an Expression.
     */
    public Expression parse(Document filter) {
        return parseDocument(filter);
    }

    private Expression parseDocument(Document doc) {
        List<Expression> conditions = new ArrayList<>();

        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.startsWith("$")) {
                // Logical operator at top level
                conditions.add(parseTopLevelOperator(key, value));
            } else {
                // Field condition
                conditions.add(parseFieldCondition(key, value));
            }
        }

        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("Empty filter document");
        }

        if (conditions.size() == 1) {
            return conditions.get(0);
        }

        // Multiple conditions are implicitly ANDed
        return new LogicalExpression(LogicalOp.AND, conditions);
    }

    private Expression parseTopLevelOperator(String op, Object value) {
        if (LOGICAL_OPS.contains(op)) {
            return parseLogicalOperator(op, value);
        }
        throw new UnsupportedOperatorException(op);
    }

    private Expression parseLogicalOperator(String op, Object value) {
        LogicalOp logicalOp = LogicalOp.fromMongo(op);

        if (logicalOp == LogicalOp.NOT) {
            if (!(value instanceof Document)) {
                throw new IllegalArgumentException("$not requires a document value");
            }
            return new LogicalExpression(logicalOp, List.of(parseDocument((Document) value)));
        }

        if (!(value instanceof List)) {
            throw new IllegalArgumentException(op + " requires an array value");
        }

        @SuppressWarnings("unchecked")
        List<Document> docs = (List<Document>) value;
        List<Expression> operands = new ArrayList<>();

        for (Document doc : docs) {
            operands.add(parseDocument(doc));
        }

        return new LogicalExpression(logicalOp, operands);
    }

    private Expression parseFieldCondition(String fieldPath, Object value) {
        if (value instanceof Document) {
            return parseFieldOperators(fieldPath, (Document) value);
        }

        // Simple equality: {"status": "active"}
        return new ComparisonExpression(
            ComparisonOp.EQ,
            createFieldPath(fieldPath, value),
            LiteralExpression.of(value)
        );
    }

    private Expression parseFieldOperators(String fieldPath, Document operators) {
        List<Expression> conditions = new ArrayList<>();

        for (Map.Entry<String, Object> entry : operators.entrySet()) {
            String op = entry.getKey();
            Object value = entry.getValue();

            if (COMPARISON_OPS.contains(op)) {
                conditions.add(parseComparisonOperator(fieldPath, op, value));
            } else if (op.equals("$not")) {
                conditions.add(parseNotOperator(fieldPath, value));
            } else if (op.equals("$exists")) {
                conditions.add(parseExistsOperator(fieldPath, value));
            } else {
                throw new UnsupportedOperatorException(op);
            }
        }

        if (conditions.size() == 1) {
            return conditions.get(0);
        }

        return new LogicalExpression(LogicalOp.AND, conditions);
    }

    private Expression parseComparisonOperator(String fieldPath, String op, Object value) {
        ComparisonOp comparisonOp = ComparisonOp.fromMongo(op);

        if (comparisonOp == ComparisonOp.IN || comparisonOp == ComparisonOp.NIN) {
            return parseInOperator(fieldPath, comparisonOp, value);
        }

        return new ComparisonExpression(
            comparisonOp,
            createFieldPath(fieldPath, value),
            LiteralExpression.of(value)
        );
    }

    private Expression parseInOperator(String fieldPath, ComparisonOp op, Object value) {
        if (!(value instanceof List)) {
            throw new IllegalArgumentException(op.getMongoOperator() + " requires an array");
        }

        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) value;

        // Create IN expression with array of values
        return new InExpression(
            createFieldPath(fieldPath, values.isEmpty() ? null : values.get(0)),
            values,
            op == ComparisonOp.NIN
        );
    }

    private Expression parseNotOperator(String fieldPath, Object value) {
        if (!(value instanceof Document)) {
            throw new IllegalArgumentException("$not requires a document value");
        }

        Expression inner = parseFieldOperators(fieldPath, (Document) value);
        return new LogicalExpression(LogicalOp.NOT, List.of(inner));
    }

    private Expression parseExistsOperator(String fieldPath, Object value) {
        boolean exists = value instanceof Boolean && (Boolean) value;
        // Use JSON_EXISTS for existence check
        return new ExistsExpression(FieldPathExpression.of(fieldPath), exists);
    }

    private FieldPathExpression createFieldPath(String path, Object sampleValue) {
        JsonReturnType returnType = inferReturnType(sampleValue);
        return FieldPathExpression.of(path, returnType);
    }

    private JsonReturnType inferReturnType(Object value) {
        if (value instanceof Number) {
            return JsonReturnType.NUMBER;
        }
        if (value instanceof Boolean) {
            return JsonReturnType.BOOLEAN;
        }
        return null; // Default VARCHAR
    }
}
```

#### Step 3: Add supporting expression classes

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/expression/InExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.List;
import java.util.Objects;

/**
 * Represents an IN/NOT IN expression.
 */
public final class InExpression implements Expression {

    private final FieldPathExpression field;
    private final List<Object> values;
    private final boolean negated;

    public InExpression(FieldPathExpression field, List<Object> values, boolean negated) {
        this.field = Objects.requireNonNull(field);
        this.values = List.copyOf(values);
        this.negated = negated;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.visit(field);
        if (negated) {
            ctx.sql(" NOT");
        }
        ctx.sql(" IN (");

        for (int i = 0; i < values.size(); i++) {
            if (i > 0) {
                ctx.sql(", ");
            }
            ctx.bind(values.get(i));
        }

        ctx.sql(")");
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/ExistsExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents an $exists check.
 */
public final class ExistsExpression implements Expression {

    private final FieldPathExpression field;
    private final boolean shouldExist;

    public ExistsExpression(FieldPathExpression field, boolean shouldExist) {
        this.field = Objects.requireNonNull(field);
        this.shouldExist = shouldExist;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        if (shouldExist) {
            ctx.sql("JSON_EXISTS(data, '");
            ctx.sql(field.getJsonPath());
            ctx.sql("')");
        } else {
            ctx.sql("NOT JSON_EXISTS(data, '");
            ctx.sql(field.getJsonPath());
            ctx.sql("')");
        }
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/ExpressionParser.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/InExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ExistsExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/parser/ExpressionParserTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-021: $match Stage Implementation

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-020

### Description
Complete implementation of the $match stage using the ExpressionParser.

### Acceptance Criteria
- [ ] Test: $match with simple equality
- [ ] Test: $match with comparison operators
- [ ] Test: $match with $and/$or
- [ ] Test: Pipeline renders correct WHERE clause
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/stage/MatchStageTest.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.*;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MatchStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderWhereClause() {
        var filter = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );
        var stage = new MatchStage(filter);

        stage.render(context);

        assertThat(context.toSql()).isEqualTo("WHERE JSON_VALUE(data, '$.status') = :1");
    }

    @Test
    void shouldRenderComplexFilter() {
        var filter = new LogicalExpression(
            LogicalOp.AND,
            java.util.List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("status"), LiteralExpression.of("active")),
                new ComparisonExpression(ComparisonOp.GT,
                    FieldPathExpression.of("amount", JsonReturnType.NUMBER), LiteralExpression.of(100))
            )
        );
        var stage = new MatchStage(filter);

        stage.render(context);

        assertThat(context.toSql())
            .startsWith("WHERE ")
            .contains("AND")
            .contains("$.status")
            .contains("$.amount");
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new MatchStage(
            new ComparisonExpression(ComparisonOp.EQ,
                FieldPathExpression.of("x"), LiteralExpression.of(1))
        );

        assertThat(stage.getOperatorName()).isEqualTo("$match");
    }
}

// core/src/test/java/com/oracle/mongodb/translator/parser/MatchStageParserTest.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class MatchStageParserTest {

    private StageParserRegistry registry;
    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        registry = new StageParserRegistry();
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldParseMatchStage() {
        var parser = registry.getParser("$match");

        var stage = parser.parse(Document.parse("{\"status\": \"active\"}"));

        assertThat(stage).isInstanceOf(MatchStage.class);
    }

    @Test
    void shouldRenderParsedMatchStage() {
        var parser = registry.getParser("$match");

        var stage = parser.parse(Document.parse("{\"status\": \"active\", \"age\": {\"$gte\": 18}}"));
        stage.render(context);

        assertThat(context.toSql())
            .startsWith("WHERE ")
            .contains("$.status")
            .contains("$.age");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/stage/MatchStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $match stage that filters documents.
 */
public final class MatchStage implements Stage {

    private final Expression filter;

    public MatchStage(Expression filter) {
        this.filter = Objects.requireNonNull(filter, "filter must not be null");
    }

    public Expression getFilter() {
        return filter;
    }

    @Override
    public String getOperatorName() {
        return "$match";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("WHERE ");
        ctx.visit(filter);
    }
}

// Update StageParserRegistry to include $match
// core/src/main/java/com/oracle/mongodb/translator/parser/StageParserRegistry.java
// Add in registerBuiltInParsers():

private void registerBuiltInParsers() {
    // Tier 1 simple stages
    register("$limit", value -> new LimitStage(toInt(value)));
    register("$skip", value -> new SkipStage(toInt(value)));

    // $match stage
    ExpressionParser expressionParser = new ExpressionParser();
    register("$match", value -> {
        if (!(value instanceof Document)) {
            throw new IllegalArgumentException("$match requires a document");
        }
        Expression filter = expressionParser.parse((Document) value);
        return new MatchStage(filter);
    });
}
```

### Files
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/MatchStage.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/parser/StageParserRegistry.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/stage/MatchStageTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/parser/MatchStageParserTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-022: Accumulator Expression Implementation

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-013

### Description
Implement AccumulatorExpression for $sum, $avg, $count, $min, $max.

### Acceptance Criteria
- [ ] Test: $sum renders to `SUM(...)`
- [ ] Test: $avg renders to `AVG(...)`
- [ ] Test: $count renders to `COUNT(*)`
- [ ] Test: $min renders to `MIN(...)`
- [ ] Test: $max renders to `MAX(...)`
- [ ] Test: Accumulator on field path works
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/AccumulatorExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.*;

class AccumulatorExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderSumAccumulator() {
        var expr = new AccumulatorExpression(
            AccumulatorOp.SUM,
            FieldPathExpression.of("amount", JsonReturnType.NUMBER)
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER))");
    }

    @Test
    void shouldRenderAvgAccumulator() {
        var expr = new AccumulatorExpression(
            AccumulatorOp.AVG,
            FieldPathExpression.of("price", JsonReturnType.NUMBER)
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("AVG(JSON_VALUE(data, '$.price' RETURNING NUMBER))");
    }

    @Test
    void shouldRenderCountAccumulator() {
        var expr = new AccumulatorExpression(AccumulatorOp.COUNT, null);

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("COUNT(*)");
    }

    @Test
    void shouldRenderSumWithConstant() {
        // $sum: 1 counts documents
        var expr = new AccumulatorExpression(
            AccumulatorOp.SUM,
            LiteralExpression.of(1)
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("SUM(:1)");
    }

    @ParameterizedTest
    @CsvSource({
        "MIN, MIN",
        "MAX, MAX",
        "SUM, SUM",
        "AVG, AVG"
    })
    void shouldRenderAccumulatorFunction(AccumulatorOp op, String expectedSql) {
        var expr = new AccumulatorExpression(
            op,
            FieldPathExpression.of("value", JsonReturnType.NUMBER)
        );

        expr.render(context);

        assertThat(context.toSql()).startsWith(expectedSql + "(");
    }

    @Test
    void shouldRenderFirstAccumulator() {
        var expr = new AccumulatorExpression(
            AccumulatorOp.FIRST,
            FieldPathExpression.of("name")
        );

        expr.render(context);

        // Uses aggregate FIRST_VALUE or similar
        assertThat(context.toSql()).contains("FIRST");
    }

    @Test
    void shouldRenderLastAccumulator() {
        var expr = new AccumulatorExpression(
            AccumulatorOp.LAST,
            FieldPathExpression.of("name")
        );

        expr.render(context);

        assertThat(context.toSql()).contains("LAST");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/expression/AccumulatorOp.java
package com.oracle.mongodb.translator.ast.expression;

/**
 * Accumulator operators for $group.
 */
public enum AccumulatorOp {
    SUM("SUM", "$sum"),
    AVG("AVG", "$avg"),
    COUNT("COUNT", "$count"),
    MIN("MIN", "$min"),
    MAX("MAX", "$max"),
    FIRST("FIRST_VALUE", "$first"),
    LAST("LAST_VALUE", "$last"),
    PUSH("JSON_ARRAYAGG", "$push"),
    ADD_TO_SET("JSON_ARRAYAGG", "$addToSet"),
    STD_DEV_POP("STDDEV_POP", "$stdDevPop"),
    STD_DEV_SAMP("STDDEV_SAMP", "$stdDevSamp");

    private final String sqlFunction;
    private final String mongoOperator;

    AccumulatorOp(String sqlFunction, String mongoOperator) {
        this.sqlFunction = sqlFunction;
        this.mongoOperator = mongoOperator;
    }

    public String getSqlFunction() {
        return sqlFunction;
    }

    public String getMongoOperator() {
        return mongoOperator;
    }

    public static AccumulatorOp fromMongo(String mongoOp) {
        for (AccumulatorOp op : values()) {
            if (op.mongoOperator.equals(mongoOp)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown accumulator operator: " + mongoOp);
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/AccumulatorExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents an accumulator expression ($sum, $avg, etc.).
 */
public final class AccumulatorExpression implements Expression {

    private final AccumulatorOp op;
    private final Expression argument; // null for COUNT(*)

    public AccumulatorExpression(AccumulatorOp op, Expression argument) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.argument = argument;
    }

    public AccumulatorOp getOp() {
        return op;
    }

    public Expression getArgument() {
        return argument;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        switch (op) {
            case COUNT -> renderCount(ctx);
            case FIRST -> renderFirst(ctx);
            case LAST -> renderLast(ctx);
            case PUSH, ADD_TO_SET -> renderArrayAgg(ctx);
            default -> renderSimpleAggregate(ctx);
        }
    }

    private void renderCount(SqlGenerationContext ctx) {
        ctx.sql("COUNT(*)");
    }

    private void renderSimpleAggregate(SqlGenerationContext ctx) {
        ctx.sql(op.getSqlFunction());
        ctx.sql("(");
        if (argument != null) {
            ctx.visit(argument);
        } else {
            ctx.sql("*");
        }
        ctx.sql(")");
    }

    private void renderFirst(SqlGenerationContext ctx) {
        // Oracle doesn't have a simple FIRST aggregate - use analytic function pattern
        // For GROUP BY, we typically need KEEP (DENSE_RANK FIRST ORDER BY ...)
        ctx.sql("MIN(");
        if (argument != null) {
            ctx.visit(argument);
        }
        ctx.sql(") KEEP (DENSE_RANK FIRST ORDER BY ROWNUM)");
    }

    private void renderLast(SqlGenerationContext ctx) {
        ctx.sql("MAX(");
        if (argument != null) {
            ctx.visit(argument);
        }
        ctx.sql(") KEEP (DENSE_RANK LAST ORDER BY ROWNUM)");
    }

    private void renderArrayAgg(SqlGenerationContext ctx) {
        ctx.sql("JSON_ARRAYAGG(");
        if (argument != null) {
            ctx.visit(argument);
        }
        if (op == AccumulatorOp.ADD_TO_SET) {
            // DISTINCT for $addToSet
            ctx.sql(" DISTINCT");
        }
        ctx.sql(")");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccumulatorExpression that = (AccumulatorExpression) o;
        return op == that.op && Objects.equals(argument, that.argument);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, argument);
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/AccumulatorOp.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/AccumulatorExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/AccumulatorExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-023: $group Stage Implementation

**Phase:** 3
**Complexity:** L
**Dependencies:** IMPL-022

### Description
Complete implementation of the $group stage with _id field and accumulators.

### Acceptance Criteria
- [ ] Test: $group with single field _id
- [ ] Test: $group with compound _id
- [ ] Test: $group with null _id (no grouping)
- [ ] Test: $group with multiple accumulators
- [ ] Test: Renders correct GROUP BY clause
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/stage/GroupStageTest.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.*;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;

class GroupStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderGroupBySingleField() {
        var groupKey = GroupKey.singleField(FieldPathExpression.of("category"));
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", new AccumulatorExpression(
            AccumulatorOp.SUM, FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var stage = new GroupStage(groupKey, accumulators);
        stage.render(context);

        assertThat(context.toSql())
            .contains("JSON_VALUE(data, '$.category') AS _id")
            .contains("SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS total")
            .contains("GROUP BY JSON_VALUE(data, '$.category')");
    }

    @Test
    void shouldRenderGroupByNull() {
        // _id: null means aggregate all documents
        var groupKey = GroupKey.nullKey();
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", new AccumulatorExpression(AccumulatorOp.COUNT, null));
        accumulators.put("total", new AccumulatorExpression(
            AccumulatorOp.SUM, FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var stage = new GroupStage(groupKey, accumulators);
        stage.render(context);

        assertThat(context.toSql())
            .contains("NULL AS _id")
            .contains("COUNT(*) AS count")
            .doesNotContain("GROUP BY");
    }

    @Test
    void shouldRenderGroupByCompoundKey() {
        var fields = new LinkedHashMap<String, Expression>();
        fields.put("year", FieldPathExpression.of("year", JsonReturnType.NUMBER));
        fields.put("month", FieldPathExpression.of("month", JsonReturnType.NUMBER));
        var groupKey = GroupKey.compound(fields);

        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", new AccumulatorExpression(AccumulatorOp.COUNT, null));

        var stage = new GroupStage(groupKey, accumulators);
        stage.render(context);

        assertThat(context.toSql())
            .contains("$.year")
            .contains("$.month")
            .contains("GROUP BY");
    }

    @Test
    void shouldRenderMultipleAccumulators() {
        var groupKey = GroupKey.singleField(FieldPathExpression.of("category"));
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", new AccumulatorExpression(
            AccumulatorOp.SUM, FieldPathExpression.of("amount", JsonReturnType.NUMBER)));
        accumulators.put("average", new AccumulatorExpression(
            AccumulatorOp.AVG, FieldPathExpression.of("amount", JsonReturnType.NUMBER)));
        accumulators.put("count", new AccumulatorExpression(AccumulatorOp.COUNT, null));

        var stage = new GroupStage(groupKey, accumulators);
        stage.render(context);

        assertThat(context.toSql())
            .contains("SUM(")
            .contains("AVG(")
            .contains("COUNT(*)");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/stage/GroupKey.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the _id field in a $group stage.
 */
public sealed interface GroupKey extends AstNode {

    /**
     * Returns true if this key results in no GROUP BY (aggregates all).
     */
    boolean isNull();

    /**
     * Creates a null key (no grouping).
     */
    static GroupKey nullKey() {
        return NullGroupKey.INSTANCE;
    }

    /**
     * Creates a single field key.
     */
    static GroupKey singleField(FieldPathExpression field) {
        return new SingleFieldKey(field);
    }

    /**
     * Creates a compound key with multiple fields.
     */
    static GroupKey compound(Map<String, Expression> fields) {
        return new CompoundKey(new LinkedHashMap<>(fields));
    }

    // Implementations

    final class NullGroupKey implements GroupKey {
        static final NullGroupKey INSTANCE = new NullGroupKey();

        private NullGroupKey() {}

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public void render(SqlGenerationContext ctx) {
            ctx.sql("NULL AS _id");
        }
    }

    final class SingleFieldKey implements GroupKey {
        private final FieldPathExpression field;

        SingleFieldKey(FieldPathExpression field) {
            this.field = Objects.requireNonNull(field);
        }

        public FieldPathExpression getField() {
            return field;
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public void render(SqlGenerationContext ctx) {
            ctx.visit(field);
            ctx.sql(" AS _id");
        }

        public void renderGroupBy(SqlGenerationContext ctx) {
            ctx.visit(field);
        }
    }

    final class CompoundKey implements GroupKey {
        private final LinkedHashMap<String, Expression> fields;

        CompoundKey(LinkedHashMap<String, Expression> fields) {
            this.fields = new LinkedHashMap<>(fields);
        }

        public Map<String, Expression> getFields() {
            return Map.copyOf(fields);
        }

        @Override
        public boolean isNull() {
            return false;
        }

        @Override
        public void render(SqlGenerationContext ctx) {
            // Render as JSON object for compound key
            ctx.sql("JSON_OBJECT(");
            boolean first = true;
            for (Map.Entry<String, Expression> entry : fields.entrySet()) {
                if (!first) {
                    ctx.sql(", ");
                }
                first = false;
                ctx.sql("'");
                ctx.sql(entry.getKey());
                ctx.sql("' VALUE ");
                ctx.visit(entry.getValue());
            }
            ctx.sql(") AS _id");
        }

        public void renderGroupBy(SqlGenerationContext ctx) {
            boolean first = true;
            for (Expression expr : fields.values()) {
                if (!first) {
                    ctx.sql(", ");
                }
                first = false;
                ctx.visit(expr);
            }
        }
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/stage/GroupStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a $group stage.
 */
public final class GroupStage implements Stage {

    private final GroupKey groupKey;
    private final LinkedHashMap<String, AccumulatorExpression> accumulators;

    public GroupStage(GroupKey groupKey, Map<String, AccumulatorExpression> accumulators) {
        this.groupKey = Objects.requireNonNull(groupKey);
        this.accumulators = new LinkedHashMap<>(accumulators);
    }

    public GroupKey getGroupKey() {
        return groupKey;
    }

    public Map<String, AccumulatorExpression> getAccumulators() {
        return Map.copyOf(accumulators);
    }

    @Override
    public String getOperatorName() {
        return "$group";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Note: This renders the SELECT and GROUP BY portions
        // The Pipeline class should handle combining with FROM

        // SELECT clause
        ctx.sql("SELECT ");
        ctx.visit(groupKey);

        for (Map.Entry<String, AccumulatorExpression> entry : accumulators.entrySet()) {
            ctx.sql(", ");
            ctx.visit(entry.getValue());
            ctx.sql(" AS ");
            ctx.identifier(entry.getKey());
        }

        // GROUP BY clause (only if not null key)
        if (!groupKey.isNull()) {
            ctx.sql(" GROUP BY ");
            renderGroupByClause(ctx);
        }
    }

    private void renderGroupByClause(SqlGenerationContext ctx) {
        if (groupKey instanceof GroupKey.SingleFieldKey single) {
            single.renderGroupBy(ctx);
        } else if (groupKey instanceof GroupKey.CompoundKey compound) {
            compound.renderGroupBy(ctx);
        }
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/GroupKey.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/GroupStage.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/stage/GroupStageTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-024: $group Stage Parser

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-023

### Description
Add $group parsing to the StageParserRegistry.

### Acceptance Criteria
- [ ] Test: Parse simple $group with single _id
- [ ] Test: Parse $group with compound _id
- [ ] Test: Parse $group with null _id
- [ ] Test: Parse all accumulator types
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/parser/GroupStageParserTest.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class GroupStageParserTest {

    private StageParserRegistry registry;
    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        registry = new StageParserRegistry();
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldParseGroupWithSingleFieldId() {
        var parser = registry.getParser("$group");
        var doc = Document.parse("{\"_id\": \"$category\", \"total\": {\"$sum\": \"$amount\"}}");

        var stage = parser.parse(doc);

        assertThat(stage).isInstanceOf(GroupStage.class);
        stage.render(context);
        assertThat(context.toSql())
            .contains("$.category")
            .contains("SUM(")
            .contains("$.amount");
    }

    @Test
    void shouldParseGroupWithNullId() {
        var parser = registry.getParser("$group");
        var doc = Document.parse("{\"_id\": null, \"count\": {\"$sum\": 1}}");

        var stage = parser.parse(doc);
        stage.render(context);

        assertThat(context.toSql())
            .contains("NULL AS _id")
            .doesNotContain("GROUP BY");
    }

    @Test
    void shouldParseGroupWithCompoundId() {
        var parser = registry.getParser("$group");
        var doc = Document.parse(
            "{\"_id\": {\"year\": \"$year\", \"month\": \"$month\"}, \"count\": {\"$sum\": 1}}");

        var stage = parser.parse(doc);
        stage.render(context);

        assertThat(context.toSql())
            .contains("$.year")
            .contains("$.month");
    }

    @Test
    void shouldParseMultipleAccumulators() {
        var parser = registry.getParser("$group");
        var doc = Document.parse("""
            {
                "_id": "$status",
                "total": {"$sum": "$amount"},
                "average": {"$avg": "$amount"},
                "count": {"$sum": 1},
                "minAmount": {"$min": "$amount"},
                "maxAmount": {"$max": "$amount"}
            }
            """);

        var stage = parser.parse(doc);
        stage.render(context);

        assertThat(context.toSql())
            .contains("SUM(")
            .contains("AVG(")
            .contains("MIN(")
            .contains("MAX(");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/parser/GroupStageParser.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.*;
import com.oracle.mongodb.translator.ast.stage.GroupKey;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parses $group stage documents.
 */
public class GroupStageParser implements StageParser<GroupStage> {

    @Override
    public GroupStage parse(Object value) {
        if (!(value instanceof Document doc)) {
            throw new IllegalArgumentException("$group requires a document");
        }

        // Parse _id field
        GroupKey groupKey = parseGroupKey(doc.get("_id"));

        // Parse accumulators
        LinkedHashMap<String, AccumulatorExpression> accumulators = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            if (!"_id".equals(entry.getKey())) {
                accumulators.put(entry.getKey(), parseAccumulator(entry.getValue()));
            }
        }

        return new GroupStage(groupKey, accumulators);
    }

    private GroupKey parseGroupKey(Object idValue) {
        if (idValue == null) {
            return GroupKey.nullKey();
        }

        if (idValue instanceof String str) {
            // Single field: "$fieldName"
            if (str.startsWith("$")) {
                return GroupKey.singleField(FieldPathExpression.of(str.substring(1)));
            }
            throw new IllegalArgumentException("Group _id field must start with $");
        }

        if (idValue instanceof Document doc) {
            // Compound key
            LinkedHashMap<String, Expression> fields = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldValue = entry.getValue();

                if (fieldValue instanceof String str && str.startsWith("$")) {
                    fields.put(fieldName, FieldPathExpression.of(str.substring(1)));
                } else {
                    // Could be an expression - for now just handle field refs
                    throw new IllegalArgumentException(
                        "Complex expressions in _id not yet supported");
                }
            }
            return GroupKey.compound(fields);
        }

        throw new IllegalArgumentException("Invalid _id value type: " + idValue.getClass());
    }

    private AccumulatorExpression parseAccumulator(Object value) {
        if (!(value instanceof Document doc)) {
            throw new IllegalArgumentException("Accumulator must be a document");
        }

        if (doc.size() != 1) {
            throw new IllegalArgumentException("Accumulator must have exactly one operator");
        }

        String opName = doc.keySet().iterator().next();
        Object argValue = doc.get(opName);

        AccumulatorOp op;
        try {
            op = AccumulatorOp.fromMongo(opName);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperatorException(opName);
        }

        Expression argument = parseAccumulatorArgument(argValue);
        return new AccumulatorExpression(op, argument);
    }

    private Expression parseAccumulatorArgument(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String str) {
            if (str.startsWith("$")) {
                // Field reference - infer numeric for accumulators
                return FieldPathExpression.of(str.substring(1), JsonReturnType.NUMBER);
            }
            return LiteralExpression.of(str);
        }

        if (value instanceof Number num) {
            return LiteralExpression.of(num);
        }

        if (value instanceof Document doc) {
            // Expression document - not yet implemented
            throw new UnsupportedOperatorException("Complex accumulator expressions");
        }

        return LiteralExpression.of(value);
    }
}

// Update StageParserRegistry
// Add in registerBuiltInParsers():
register("$group", new GroupStageParser());
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/GroupStageParser.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/parser/StageParserRegistry.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/parser/GroupStageParserTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-025: $project Stage Implementation

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-013

### Description
Implement the $project stage for field inclusion/exclusion and computed fields.

### Acceptance Criteria
- [ ] Test: Field inclusion `{field: 1}`
- [ ] Test: Field exclusion `{field: 0}`
- [ ] Test: Field rename `{newName: "$oldName"}`
- [ ] Test: Computed field with expression
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/stage/ProjectStageTest.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.*;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import static org.assertj.core.api.Assertions.*;

class ProjectStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderIncludedFields() {
        var projections = new LinkedHashMap<String, ProjectionSpec>();
        projections.put("name", ProjectionSpec.include());
        projections.put("email", ProjectionSpec.include());

        var stage = new ProjectStage(projections);
        stage.render(context);

        assertThat(context.toSql())
            .contains("JSON_VALUE(data, '$.name') AS name")
            .contains("JSON_VALUE(data, '$.email') AS email");
    }

    @Test
    void shouldRenderFieldRename() {
        var projections = new LinkedHashMap<String, ProjectionSpec>();
        projections.put("customerName", ProjectionSpec.field(FieldPathExpression.of("customer.name")));

        var stage = new ProjectStage(projections);
        stage.render(context);

        assertThat(context.toSql())
            .contains("JSON_VALUE(data, '$.customer.name') AS customerName");
    }

    @Test
    void shouldRenderComputedField() {
        var projections = new LinkedHashMap<String, ProjectionSpec>();
        projections.put("total", ProjectionSpec.computed(
            new ArithmeticExpression(
                ArithmeticOp.MULTIPLY,
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                FieldPathExpression.of("quantity", JsonReturnType.NUMBER)
            )
        ));

        var stage = new ProjectStage(projections);
        stage.render(context);

        assertThat(context.toSql())
            .contains("$.price")
            .contains("$.quantity")
            .contains("AS total");
    }

    @Test
    void shouldRenderExcludedId() {
        var projections = new LinkedHashMap<String, ProjectionSpec>();
        projections.put("_id", ProjectionSpec.exclude());
        projections.put("name", ProjectionSpec.include());

        var stage = new ProjectStage(projections);
        stage.render(context);

        // _id excluded means it's not in SELECT
        assertThat(context.toSql()).doesNotContain("_id");
        assertThat(context.toSql()).contains("name");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/stage/ProjectionSpec.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;

/**
 * Specification for a projection field.
 */
public sealed interface ProjectionSpec {

    boolean isIncluded();

    static ProjectionSpec include() {
        return IncludeSpec.INSTANCE;
    }

    static ProjectionSpec exclude() {
        return ExcludeSpec.INSTANCE;
    }

    static ProjectionSpec field(FieldPathExpression source) {
        return new FieldRenameSpec(source);
    }

    static ProjectionSpec computed(Expression expression) {
        return new ComputedSpec(expression);
    }

    // Implementations

    final class IncludeSpec implements ProjectionSpec {
        static final IncludeSpec INSTANCE = new IncludeSpec();

        @Override
        public boolean isIncluded() {
            return true;
        }
    }

    final class ExcludeSpec implements ProjectionSpec {
        static final ExcludeSpec INSTANCE = new ExcludeSpec();

        @Override
        public boolean isIncluded() {
            return false;
        }
    }

    record FieldRenameSpec(FieldPathExpression source) implements ProjectionSpec {
        @Override
        public boolean isIncluded() {
            return true;
        }
    }

    record ComputedSpec(Expression expression) implements ProjectionSpec {
        @Override
        public boolean isIncluded() {
            return true;
        }
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/stage/ProjectStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a $project stage.
 */
public final class ProjectStage implements Stage {

    private final LinkedHashMap<String, ProjectionSpec> projections;

    public ProjectStage(Map<String, ProjectionSpec> projections) {
        this.projections = new LinkedHashMap<>(projections);
    }

    public Map<String, ProjectionSpec> getProjections() {
        return Map.copyOf(projections);
    }

    @Override
    public String getOperatorName() {
        return "$project";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("SELECT ");

        boolean first = true;
        for (Map.Entry<String, ProjectionSpec> entry : projections.entrySet()) {
            String outputField = entry.getKey();
            ProjectionSpec spec = entry.getValue();

            if (!spec.isIncluded()) {
                continue;
            }

            if (!first) {
                ctx.sql(", ");
            }
            first = false;

            renderProjection(ctx, outputField, spec);
        }
    }

    private void renderProjection(SqlGenerationContext ctx, String outputField, ProjectionSpec spec) {
        switch (spec) {
            case ProjectionSpec.IncludeSpec include -> {
                // Include field with same name
                ctx.visit(FieldPathExpression.of(outputField));
                ctx.sql(" AS ");
                ctx.identifier(outputField);
            }
            case ProjectionSpec.FieldRenameSpec rename -> {
                ctx.visit(rename.source());
                ctx.sql(" AS ");
                ctx.identifier(outputField);
            }
            case ProjectionSpec.ComputedSpec computed -> {
                ctx.visit(computed.expression());
                ctx.sql(" AS ");
                ctx.identifier(outputField);
            }
            default -> throw new IllegalStateException("Unexpected spec: " + spec);
        }
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/ProjectionSpec.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/ProjectStage.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/stage/ProjectStageTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-026: $sort Stage Implementation

**Phase:** 3
**Complexity:** S
**Dependencies:** IMPL-013

### Description
Implement the $sort stage.

### Acceptance Criteria
- [ ] Test: Sort ascending `{field: 1}`
- [ ] Test: Sort descending `{field: -1}`
- [ ] Test: Multi-field sort
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/stage/SortStageTest.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import static org.assertj.core.api.Assertions.*;

class SortStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderAscendingSort() {
        var keys = new LinkedHashMap<String, SortDirection>();
        keys.put("name", SortDirection.ASC);

        var stage = new SortStage(keys);
        stage.render(context);

        assertThat(context.toSql())
            .isEqualTo("ORDER BY JSON_VALUE(data, '$.name') ASC");
    }

    @Test
    void shouldRenderDescendingSort() {
        var keys = new LinkedHashMap<String, SortDirection>();
        keys.put("createdAt", SortDirection.DESC);

        var stage = new SortStage(keys);
        stage.render(context);

        assertThat(context.toSql())
            .isEqualTo("ORDER BY JSON_VALUE(data, '$.createdAt') DESC");
    }

    @Test
    void shouldRenderMultiFieldSort() {
        var keys = new LinkedHashMap<String, SortDirection>();
        keys.put("category", SortDirection.ASC);
        keys.put("price", SortDirection.DESC);

        var stage = new SortStage(keys);
        stage.render(context);

        assertThat(context.toSql())
            .contains("$.category")
            .contains("ASC")
            .contains("$.price")
            .contains("DESC");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/stage/SortDirection.java
package com.oracle.mongodb.translator.ast.stage;

/**
 * Sort direction.
 */
public enum SortDirection {
    ASC("ASC"),
    DESC("DESC");

    private final String sql;

    SortDirection(String sql) {
        this.sql = sql;
    }

    public String getSql() {
        return sql;
    }

    public static SortDirection fromMongo(int value) {
        return value >= 0 ? ASC : DESC;
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/stage/SortStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a $sort stage.
 */
public final class SortStage implements Stage {

    private final LinkedHashMap<String, SortDirection> keys;

    public SortStage(Map<String, SortDirection> keys) {
        this.keys = new LinkedHashMap<>(keys);
    }

    public Map<String, SortDirection> getKeys() {
        return Map.copyOf(keys);
    }

    @Override
    public String getOperatorName() {
        return "$sort";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("ORDER BY ");

        boolean first = true;
        for (Map.Entry<String, SortDirection> entry : keys.entrySet()) {
            if (!first) {
                ctx.sql(", ");
            }
            first = false;

            ctx.visit(FieldPathExpression.of(entry.getKey()));
            ctx.sql(" ");
            ctx.sql(entry.getValue().getSql());
        }
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/SortDirection.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/SortStage.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/stage/SortStageTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-027: Arithmetic Expression Implementation

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-013

### Description
Implement ArithmeticExpression for $add, $subtract, $multiply, $divide.

### Acceptance Criteria
- [ ] Test: $add renders to `+`
- [ ] Test: $subtract renders to `-`
- [ ] Test: $multiply renders to `*`
- [ ] Test: $divide renders to `/`
- [ ] Test: Nested arithmetic works
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/ArithmeticExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.*;

class ArithmeticExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @ParameterizedTest
    @CsvSource({
        "ADD, +",
        "SUBTRACT, -",
        "MULTIPLY, *",
        "DIVIDE, /"
    })
    void shouldRenderArithmeticOperator(ArithmeticOp op, String expectedSql) {
        var expr = new ArithmeticExpression(
            op,
            FieldPathExpression.of("price", JsonReturnType.NUMBER),
            FieldPathExpression.of("quantity", JsonReturnType.NUMBER)
        );

        expr.render(context);

        assertThat(context.toSql()).contains(expectedSql);
    }

    @Test
    void shouldRenderAddition() {
        var expr = new ArithmeticExpression(
            ArithmeticOp.ADD,
            FieldPathExpression.of("subtotal", JsonReturnType.NUMBER),
            FieldPathExpression.of("tax", JsonReturnType.NUMBER)
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(
            "(JSON_VALUE(data, '$.subtotal' RETURNING NUMBER) + JSON_VALUE(data, '$.tax' RETURNING NUMBER))");
    }

    @Test
    void shouldRenderNestedArithmetic() {
        // (price * quantity) + tax
        var multiply = new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            FieldPathExpression.of("price", JsonReturnType.NUMBER),
            FieldPathExpression.of("quantity", JsonReturnType.NUMBER)
        );
        var add = new ArithmeticExpression(
            ArithmeticOp.ADD,
            multiply,
            FieldPathExpression.of("tax", JsonReturnType.NUMBER)
        );

        add.render(context);

        assertThat(context.toSql())
            .contains("*")
            .contains("+")
            .contains("$.price")
            .contains("$.quantity")
            .contains("$.tax");
    }

    @Test
    void shouldRenderWithLiteral() {
        var expr = new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            FieldPathExpression.of("price", JsonReturnType.NUMBER),
            LiteralExpression.of(1.1)  // 10% markup
        );

        expr.render(context);

        assertThat(context.toSql()).contains("*");
        assertThat(context.getBindVariables()).contains(1.1);
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArithmeticOp.java
package com.oracle.mongodb.translator.ast.expression;

/**
 * Arithmetic operators.
 */
public enum ArithmeticOp {
    ADD("+", "$add"),
    SUBTRACT("-", "$subtract"),
    MULTIPLY("*", "$multiply"),
    DIVIDE("/", "$divide"),
    MOD("MOD", "$mod"),
    POW("POWER", "$pow"),
    SQRT("SQRT", "$sqrt"),
    ABS("ABS", "$abs"),
    CEIL("CEIL", "$ceil"),
    FLOOR("FLOOR", "$floor"),
    ROUND("ROUND", "$round"),
    TRUNC("TRUNC", "$trunc");

    private final String sqlOperator;
    private final String mongoOperator;

    ArithmeticOp(String sqlOperator, String mongoOperator) {
        this.sqlOperator = sqlOperator;
        this.mongoOperator = mongoOperator;
    }

    public String getSqlOperator() {
        return sqlOperator;
    }

    public String getMongoOperator() {
        return mongoOperator;
    }

    public boolean isBinary() {
        return this == ADD || this == SUBTRACT || this == MULTIPLY ||
               this == DIVIDE || this == MOD || this == POW;
    }

    public static ArithmeticOp fromMongo(String mongoOp) {
        for (ArithmeticOp op : values()) {
            if (op.mongoOperator.equals(mongoOp)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown arithmetic operator: " + mongoOp);
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArithmeticExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents an arithmetic expression ($add, $multiply, etc.).
 */
public final class ArithmeticExpression implements Expression {

    private final ArithmeticOp op;
    private final Expression left;
    private final Expression right; // null for unary operations

    public ArithmeticExpression(ArithmeticOp op, Expression left, Expression right) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.left = Objects.requireNonNull(left, "left must not be null");
        this.right = right;

        if (op.isBinary() && right == null) {
            throw new IllegalArgumentException(op + " requires two operands");
        }
    }

    public ArithmeticOp getOp() {
        return op;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        if (op.isBinary()) {
            renderBinary(ctx);
        } else {
            renderUnary(ctx);
        }
    }

    private void renderBinary(SqlGenerationContext ctx) {
        ctx.sql("(");
        ctx.visit(left);
        ctx.sql(" ");
        ctx.sql(op.getSqlOperator());
        ctx.sql(" ");
        ctx.visit(right);
        ctx.sql(")");
    }

    private void renderUnary(SqlGenerationContext ctx) {
        ctx.sql(op.getSqlOperator());
        ctx.sql("(");
        ctx.visit(left);
        ctx.sql(")");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ArithmeticExpression that = (ArithmeticExpression) o;
        return op == that.op && Objects.equals(left, that.left) && Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, left, right);
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArithmeticOp.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArithmeticExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/ArithmeticExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-028: Conditional Expression Implementation

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-018

### Description
Implement ConditionalExpression for $cond and $ifNull.

### Acceptance Criteria
- [ ] Test: $cond renders to CASE WHEN
- [ ] Test: $ifNull renders to NVL/COALESCE
- [ ] Test: Nested conditionals work
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/ConditionalExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ConditionalExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderCondExpression() {
        var expr = new CondExpression(
            new ComparisonExpression(ComparisonOp.GT,
                FieldPathExpression.of("amount", JsonReturnType.NUMBER),
                LiteralExpression.of(100)),
            LiteralExpression.of("high"),
            LiteralExpression.of("low")
        );

        expr.render(context);

        assertThat(context.toSql())
            .contains("CASE WHEN")
            .contains("THEN")
            .contains("ELSE")
            .contains("END");
    }

    @Test
    void shouldRenderIfNullExpression() {
        var expr = new IfNullExpression(
            FieldPathExpression.of("nickname"),
            FieldPathExpression.of("name")
        );

        expr.render(context);

        assertThat(context.toSql())
            .contains("COALESCE(")
            .contains("$.nickname")
            .contains("$.name");
    }

    @Test
    void shouldRenderNestedCond() {
        // if amount > 1000 then 'premium' else if amount > 100 then 'standard' else 'basic'
        var innerCond = new CondExpression(
            new ComparisonExpression(ComparisonOp.GT,
                FieldPathExpression.of("amount", JsonReturnType.NUMBER),
                LiteralExpression.of(100)),
            LiteralExpression.of("standard"),
            LiteralExpression.of("basic")
        );

        var outerCond = new CondExpression(
            new ComparisonExpression(ComparisonOp.GT,
                FieldPathExpression.of("amount", JsonReturnType.NUMBER),
                LiteralExpression.of(1000)),
            LiteralExpression.of("premium"),
            innerCond
        );

        outerCond.render(context);

        assertThat(context.toSql())
            .contains("CASE WHEN")
            .contains("premium")
            .contains("standard")
            .contains("basic");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/expression/CondExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $cond expression (if-then-else).
 */
public final class CondExpression implements Expression, ConditionalExpression {

    private final Expression condition;
    private final Expression thenBranch;
    private final Expression elseBranch;

    public CondExpression(Expression condition, Expression thenBranch, Expression elseBranch) {
        this.condition = Objects.requireNonNull(condition);
        this.thenBranch = Objects.requireNonNull(thenBranch);
        this.elseBranch = Objects.requireNonNull(elseBranch);
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("CASE WHEN ");
        ctx.visit(condition);
        ctx.sql(" THEN ");
        ctx.visit(thenBranch);
        ctx.sql(" ELSE ");
        ctx.visit(elseBranch);
        ctx.sql(" END");
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/IfNullExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents an $ifNull expression.
 */
public final class IfNullExpression implements Expression, ConditionalExpression {

    private final Expression expression;
    private final Expression replacement;

    public IfNullExpression(Expression expression, Expression replacement) {
        this.expression = Objects.requireNonNull(expression);
        this.replacement = Objects.requireNonNull(replacement);
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("COALESCE(");
        ctx.visit(expression);
        ctx.sql(", ");
        ctx.visit(replacement);
        ctx.sql(")");
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/ConditionalExpression.java
package com.oracle.mongodb.translator.ast.expression;

/**
 * Marker interface for conditional expressions.
 * Implementations: CondExpression, IfNullExpression, SwitchExpression
 */
public sealed interface ConditionalExpression extends Expression
    permits CondExpression, IfNullExpression {
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/CondExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/IfNullExpression.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ConditionalExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/ConditionalExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-029: Stage Parsers for Remaining Tier 1 Stages

**Phase:** 3
**Complexity:** M
**Dependencies:** IMPL-025, IMPL-026, IMPL-027, IMPL-028

### Description
Add parsers for $project, $sort, and remaining stage types to complete Tier 1.

### Acceptance Criteria
- [ ] Test: Parse $project stage
- [ ] Test: Parse $sort stage
- [ ] Test: Full Tier 1 pipeline parses correctly
- [ ] Code coverage >= 80%

### Test-First Implementation

```java
// core/src/test/java/com/oracle/mongodb/translator/parser/Tier1PipelineIntegrationTest.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class Tier1PipelineIntegrationTest {

    private AggregationTranslator translator;

    @BeforeEach
    void setUp() {
        translator = AggregationTranslator.create(
            OracleConfiguration.builder()
                .collectionName("orders")
                .build()
        );
    }

    @Test
    void shouldTranslateCompleteMatchGroupSortPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"completed\"}}"),
            Document.parse("{\"$group\": {\"_id\": \"$customerId\", \"total\": {\"$sum\": \"$amount\"}}}"),
            Document.parse("{\"$sort\": {\"total\": -1}}"),
            Document.parse("{\"$limit\": 10}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("WHERE")
            .contains("GROUP BY")
            .contains("ORDER BY")
            .contains("FETCH FIRST 10 ROWS ONLY");
    }

    @Test
    void shouldTranslateProjectPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$project\": {\"name\": 1, \"email\": 1, \"_id\": 0}}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("$.name")
            .contains("$.email");
    }

    @Test
    void shouldTranslateMatchWithMultipleConditions() {
        var pipeline = List.of(
            Document.parse("{\"$match\": {\"$and\": [{\"status\": \"active\"}, {\"age\": {\"$gte\": 18}}]}}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("AND")
            .contains("$.status")
            .contains("$.age");
    }
}
```

### Files
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/parser/StageParserRegistry.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/ProjectStageParser.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/SortStageParser.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/parser/Tier1PipelineIntegrationTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] All Tier 1 operators work
- [ ] Code coverage >= 80%

---

## IMPL-030: Pipeline Rendering Refactor

**Phase:** 3
**Complexity:** L
**Dependencies:** IMPL-029

### Description
Refactor Pipeline class to properly combine stages into coherent SQL.

### Acceptance Criteria
- [ ] Test: Pipeline renders valid SQL for all stage combinations
- [ ] Test: FROM clause appears once
- [ ] Test: Stage clauses in correct order
- [ ] Code coverage >= 80%

### Implementation Notes

The Pipeline rendering must handle the order and combination of:
1. SELECT (from $project or $group)
2. FROM (always collection name)
3. WHERE (from $match)
4. GROUP BY (from $group)
5. ORDER BY (from $sort)
6. OFFSET / FETCH FIRST (from $skip / $limit)

### Files
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/Pipeline.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/generator/PipelineRendererTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Valid SQL generated for all combinations
- [ ] Code coverage >= 80%

---
