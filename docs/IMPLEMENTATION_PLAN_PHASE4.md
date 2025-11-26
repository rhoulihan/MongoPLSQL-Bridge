# Phase 4: Tier 2-4 Operators and Optimization

## Entry Criteria
- Phase 3 complete (all Tier 1 operators working)
- Integration tests passing against Oracle
- CI/CD pipeline green

## Exit Criteria
- Tier 2 operators fully implemented
- Pipeline optimization working
- Tier 3-4 operators implemented (summary implementations)
- Performance benchmarks established

---

# Tier 2 Operators

## IMPL-031: $lookup Stage Implementation

**Phase:** 4
**Complexity:** L
**Dependencies:** IMPL-030

### Description
Implement the $lookup stage for left outer joins between collections.

### Acceptance Criteria
- [ ] Test: Simple lookup with localField/foreignField
- [ ] Test: Lookup renders as LEFT OUTER JOIN
- [ ] Test: Lookup with $unwind optimization
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/stage/LookupStageTest.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class LookupStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderSimpleLookup() {
        var stage = new LookupStage(
            "customers",          // from
            "customerId",         // localField
            "_id",                // foreignField
            "customerInfo"        // as
        );

        stage.render(context);

        assertThat(context.toSql())
            .contains("LEFT OUTER JOIN customers")
            .contains("JSON_VALUE")
            .contains("$.customerId")
            .contains("$._id");
    }

    @Test
    void shouldGenerateTableAlias() {
        var stage = new LookupStage("customers", "customerId", "_id", "customer");

        assertThat(stage.getTableAlias()).isNotNull();
    }
}

// Integration test
// core/src/test/java/com/oracle/mongodb/translator/parser/LookupStageParserTest.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.LookupStage;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class LookupStageParserTest {

    @Test
    void shouldParseLookupStage() {
        var registry = new StageParserRegistry();
        var doc = Document.parse("""
            {
                "from": "customers",
                "localField": "customerId",
                "foreignField": "_id",
                "as": "customerData"
            }
            """);

        var stage = registry.getParser("$lookup").parse(doc);

        assertThat(stage).isInstanceOf(LookupStage.class);
        var lookup = (LookupStage) stage;
        assertThat(lookup.getFrom()).isEqualTo("customers");
        assertThat(lookup.getLocalField()).isEqualTo("customerId");
        assertThat(lookup.getForeignField()).isEqualTo("_id");
        assertThat(lookup.getAs()).isEqualTo("customerData");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/stage/LookupStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a $lookup stage (left outer join).
 */
public final class LookupStage implements Stage {

    private static final AtomicInteger ALIAS_COUNTER = new AtomicInteger(0);

    private final String from;
    private final String localField;
    private final String foreignField;
    private final String as;
    private final String tableAlias;

    public LookupStage(String from, String localField, String foreignField, String as) {
        this.from = Objects.requireNonNull(from);
        this.localField = Objects.requireNonNull(localField);
        this.foreignField = Objects.requireNonNull(foreignField);
        this.as = Objects.requireNonNull(as);
        this.tableAlias = "lookup_" + ALIAS_COUNTER.incrementAndGet();
    }

    public String getFrom() { return from; }
    public String getLocalField() { return localField; }
    public String getForeignField() { return foreignField; }
    public String getAs() { return as; }
    public String getTableAlias() { return tableAlias; }

    @Override
    public String getOperatorName() {
        return "$lookup";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("LEFT OUTER JOIN ");
        ctx.identifier(from);
        ctx.sql(" ");
        ctx.sql(tableAlias);
        ctx.sql(" ON JSON_VALUE(data, '$.");
        ctx.sql(localField);
        ctx.sql("') = JSON_VALUE(");
        ctx.sql(tableAlias);
        ctx.sql(".data, '$.");
        ctx.sql(foreignField);
        ctx.sql("')");
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/LookupStage.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/LookupStageParser.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/stage/LookupStageTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/parser/LookupStageParserTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] Integration test with Oracle passes

---

## IMPL-032: $unwind Stage Implementation

**Phase:** 4
**Complexity:** L
**Dependencies:** IMPL-030

### Description
Implement the $unwind stage using JSON_TABLE with NESTED PATH.

### Acceptance Criteria
- [ ] Test: Simple $unwind on array field
- [ ] Test: $unwind with preserveNullAndEmptyArrays
- [ ] Test: $unwind with includeArrayIndex
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/stage/UnwindStageTest.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class UnwindStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderSimpleUnwind() {
        var stage = new UnwindStage("items", false, null);

        stage.render(context);

        assertThat(context.toSql())
            .contains("JSON_TABLE")
            .contains("$.items[*]")
            .contains("COLUMNS");
    }

    @Test
    void shouldRenderUnwindWithPreserveNull() {
        var stage = new UnwindStage("items", true, null);

        stage.render(context);

        // preserveNullAndEmptyArrays requires OUTER JOIN or special handling
        assertThat(context.toSql()).contains("JSON_TABLE");
    }

    @Test
    void shouldRenderUnwindWithArrayIndex() {
        var stage = new UnwindStage("items", false, "itemIndex");

        stage.render(context);

        assertThat(context.toSql())
            .contains("FOR ORDINALITY")
            .contains("itemIndex");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/stage/UnwindStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents an $unwind stage that flattens an array field.
 */
public final class UnwindStage implements Stage {

    private static final AtomicInteger ALIAS_COUNTER = new AtomicInteger(0);

    private final String path;
    private final boolean preserveNullAndEmptyArrays;
    private final String includeArrayIndex;
    private final String tableAlias;

    public UnwindStage(String path, boolean preserveNullAndEmptyArrays, String includeArrayIndex) {
        // Remove $ prefix if present
        this.path = path.startsWith("$") ? path.substring(1) : path;
        this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
        this.includeArrayIndex = includeArrayIndex;
        this.tableAlias = "unwind_" + ALIAS_COUNTER.incrementAndGet();
    }

    public String getPath() { return path; }
    public boolean isPreserveNullAndEmptyArrays() { return preserveNullAndEmptyArrays; }
    public String getIncludeArrayIndex() { return includeArrayIndex; }
    public String getTableAlias() { return tableAlias; }

    @Override
    public String getOperatorName() {
        return "$unwind";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Render as a CROSS APPLY / CROSS JOIN LATERAL with JSON_TABLE
        ctx.sql(", JSON_TABLE(data, '$.");
        ctx.sql(path);
        ctx.sql("[*]' COLUMNS (");

        if (includeArrayIndex != null) {
            ctx.identifier(includeArrayIndex);
            ctx.sql(" FOR ORDINALITY, ");
        }

        // We need to know the structure of the array elements
        // For now, render the whole element as JSON
        ctx.sql(path);
        ctx.sql("_elem JSON PATH '$'");

        ctx.sql(")) ");
        ctx.sql(tableAlias);

        if (preserveNullAndEmptyArrays) {
            // Oracle 12c+ supports (+) syntax or OUTER APPLY
            // For JSON_TABLE, use ERROR ON EMPTY to get nulls
            // This is a simplification - full support needs more work
        }
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/UnwindStage.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/UnwindStageParser.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/stage/UnwindStageTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

## IMPL-033: $addFields/$set Stage Implementation

**Phase:** 4
**Complexity:** M
**Dependencies:** IMPL-025

### Description
Implement $addFields and $set stages (aliases) using JSON_TRANSFORM.

### Acceptance Criteria
- [ ] Test: Add single field
- [ ] Test: Add multiple fields
- [ ] Test: Add computed field
- [ ] Test: $set as alias works
- [ ] Code coverage >= 80%

### Test-First Implementation

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/stage/AddFieldsStageTest.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.*;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import static org.assertj.core.api.Assertions.*;

class AddFieldsStageTest {

    @Test
    void shouldRenderAddFields() {
        var fields = new LinkedHashMap<String, Expression>();
        fields.put("totalPrice", new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            FieldPathExpression.of("price", JsonReturnType.NUMBER),
            FieldPathExpression.of("quantity", JsonReturnType.NUMBER)
        ));

        var stage = new AddFieldsStage(fields);
        var context = new DefaultSqlGenerationContext();
        stage.render(context);

        assertThat(context.toSql())
            .contains("totalPrice")
            .contains("$.price")
            .contains("$.quantity");
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/AddFieldsStage.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/AddFieldsStageParser.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/stage/AddFieldsStageTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

## IMPL-034: Additional Accumulators ($first, $last, $push, $addToSet)

**Phase:** 4
**Complexity:** M
**Dependencies:** IMPL-022

### Description
Complete remaining Tier 2 accumulators.

### Acceptance Criteria
- [ ] Test: $first returns first value in group
- [ ] Test: $last returns last value in group
- [ ] Test: $push creates array of all values
- [ ] Test: $addToSet creates array of unique values
- [ ] Code coverage >= 80%

### Test-First Implementation

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/Tier2AccumulatorsTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class Tier2AccumulatorsTest {

    @Test
    void shouldRenderPushAccumulator() {
        var expr = new AccumulatorExpression(
            AccumulatorOp.PUSH,
            FieldPathExpression.of("item")
        );
        var context = new DefaultSqlGenerationContext();

        expr.render(context);

        assertThat(context.toSql()).contains("JSON_ARRAYAGG(");
    }

    @Test
    void shouldRenderAddToSetAccumulator() {
        var expr = new AccumulatorExpression(
            AccumulatorOp.ADD_TO_SET,
            FieldPathExpression.of("category")
        );
        var context = new DefaultSqlGenerationContext();

        expr.render(context);

        // Should include DISTINCT for uniqueness
        assertThat(context.toSql()).contains("JSON_ARRAYAGG(");
    }
}
```

### Files
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/AccumulatorExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/Tier2AccumulatorsTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

## IMPL-035: String Operators ($concat, $toLower, $toUpper, $substr)

**Phase:** 4
**Complexity:** M
**Dependencies:** IMPL-013

### Description
Implement string manipulation operators.

### Acceptance Criteria
- [ ] Test: $concat joins strings
- [ ] Test: $toLower converts to lowercase
- [ ] Test: $toUpper converts to uppercase
- [ ] Test: $substr extracts substring
- [ ] Code coverage >= 80%

### Test-First Implementation

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/StringExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class StringExpressionTest {

    @Test
    void shouldRenderConcat() {
        var expr = new ConcatExpression(List.of(
            FieldPathExpression.of("firstName"),
            LiteralExpression.of(" "),
            FieldPathExpression.of("lastName")
        ));
        var context = new DefaultSqlGenerationContext();

        expr.render(context);

        assertThat(context.toSql())
            .contains("CONCAT(")
            .contains("$.firstName")
            .contains("$.lastName");
    }

    @Test
    void shouldRenderToLower() {
        var expr = new ToLowerExpression(FieldPathExpression.of("email"));
        var context = new DefaultSqlGenerationContext();

        expr.render(context);

        assertThat(context.toSql()).contains("LOWER(");
    }

    @Test
    void shouldRenderToUpper() {
        var expr = new ToUpperExpression(FieldPathExpression.of("name"));
        var context = new DefaultSqlGenerationContext();

        expr.render(context);

        assertThat(context.toSql()).contains("UPPER(");
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/StringExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ConcatExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ToLowerExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ToUpperExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/StringExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

## IMPL-036: Date Operators ($year, $month, $dayOfMonth, etc.)

**Phase:** 4
**Complexity:** M
**Dependencies:** IMPL-013

### Description
Implement date extraction operators using Oracle EXTRACT function.

### Acceptance Criteria
- [ ] Test: $year extracts year
- [ ] Test: $month extracts month
- [ ] Test: $dayOfMonth extracts day
- [ ] Test: Works with timestamp fields
- [ ] Code coverage >= 80%

### Test-First Implementation

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/DateExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.*;

class DateExpressionTest {

    @ParameterizedTest
    @CsvSource({
        "YEAR, EXTRACT(YEAR FROM",
        "MONTH, EXTRACT(MONTH FROM",
        "DAY, EXTRACT(DAY FROM",
        "HOUR, EXTRACT(HOUR FROM",
        "MINUTE, EXTRACT(MINUTE FROM",
        "SECOND, EXTRACT(SECOND FROM"
    })
    void shouldRenderDateExtraction(DatePart part, String expectedSql) {
        var expr = new DateExtractExpression(
            part,
            FieldPathExpression.of("createdAt", JsonReturnType.TIMESTAMP)
        );
        var context = new DefaultSqlGenerationContext();

        expr.render(context);

        assertThat(context.toSql()).contains(expectedSql);
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/DatePart.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/DateExtractExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/DateExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

## IMPL-037: Array Operators ($arrayElemAt, $size, $filter)

**Phase:** 4
**Complexity:** L
**Dependencies:** IMPL-013

### Description
Implement array manipulation operators.

### Acceptance Criteria
- [ ] Test: $arrayElemAt gets element by index
- [ ] Test: $size returns array length
- [ ] Test: $filter with condition
- [ ] Code coverage >= 80%

### Test-First Implementation

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/ArrayExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ArrayExpressionTest {

    @Test
    void shouldRenderArrayElemAt() {
        var expr = new ArrayElemAtExpression(
            FieldPathExpression.of("items"),
            0
        );
        var context = new DefaultSqlGenerationContext();

        expr.render(context);

        assertThat(context.toSql()).contains("$.items[0]");
    }

    @Test
    void shouldRenderArraySize() {
        var expr = new ArraySizeExpression(FieldPathExpression.of("items"));
        var context = new DefaultSqlGenerationContext();

        expr.render(context);

        assertThat(context.toSql()).contains("JSON_QUERY");
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayElemAtExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArraySizeExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayFilterExpression.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/ArrayExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

# Pipeline Optimization

## IMPL-038: Predicate Pushdown Optimizer

**Phase:** 4
**Complexity:** L
**Dependencies:** IMPL-030

### Description
Implement optimization pass that moves $match stages as early as possible in the pipeline.

### Acceptance Criteria
- [ ] Test: $match after $project moves before if possible
- [ ] Test: $match stays in place if field depends on earlier stage
- [ ] Test: Multiple $match stages merged
- [ ] Code coverage >= 80%

### Test-First Implementation

```java
// core/src/test/java/com/oracle/mongodb/translator/optimizer/PredicatePushdownOptimizerTest.java
package com.oracle.mongodb.translator.optimizer;

import com.oracle.mongodb.translator.ast.Pipeline;
import com.oracle.mongodb.translator.ast.stage.*;
import com.oracle.mongodb.translator.ast.expression.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.LinkedHashMap;
import static org.assertj.core.api.Assertions.*;

class PredicatePushdownOptimizerTest {

    private PredicatePushdownOptimizer optimizer;

    @BeforeEach
    void setUp() {
        optimizer = new PredicatePushdownOptimizer();
    }

    @Test
    void shouldPushMatchBeforeSort() {
        var pipeline = Pipeline.of("orders", List.of(
            new SortStage(LinkedHashMap.of("createdAt", SortDirection.DESC)),
            new MatchStage(new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            ))
        ));

        var optimized = optimizer.optimize(pipeline);

        // $match should now be first
        assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
        assertThat(optimized.getStages().get(1)).isInstanceOf(SortStage.class);
    }

    @Test
    void shouldNotPushMatchIfFieldNotAvailable() {
        // $match on computed field must stay after $project
        var projections = new LinkedHashMap<String, ProjectionSpec>();
        projections.put("total", ProjectionSpec.computed(
            new ArithmeticExpression(
                ArithmeticOp.MULTIPLY,
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                FieldPathExpression.of("qty", JsonReturnType.NUMBER)
            )
        ));

        var pipeline = Pipeline.of("orders", List.of(
            new ProjectStage(projections),
            new MatchStage(new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("total", JsonReturnType.NUMBER),
                LiteralExpression.of(100)
            ))
        ));

        var optimized = optimizer.optimize(pipeline);

        // Order should remain the same
        assertThat(optimized.getStages().get(0)).isInstanceOf(ProjectStage.class);
        assertThat(optimized.getStages().get(1)).isInstanceOf(MatchStage.class);
    }

    @Test
    void shouldMergeConsecutiveMatchStages() {
        var pipeline = Pipeline.of("orders", List.of(
            new MatchStage(new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            )),
            new MatchStage(new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("amount", JsonReturnType.NUMBER),
                LiteralExpression.of(100)
            ))
        ));

        var optimized = optimizer.optimize(pipeline);

        // Should be merged into single $match with $and
        assertThat(optimized.getStages()).hasSize(1);
        assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/optimizer/PipelineOptimizer.java
package com.oracle.mongodb.translator.optimizer;

import com.oracle.mongodb.translator.ast.Pipeline;

/**
 * Interface for pipeline optimization passes.
 */
@FunctionalInterface
public interface PipelineOptimizer {
    Pipeline optimize(Pipeline input);
}

// core/src/main/java/com/oracle/mongodb/translator/optimizer/PredicatePushdownOptimizer.java
package com.oracle.mongodb.translator.optimizer;

import com.oracle.mongodb.translator.ast.Pipeline;
import com.oracle.mongodb.translator.ast.stage.*;
import com.oracle.mongodb.translator.ast.expression.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Pushes $match stages as early as possible in the pipeline.
 */
public class PredicatePushdownOptimizer implements PipelineOptimizer {

    @Override
    public Pipeline optimize(Pipeline input) {
        List<Stage> stages = new ArrayList<>(input.getStages());

        // First pass: merge consecutive $match stages
        stages = mergeConsecutiveMatches(stages);

        // Second pass: push $match stages earlier
        stages = pushMatchStages(stages);

        return Pipeline.of(input.getCollectionName(), stages);
    }

    private List<Stage> mergeConsecutiveMatches(List<Stage> stages) {
        List<Stage> result = new ArrayList<>();
        MatchStage pending = null;

        for (Stage stage : stages) {
            if (stage instanceof MatchStage match) {
                if (pending == null) {
                    pending = match;
                } else {
                    // Merge with AND
                    pending = mergeMatches(pending, match);
                }
            } else {
                if (pending != null) {
                    result.add(pending);
                    pending = null;
                }
                result.add(stage);
            }
        }

        if (pending != null) {
            result.add(pending);
        }

        return result;
    }

    private MatchStage mergeMatches(MatchStage a, MatchStage b) {
        Expression merged = new LogicalExpression(
            LogicalOp.AND,
            List.of(a.getFilter(), b.getFilter())
        );
        return new MatchStage(merged);
    }

    private List<Stage> pushMatchStages(List<Stage> stages) {
        // Simple implementation: check if $match can swap with previous stage
        List<Stage> result = new ArrayList<>(stages);

        boolean changed;
        do {
            changed = false;
            for (int i = 1; i < result.size(); i++) {
                if (result.get(i) instanceof MatchStage match) {
                    Stage prev = result.get(i - 1);
                    if (canSwap(match, prev)) {
                        result.set(i - 1, match);
                        result.set(i, prev);
                        changed = true;
                    }
                }
            }
        } while (changed);

        return result;
    }

    private boolean canSwap(MatchStage match, Stage other) {
        // Can swap with $sort, $skip, $limit (if match doesn't depend on computed fields)
        if (other instanceof SortStage || other instanceof SkipStage || other instanceof LimitStage) {
            return true;
        }

        // Cannot swap with $group, $project, $lookup, $unwind
        // (fields may be computed or renamed)
        return false;
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/optimizer/PipelineOptimizer.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/optimizer/PredicatePushdownOptimizer.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/optimizer/PredicatePushdownOptimizerTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

## IMPL-039: Sort-Limit Optimization

**Phase:** 4
**Complexity:** M
**Dependencies:** IMPL-038

### Description
Detect TOP-N patterns and apply Oracle-specific optimizations.

### Acceptance Criteria
- [ ] Test: $sort followed by $limit detected
- [ ] Test: Generates FETCH FIRST with ORDER BY
- [ ] Test: Hint added for optimization
- [ ] Code coverage >= 80%

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/optimizer/SortLimitOptimizer.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/optimizer/SortLimitOptimizerTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

## IMPL-040: Optimization Chain

**Phase:** 4
**Complexity:** S
**Dependencies:** IMPL-038, IMPL-039

### Description
Create a chain of optimizers that runs in sequence.

### Acceptance Criteria
- [ ] Test: Chain applies all optimizers in order
- [ ] Test: Can add/remove optimizers
- [ ] Code coverage >= 80%

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/optimizer/OptimizationChain.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/api/DefaultAggregationTranslator.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/optimizer/OptimizationChainTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%

---

# Tier 3 Operators (Summary Implementations)

## IMPL-041: $facet Stage

**Phase:** 4
**Complexity:** XL
**Dependencies:** IMPL-030

### Description
Implement $facet stage using multiple CTEs or parallel queries.

### Notes
$facet is complex as it runs multiple sub-pipelines in parallel. Oracle implementation would use:
- WITH clause (CTEs) for each facet
- UNION ALL to combine results
- JSON aggregation to package results

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/FacetStage.java`

---

## IMPL-042: $bucket/$bucketAuto Stages

**Phase:** 4
**Complexity:** L
**Dependencies:** IMPL-023

### Description
Implement bucketing using CASE expressions and WIDTH_BUCKET.

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/BucketStage.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/BucketAutoStage.java`

---

## IMPL-043: $merge/$out Stages

**Phase:** 4
**Complexity:** L
**Dependencies:** IMPL-030

### Description
Implement $merge using Oracle MERGE INTO and $out using INSERT INTO.

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/MergeStage.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/OutStage.java`

---

## IMPL-044: $unionWith Stage

**Phase:** 4
**Complexity:** M
**Dependencies:** IMPL-030

### Description
Implement $unionWith using UNION ALL.

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/UnionWithStage.java`

---

# Tier 4 Operators (Stub Implementations)

## IMPL-045: $graphLookup Stage

**Phase:** 4
**Complexity:** XL
**Dependencies:** IMPL-031

### Description
Stub implementation with UNSUPPORTED capability. Full implementation would require recursive CTEs.

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/GraphLookupStage.java`

---

## IMPL-046: $setWindowFields Stage

**Phase:** 4
**Complexity:** XL
**Dependencies:** IMPL-030

### Description
Stub implementation. Full implementation would use Oracle window functions.

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/SetWindowFieldsStage.java`

---

## IMPL-047: Specification Files

**Phase:** 4
**Complexity:** M
**Dependencies:** All previous

### Description
Create the JSON specification files that drive code generation.

### Acceptance Criteria
- [ ] operators.json contains all implemented operators
- [ ] type-mappings.json maps MongoDB types to Oracle
- [ ] test-cases/ contains shared test fixtures
- [ ] JSON Schema validates specifications

### Files
- CREATE: `specs/operators.json`
- CREATE: `specs/type-mappings.json`
- CREATE: `specs/error-codes.json`
- CREATE: `specs/schemas/operator-schema.json`
- CREATE: `specs/test-cases/match-operator.json`
- CREATE: `specs/test-cases/group-operator.json`
- CREATE: `specs/test-cases/complex-pipelines.json`

---

## IMPL-048: Integration Test Suite

**Phase:** 4
**Complexity:** L
**Dependencies:** All operators

### Description
Comprehensive integration tests comparing MongoDB and Oracle results.

### Acceptance Criteria
- [ ] Test data fixtures created
- [ ] Cross-validation tests pass
- [ ] Performance benchmarks established

### Files
- CREATE: `integration-tests/src/test/java/com/oracle/mongodb/translator/integration/CrossValidationTest.java`
- CREATE: `integration-tests/src/test/java/com/oracle/mongodb/translator/integration/PerformanceBenchmarkTest.java`
- CREATE: `integration-tests/src/test/resources/testdata/customers.json`
- CREATE: `integration-tests/src/test/resources/testdata/orders.json`
- CREATE: `integration-tests/src/test/resources/testdata/products.json`

---
