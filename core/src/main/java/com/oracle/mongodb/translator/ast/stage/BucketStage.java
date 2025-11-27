/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a $bucket stage that categorizes documents into groups (buckets)
 * based on a specified expression and boundary values.
 *
 * <p>MongoDB syntax:
 * <pre>
 * {
 *   $bucket: {
 *     groupBy: &lt;expression&gt;,
 *     boundaries: [&lt;lowerbound1&gt;, &lt;lowerbound2&gt;, ...],
 *     default: &lt;literal&gt;,
 *     output: {
 *       &lt;field1&gt;: { &lt;accumulator expression&gt; },
 *       ...
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation uses CASE expression:
 * <pre>
 * SELECT
 *   CASE
 *     WHEN expr &gt;= boundary1 AND expr &lt; boundary2 THEN boundary1
 *     WHEN expr &gt;= boundary2 AND expr &lt; boundary3 THEN boundary2
 *     ...
 *     ELSE default
 *   END AS _id,
 *   aggregations...
 * FROM collection
 * GROUP BY (CASE expression)
 * </pre>
 */
public final class BucketStage implements Stage {

    private final Expression groupBy;
    private final List<Object> boundaries;
    private final Object defaultBucket;
    private final Map<String, AccumulatorExpression> output;

    /**
     * Creates a bucket stage.
     *
     * @param groupBy       the expression to group by
     * @param boundaries    the boundary values (at least 2 required)
     * @param defaultBucket optional default bucket for out-of-range values (may be null)
     * @param output        optional output accumulators (may be empty)
     */
    public BucketStage(
            Expression groupBy,
            List<Object> boundaries,
            Object defaultBucket,
            Map<String, AccumulatorExpression> output) {
        this.groupBy = Objects.requireNonNull(groupBy, "groupBy must not be null");
        if (boundaries == null || boundaries.size() < 2) {
            throw new IllegalArgumentException("boundaries must have at least 2 values");
        }
        this.boundaries = List.copyOf(boundaries);
        this.defaultBucket = defaultBucket;
        this.output = output != null ? new LinkedHashMap<>(output) : new LinkedHashMap<>();
    }

    public Expression getGroupBy() {
        return groupBy;
    }

    public List<Object> getBoundaries() {
        return boundaries;
    }

    public Object getDefaultBucket() {
        return defaultBucket;
    }

    public boolean hasDefault() {
        return defaultBucket != null;
    }

    public Map<String, AccumulatorExpression> getOutput() {
        return output;
    }

    @Override
    public String getOperatorName() {
        return "$bucket";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Render CASE expression for bucket assignment
        ctx.sql("CASE");
        for (int i = 0; i < boundaries.size() - 1; i++) {
            Object lower = boundaries.get(i);
            Object upper = boundaries.get(i + 1);
            ctx.sql(" WHEN ");
            ctx.visit(groupBy);
            ctx.sql(" >= ");
            renderLiteral(ctx, lower);
            ctx.sql(" AND ");
            ctx.visit(groupBy);
            ctx.sql(" < ");
            renderLiteral(ctx, upper);
            ctx.sql(" THEN ");
            renderLiteral(ctx, lower);
        }
        if (hasDefault()) {
            ctx.sql(" ELSE ");
            renderLiteral(ctx, defaultBucket);
        }
        ctx.sql(" END AS ");
        ctx.identifier("_id");

        // Render output accumulators
        for (Map.Entry<String, AccumulatorExpression> entry : output.entrySet()) {
            ctx.sql(", ");
            ctx.visit(entry.getValue());
            ctx.sql(" AS ");
            ctx.identifier(entry.getKey());
        }
    }

    private void renderLiteral(SqlGenerationContext ctx, Object value) {
        if (value instanceof String) {
            ctx.sql("'");
            ctx.sql(((String) value).replace("'", "''"));
            ctx.sql("'");
        } else if (value instanceof Number) {
            ctx.sql(value.toString());
        } else if (value == null) {
            ctx.sql("NULL");
        } else {
            ctx.sql("'");
            ctx.sql(value.toString().replace("'", "''"));
            ctx.sql("'");
        }
    }

    @Override
    public String toString() {
        return "BucketStage(groupBy=" + groupBy
            + ", boundaries=" + boundaries
            + ", default=" + defaultBucket
            + ", output=" + output + ")";
    }
}
