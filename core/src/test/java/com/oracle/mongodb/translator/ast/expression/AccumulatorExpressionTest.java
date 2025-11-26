/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AccumulatorExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderSum() {
        var expr = AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER))");
    }

    @Test
    void shouldRenderAvg() {
        var expr = AccumulatorExpression.avg(FieldPathExpression.of("price", JsonReturnType.NUMBER));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("AVG(JSON_VALUE(data, '$.price' RETURNING NUMBER))");
    }

    @Test
    void shouldRenderCount() {
        var expr = AccumulatorExpression.count();

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("COUNT(*)");
    }

    @Test
    void shouldRenderMin() {
        var expr = AccumulatorExpression.min(FieldPathExpression.of("score", JsonReturnType.NUMBER));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("MIN(JSON_VALUE(data, '$.score' RETURNING NUMBER))");
    }

    @Test
    void shouldRenderMax() {
        var expr = AccumulatorExpression.max(FieldPathExpression.of("score", JsonReturnType.NUMBER));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("MAX(JSON_VALUE(data, '$.score' RETURNING NUMBER))");
    }

    @Test
    void shouldRenderFirst() {
        var expr = AccumulatorExpression.first(FieldPathExpression.of("name"));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("FIRST_VALUE(JSON_VALUE(data, '$.name'))");
    }

    @Test
    void shouldRenderLast() {
        var expr = AccumulatorExpression.last(FieldPathExpression.of("name"));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("LAST_VALUE(JSON_VALUE(data, '$.name'))");
    }

    @Test
    void shouldRenderSumWithLiteral() {
        // {$sum: 1} counts documents
        var expr = AccumulatorExpression.sum(LiteralExpression.of(1));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("SUM(:1)");
        assertThat(context.getBindVariables()).containsExactly(1);
    }

    @Test
    void shouldReturnOp() {
        var expr = AccumulatorExpression.sum(FieldPathExpression.of("amount"));
        assertThat(expr.getOp()).isEqualTo(AccumulatorOp.SUM);
    }

    @Test
    void shouldReturnArgument() {
        var field = FieldPathExpression.of("amount");
        var expr = AccumulatorExpression.sum(field);
        assertThat(expr.getArgument()).isEqualTo(field);
    }

    @Test
    void shouldReturnNullArgumentForCount() {
        var expr = AccumulatorExpression.count();
        assertThat(expr.getArgument()).isNull();
    }

    @Test
    void shouldProvideReadableToStringForCount() {
        var expr = AccumulatorExpression.count();
        assertThat(expr.toString()).contains("COUNT(*)");
    }

    @Test
    void shouldProvideReadableToStringForSum() {
        var expr = AccumulatorExpression.sum(FieldPathExpression.of("amount"));
        assertThat(expr.toString()).contains("$sum");
    }
}
