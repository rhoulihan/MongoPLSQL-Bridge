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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
    void shouldHandleNullEqualityComparison() {
        var expr = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("deletedAt"),
            LiteralExpression.ofNull()
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.deletedAt') IS NULL");
    }

    @Test
    void shouldHandleNullInequalityComparison() {
        var expr = new ComparisonExpression(
            ComparisonOp.NE,
            FieldPathExpression.of("deletedAt"),
            LiteralExpression.ofNull()
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.deletedAt') IS NOT NULL");
    }

    @Test
    void shouldReturnOperator() {
        var expr = new ComparisonExpression(
            ComparisonOp.GT,
            FieldPathExpression.of("amount"),
            LiteralExpression.of(100)
        );

        assertThat(expr.getOp()).isEqualTo(ComparisonOp.GT);
    }

    @Test
    void shouldReturnLeftExpression() {
        var left = FieldPathExpression.of("status");
        var expr = new ComparisonExpression(
            ComparisonOp.EQ,
            left,
            LiteralExpression.of("active")
        );

        assertThat(expr.getLeft()).isEqualTo(left);
    }

    @Test
    void shouldReturnRightExpression() {
        var right = LiteralExpression.of("active");
        var expr = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            right
        );

        assertThat(expr.getRight()).isEqualTo(right);
    }

    @Test
    void shouldRenderLessThanOrEqual() {
        var expr = new ComparisonExpression(
            ComparisonOp.LTE,
            FieldPathExpression.of("price", JsonReturnType.NUMBER),
            LiteralExpression.of(99.99)
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.price' RETURNING NUMBER) <= :1");
    }

    @Test
    void shouldProvideReadableToString() {
        var expr = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );

        assertThat(expr.toString()).contains("EQ");
    }
}
