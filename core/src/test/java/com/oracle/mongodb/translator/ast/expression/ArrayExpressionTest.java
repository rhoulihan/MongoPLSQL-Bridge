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

class ArrayExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderArrayElemAtWithLiteralIndex() {
        // MongoDB: {$arrayElemAt: ["$items", 0]} - 0-based
        // Oracle: JSON_VALUE(data, '$.items[0]')
        var expr = ArrayExpression.arrayElemAt(
            FieldPathExpression.of("items"),
            LiteralExpression.of(0)
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.items[0]')");
    }

    @Test
    void shouldRenderArrayElemAtWithNonZeroIndex() {
        // MongoDB: {$arrayElemAt: ["$tags", 2]}
        // Oracle: JSON_VALUE(data, '$.tags[2]')
        var expr = ArrayExpression.arrayElemAt(
            FieldPathExpression.of("tags"),
            LiteralExpression.of(2)
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.tags[2]')");
    }

    @Test
    void shouldRenderSize() {
        // MongoDB: {$size: "$items"}
        // Oracle: JSON_QUERY(data, '$.items.size()')
        var expr = ArrayExpression.size(FieldPathExpression.of("items"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.items.size()')");
    }

    @Test
    void shouldRenderFirst() {
        // MongoDB: {$first: "$items"}
        // Oracle: JSON_VALUE(data, '$.items[0]')
        var expr = ArrayExpression.first(FieldPathExpression.of("items"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.items[0]')");
    }

    @Test
    void shouldRenderLast() {
        // MongoDB: {$last: "$items"}
        // Oracle: JSON_VALUE(data, '$.items[last]')
        var expr = ArrayExpression.last(FieldPathExpression.of("items"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.items[last]')");
    }

    @Test
    void shouldReturnOp() {
        var expr = ArrayExpression.size(FieldPathExpression.of("x"));
        assertThat(expr.getOp()).isEqualTo(ArrayOp.SIZE);
    }

    @Test
    void shouldReturnArrayExpression() {
        var field = FieldPathExpression.of("x");
        var expr = ArrayExpression.size(field);
        assertThat(expr.getArrayExpression()).isEqualTo(field);
    }

    @Test
    void shouldProvideReadableToString() {
        var expr = ArrayExpression.size(FieldPathExpression.of("items"));
        assertThat(expr.toString()).contains("$size");
    }

    @Test
    void shouldRenderNestedArrayAccess() {
        // Access nested array: {$arrayElemAt: ["$orders.items", 0]}
        var expr = ArrayExpression.arrayElemAt(
            FieldPathExpression.of("orders.items"),
            LiteralExpression.of(0)
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.orders.items[0]')");
    }
}
