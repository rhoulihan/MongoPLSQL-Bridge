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

    // New array operator tests

    @Test
    void shouldRenderConcatArrays() {
        // MongoDB: {$concatArrays: ["$arr1", "$arr2"]}
        var expr = ArrayExpression.concatArrays(
            java.util.List.of(
                FieldPathExpression.of("arr1"),
                FieldPathExpression.of("arr2")
            )
        );

        expr.render(context);

        // Uses JSON_ARRAYAGG with JSON_TABLE to flatten and re-aggregate
        assertThat(context.toSql()).contains("JSON_ARRAYAGG");
        assertThat(context.toSql()).contains("JSON_TABLE");
        assertThat(context.toSql()).contains("arr1");
        assertThat(context.toSql()).contains("arr2");
    }

    @Test
    void shouldRenderSliceWithTwoArgs() {
        // MongoDB: {$slice: ["$items", 3]} - first 3 elements
        var expr = ArrayExpression.slice(
            FieldPathExpression.of("items"),
            LiteralExpression.of(3)
        );

        expr.render(context);

        assertThat(context.toSql()).contains("JSON_QUERY");
    }

    @Test
    void shouldRenderSliceWithThreeArgs() {
        // MongoDB: {$slice: ["$items", 2, 5]} - skip 2, take 5
        var expr = ArrayExpression.sliceWithSkip(
            FieldPathExpression.of("items"),
            LiteralExpression.of(2),
            LiteralExpression.of(5)
        );

        expr.render(context);

        assertThat(context.toSql()).contains("JSON_QUERY");
    }

    @Test
    void shouldReturnNewOps() {
        assertThat(ArrayExpression.concatArrays(java.util.List.of(FieldPathExpression.of("x"))).getOp())
            .isEqualTo(ArrayOp.CONCAT_ARRAYS);
        assertThat(ArrayExpression.slice(FieldPathExpression.of("x"), LiteralExpression.of(1)).getOp())
            .isEqualTo(ArrayOp.SLICE);
    }
}
