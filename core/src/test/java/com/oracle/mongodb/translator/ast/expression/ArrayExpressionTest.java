/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
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
    var expr =
        ArrayExpression.arrayElemAt(FieldPathExpression.of("items"), LiteralExpression.of(0));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.items[0]')");
  }

  @Test
  void shouldRenderArrayElemAtWithNonZeroIndex() {
    // MongoDB: {$arrayElemAt: ["$tags", 2]}
    // Oracle: JSON_VALUE(data, '$.tags[2]')
    var expr = ArrayExpression.arrayElemAt(FieldPathExpression.of("tags"), LiteralExpression.of(2));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.tags[2]')");
  }

  @Test
  void shouldRenderSize() {
    // MongoDB: {$size: "$items"}
    // Oracle: JSON_QUERY(data, '$.items.size()')
    var expr = ArrayExpression.size(FieldPathExpression.of("items"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.items.size()')");
  }

  @Test
  void shouldRenderFirst() {
    // MongoDB: {$first: "$items"}
    // Oracle: JSON_VALUE(data, '$.items[0]')
    var expr = ArrayExpression.first(FieldPathExpression.of("items"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.items[0]')");
  }

  @Test
  void shouldRenderLast() {
    // MongoDB: {$last: "$items"}
    // Oracle: JSON_VALUE(data, '$.items[last]')
    var expr = ArrayExpression.last(FieldPathExpression.of("items"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.items[last]')");
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
    var expr =
        ArrayExpression.arrayElemAt(
            FieldPathExpression.of("orders.items"), LiteralExpression.of(0));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.orders.items[0]')");
  }

  // New array operator tests

  @Test
  void shouldRenderConcatArrays() {
    // MongoDB: {$concatArrays: ["$arr1", "$arr2"]}
    var expr =
        ArrayExpression.concatArrays(
            java.util.List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")));

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
    var expr = ArrayExpression.slice(FieldPathExpression.of("items"), LiteralExpression.of(3));

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_QUERY");
  }

  @Test
  void shouldRenderSliceWithThreeArgs() {
    // MongoDB: {$slice: ["$items", 2, 5]} - skip 2, take 5
    var expr =
        ArrayExpression.sliceWithSkip(
            FieldPathExpression.of("items"), LiteralExpression.of(2), LiteralExpression.of(5));

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

  @Test
  void shouldRenderArrayElemAtWithNegativeIndex() {
    // MongoDB: {$arrayElemAt: ["$items", -1]} - last element
    var expr =
        ArrayExpression.arrayElemAt(FieldPathExpression.of("items"), LiteralExpression.of(-1));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.items[last]')");
  }

  @Test
  void shouldRenderArrayElemAtWithNegativeIndexOffset() {
    // MongoDB: {$arrayElemAt: ["$items", -2]} - second to last element
    var expr =
        ArrayExpression.arrayElemAt(FieldPathExpression.of("items"), LiteralExpression.of(-2));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.items[last-1]')");
  }

  @Test
  void shouldRenderSliceWithNegativeCount() {
    // MongoDB: {$slice: ["$items", -3]} - last 3 elements
    var expr = ArrayExpression.slice(FieldPathExpression.of("items"), LiteralExpression.of(-3));

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_QUERY").contains("last");
  }

  @Test
  void shouldRenderEmptyConcatArrays() {
    var expr = ArrayExpression.concatArrays(java.util.List.of());

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_QUERY('[]', '$')");
  }

  @Test
  void shouldRenderFilter() {
    var expr =
        ArrayExpression.filter(
            FieldPathExpression.of("items"),
            new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                LiteralExpression.of(10)));

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_ARRAYAGG").contains("JSON_TABLE").contains("WHERE");
  }

  @Test
  void shouldRenderMap() {
    var expr = ArrayExpression.map(FieldPathExpression.of("items"), FieldPathExpression.of("name"));

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_ARRAYAGG").contains("JSON_TABLE");
  }

  @Test
  void shouldRenderReduce() {
    var expr =
        ArrayExpression.reduce(
            FieldPathExpression.of("items"), LiteralExpression.of(0), LiteralExpression.of("sum"));

    expr.render(context);

    assertThat(context.toSql()).contains("$reduce not fully supported");
  }

  @Test
  void shouldReturnIndexExpression() {
    var index = LiteralExpression.of(5);
    var expr = ArrayExpression.arrayElemAt(FieldPathExpression.of("arr"), index);

    assertThat(expr.getIndexExpression()).isEqualTo(index);
  }

  @Test
  void shouldReturnAdditionalArgs() {
    var expr =
        ArrayExpression.sliceWithSkip(
            FieldPathExpression.of("items"), LiteralExpression.of(1), LiteralExpression.of(3));

    assertThat(expr.getAdditionalArgs()).hasSize(1);
  }

  @Test
  void shouldReturnNullAdditionalArgsForSimpleOps() {
    var expr = ArrayExpression.size(FieldPathExpression.of("items"));

    assertThat(expr.getAdditionalArgs()).isNull();
  }

  @Test
  void shouldProvideToStringWithIndex() {
    var expr =
        ArrayExpression.arrayElemAt(FieldPathExpression.of("items"), LiteralExpression.of(0));

    assertThat(expr.toString()).contains("$arrayElemAt").contains("items");
  }

  // Tests for expression-based array operations (not field paths)

  @Test
  void shouldRenderArrayElemAtOnSplitResult() {
    // MongoDB: {$arrayElemAt: [{$split: ["$name", " "]}, 0]} - get first word
    var splitExpr =
        StringExpression.split(FieldPathExpression.of("name"), LiteralExpression.of(" "));
    var expr = ArrayExpression.arrayElemAt(splitExpr, LiteralExpression.of(0));

    expr.render(context);

    assertThat(context.toSql()).contains("REGEXP_SUBSTR");
    assertThat(context.toSql()).contains("1, 1"); // position 1 (1-based)
  }

  @Test
  void shouldRenderArrayElemAtOnSplitWithNonZeroIndex() {
    // Get second word
    var splitExpr =
        StringExpression.split(FieldPathExpression.of("fullName"), LiteralExpression.of("-"));
    var expr = ArrayExpression.arrayElemAt(splitExpr, LiteralExpression.of(2));

    expr.render(context);

    assertThat(context.toSql()).contains("REGEXP_SUBSTR");
    assertThat(context.toSql()).contains("1, 3"); // position 3 (1-based, index 2 + 1)
  }

  @Test
  void shouldRenderSizeOnSplitResult() {
    // MongoDB: {$size: {$split: ["$tags", ","]}} - count comma-separated items
    var splitExpr =
        StringExpression.split(FieldPathExpression.of("tags"), LiteralExpression.of(","));
    var expr = ArrayExpression.size(splitExpr);

    expr.render(context);

    assertThat(context.toSql()).contains("REGEXP_COUNT");
    assertThat(context.toSql()).contains("+ 1"); // delimiter count + 1 = element count
  }

  @Test
  void shouldRenderFirstOnSplitResult() {
    // MongoDB: {$first: {$split: ["$path", "/"]}}
    var splitExpr =
        StringExpression.split(FieldPathExpression.of("path"), LiteralExpression.of("/"));
    var expr = ArrayExpression.first(splitExpr);

    expr.render(context);

    assertThat(context.toSql()).contains("REGEXP_SUBSTR");
    assertThat(context.toSql()).contains("1, 1"); // first element
  }

  @Test
  void shouldRenderLastOnSplitResult() {
    // MongoDB: {$last: {$split: ["$path", "/"]}} - get last path segment
    var splitExpr =
        StringExpression.split(FieldPathExpression.of("path"), LiteralExpression.of("/"));
    var expr = ArrayExpression.last(splitExpr);

    expr.render(context);

    assertThat(context.toSql()).contains("REGEXP_SUBSTR");
    assertThat(context.toSql()).contains("REGEXP_COUNT"); // uses count to find last position
  }

  @Test
  void shouldRenderSliceOnExpressionWithPositiveCount() {
    // Use a generic expression for slice
    var expr =
        new ArrayExpression(
            ArrayOp.SLICE, LiteralExpression.of("[1,2,3,4,5]"), LiteralExpression.of(3));

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_ARRAYAGG");
    assertThat(context.toSql()).contains("FETCH FIRST");
  }

  @Test
  void shouldRenderSliceOnExpressionWithNegativeCount() {
    // Get last 2 elements
    var expr =
        new ArrayExpression(
            ArrayOp.SLICE, LiteralExpression.of("[1,2,3,4,5]"), LiteralExpression.of(-2));

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_ARRAYAGG");
    assertThat(context.toSql()).contains("ORDER BY rn DESC");
  }

  @Test
  void shouldRenderSliceOnExpressionWithSkipAndCount() {
    // Skip 1, take 2
    var expr =
        ArrayExpression.sliceWithSkip(
            LiteralExpression.of("[1,2,3,4,5]"),
            LiteralExpression.of(1),
            LiteralExpression.of(2));

    expr.render(context);

    assertThat(context.toSql()).contains("WHERE rn > 1");
    assertThat(context.toSql()).contains("FETCH FIRST 2 ROWS ONLY");
  }

  @Test
  void shouldRenderArrayElemAtOnGenericExpression() {
    // Non-split expression array access using JSON_TABLE
    var expr =
        new ArrayExpression(
            ArrayOp.ARRAY_ELEM_AT,
            LiteralExpression.of("[\"a\",\"b\",\"c\"]"),
            LiteralExpression.of(1));

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_TABLE");
    assertThat(context.toSql()).contains("WHERE rn = 2"); // 1-based
  }

  @Test
  void shouldRenderSizeOnGenericExpression() {
    var expr =
        new ArrayExpression(
            ArrayOp.SIZE, LiteralExpression.of("[\"a\",\"b\",\"c\"]"), null);

    expr.render(context);

    assertThat(context.toSql()).contains("SELECT COUNT(*)");
    assertThat(context.toSql()).contains("JSON_TABLE");
  }

  @Test
  void shouldRenderFirstOnGenericExpression() {
    var expr =
        new ArrayExpression(
            ArrayOp.FIRST, LiteralExpression.of("[\"a\",\"b\",\"c\"]"), null);

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_TABLE");
    assertThat(context.toSql()).contains("$[0]");
  }

  @Test
  void shouldRenderLastOnGenericExpression() {
    var expr =
        new ArrayExpression(
            ArrayOp.LAST, LiteralExpression.of("[\"a\",\"b\",\"c\"]"), null);

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_TABLE");
    assertThat(context.toSql()).contains("$[last]");
  }

  // Tests with base table alias

  @Test
  void shouldRenderWithBaseTableAlias() {
    var contextWithAlias = new DefaultSqlGenerationContext(false, null, "base");
    var expr = ArrayExpression.size(FieldPathExpression.of("items"));

    expr.render(contextWithAlias);

    assertThat(contextWithAlias.toSql()).contains("base.data");
  }

  @Test
  void shouldRenderArrayElemAtWithBaseTableAlias() {
    var contextWithAlias = new DefaultSqlGenerationContext(false, null, "orders");
    var expr =
        ArrayExpression.arrayElemAt(FieldPathExpression.of("items"), LiteralExpression.of(0));

    expr.render(contextWithAlias);

    assertThat(contextWithAlias.toSql()).contains("orders.data");
  }

  // Error handling tests

  @Test
  void shouldThrowOnNonLiteralArrayElemAtIndex() {
    var expr =
        ArrayExpression.arrayElemAt(
            FieldPathExpression.of("items"), FieldPathExpression.of("idx"));

    assertThatThrownBy(() -> expr.render(context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("literal number");
  }

  @Test
  void shouldThrowOnNonFieldPathConcatArraysArg() {
    var expr = ArrayExpression.concatArrays(List.of(LiteralExpression.of("[1,2,3]")));

    assertThatThrownBy(() -> expr.render(context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("field paths");
  }

  @Test
  void shouldThrowOnNonLiteralSliceCount() {
    var expr = ArrayExpression.slice(FieldPathExpression.of("items"), FieldPathExpression.of("n"));

    assertThatThrownBy(() -> expr.render(context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("literal number");
  }

  @Test
  void shouldThrowOnNonLiteralSliceSkipArgs() {
    var expr =
        new ArrayExpression(
            ArrayOp.SLICE,
            FieldPathExpression.of("items"),
            FieldPathExpression.of("skip"),
            List.of(LiteralExpression.of(5)));

    assertThatThrownBy(() -> expr.render(context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("literal numbers");
  }
}
