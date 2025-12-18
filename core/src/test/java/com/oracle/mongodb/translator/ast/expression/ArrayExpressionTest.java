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
import java.util.Map;
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
    // Oracle: JSON_QUERY(data, '$.items[0]') - preserves types (numbers, strings, booleans)
    var expr =
        ArrayExpression.arrayElemAt(FieldPathExpression.of("items"), LiteralExpression.of(0));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_QUERY(data, '$.items[0]')");
  }

  @Test
  void shouldRenderArrayElemAtWithNonZeroIndex() {
    // MongoDB: {$arrayElemAt: ["$tags", 2]}
    // Oracle: JSON_QUERY(data, '$.tags[2]') - preserves types
    var expr = ArrayExpression.arrayElemAt(FieldPathExpression.of("tags"), LiteralExpression.of(2));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_QUERY(data, '$.tags[2]')");
  }

  @Test
  void shouldRenderSize() {
    // MongoDB: {$size: "$items"}
    // Oracle: JSON_VALUE(data, '$.items.size()' RETURNING NUMBER) - .size() is a JSON path function
    var expr = ArrayExpression.size(FieldPathExpression.of("items"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.items.size()' RETURNING NUMBER)");
  }

  @Test
  void shouldRenderFirst() {
    // MongoDB: {$first: "$items"}
    // Oracle: JSON_QUERY(data, '$.items[0]') - preserves types
    var expr = ArrayExpression.first(FieldPathExpression.of("items"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_QUERY(data, '$.items[0]')");
  }

  @Test
  void shouldRenderLast() {
    // MongoDB: {$last: "$items"}
    // Oracle: JSON_QUERY(data, '$.items[last]') - preserves types
    var expr = ArrayExpression.last(FieldPathExpression.of("items"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_QUERY(data, '$.items[last]')");
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
    // JSON_QUERY preserves types
    var expr =
        ArrayExpression.arrayElemAt(
            FieldPathExpression.of("orders.items"), LiteralExpression.of(0));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_QUERY(data, '$.orders.items[0]')");
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
    // JSON_QUERY preserves types
    var expr =
        ArrayExpression.arrayElemAt(FieldPathExpression.of("items"), LiteralExpression.of(-1));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_QUERY(data, '$.items[last]')");
  }

  @Test
  void shouldRenderArrayElemAtWithNegativeIndexOffset() {
    // MongoDB: {$arrayElemAt: ["$items", -2]} - second to last element
    // JSON_QUERY preserves types
    var expr =
        ArrayExpression.arrayElemAt(FieldPathExpression.of("items"), LiteralExpression.of(-2));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_QUERY(data, '$.items[last-1]')");
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
  void shouldRenderFilterWithVariableFieldAccess() {
    // MongoDB: {$filter: {input: "$items", as: "item", cond: {$gt: ["$$item.price", 100]}}}
    // Oracle: (SELECT JSON_ARRAYAGG(val FORMAT JSON) FROM JSON_TABLE(...,
    //         COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$', price NUMBER PATH '$.price'))
    //         WHERE price > :1)
    var expr =
        ArrayExpression.filter(
            FieldPathExpression.of("items"),
            new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("$item.price", JsonReturnType.NUMBER),
                LiteralExpression.of(100)));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("JSON_TABLE");
    assertThat(sql).contains("WHERE");
    // Should extract the price field for comparison
    assertThat(sql).contains("$.price");
    // Should use price column name in WHERE clause
    assertThat(sql).contains("price >");
  }

  @Test
  void shouldRenderMap() {
    var expr = ArrayExpression.map(FieldPathExpression.of("items"), FieldPathExpression.of("name"));

    expr.render(context);

    assertThat(context.toSql()).contains("JSON_ARRAYAGG").contains("JSON_TABLE");
  }

  @Test
  void shouldRenderMapWithVariableFieldAccess() {
    // MongoDB: {$map: {input: "$items", as: "item", in: "$$item.product"}}
    // Oracle: (SELECT JSON_ARRAYAGG(product) FROM JSON_TABLE(...,
    //         COLUMNS (product VARCHAR2(4000) PATH '$.product')))
    var expr =
        ArrayExpression.map(
            FieldPathExpression.of("items"), FieldPathExpression.of("$item.product"));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("JSON_TABLE");
    // Should extract the product field for mapping
    assertThat(sql).contains("$.product");
  }

  // ==================== $reduce Tests ====================

  @Test
  void shouldRenderReduceSumPattern() {
    // MongoDB: {$reduce: {input: "$scores", initialValue: 0,
    //           in: {$add: ["$$value", "$$this"]}}}
    // Oracle: (SELECT NVL(SUM(TO_NUMBER(val)), 0) FROM JSON_TABLE(...)
    var addExpr =
        new ArithmeticExpression(
            ArithmeticOp.ADD,
            List.of(FieldPathExpression.of("$value"), FieldPathExpression.of("$this")));
    var expr =
        ArrayExpression.reduce(FieldPathExpression.of("scores"), LiteralExpression.of(0), addExpr);

    expr.render(context);

    String sql = context.toSql();
    // Should translate $reduce with $add to SQL SUM
    assertThat(sql).containsIgnoringCase("SUM");
    assertThat(sql).contains("JSON_TABLE");
    assertThat(sql).contains("$.scores");
  }

  @Test
  void shouldRenderReduceWithConcatPattern() {
    // MongoDB: {$reduce: {input: "$tags", initialValue: "", in: {$concat: ["$$value", "$$this"]}}}
    // Oracle: (SELECT LISTAGG(val, '') FROM JSON_TABLE(...))
    var concatExpr =
        new StringExpression(
            StringOp.CONCAT,
            List.of(FieldPathExpression.of("$value"), FieldPathExpression.of("$this")));
    var expr =
        ArrayExpression.reduce(
            FieldPathExpression.of("tags"), LiteralExpression.of(""), concatExpr);

    expr.render(context);

    String sql = context.toSql();
    // Should translate $reduce with $concat to SQL LISTAGG
    assertThat(sql).containsIgnoringCase("LISTAGG");
    assertThat(sql).contains("JSON_TABLE");
  }

  @Test
  void shouldRenderReduceGenericCase() {
    // For unsupported patterns, should render a working fallback or descriptive placeholder
    var expr =
        ArrayExpression.reduce(
            FieldPathExpression.of("items"), LiteralExpression.of(0), LiteralExpression.of("sum"));

    expr.render(context);

    // Should at least render something that indicates $reduce
    assertThat(context.toSql()).containsIgnoringCase("reduce");
  }

  @Test
  void shouldRenderReduceSumPatternWithNestedFieldAccess() {
    // MongoDB: {$reduce: {input: "$items", initialValue: 0,
    //           in: {$add: ["$$value", "$$this.price"]}}}
    // Oracle: (SELECT NVL(SUM(val), 0) FROM JSON_TABLE(...'$.price'))
    var addExpr =
        new ArithmeticExpression(
            ArithmeticOp.ADD,
            List.of(FieldPathExpression.of("$value"), FieldPathExpression.of("$this.price")));
    var expr =
        ArrayExpression.reduce(FieldPathExpression.of("items"), LiteralExpression.of(0), addExpr);

    expr.render(context);

    String sql = context.toSql();
    // Should translate $reduce with $add and nested field to SQL SUM
    assertThat(sql).containsIgnoringCase("SUM");
    assertThat(sql).contains("JSON_TABLE");
    assertThat(sql).contains("$.items");
    // Should use nested path for field access
    assertThat(sql).contains("$.price");
  }

  @Test
  void shouldRenderReduceWithNestedArithmeticExpression() {
    // MongoDB: {$reduce: {input: "$items", initialValue: 0,
    //           in: {$add: ["$$value", {$multiply: ["$$this.qty", "$$this.price"]}]}}}
    // This is used in COMPLEX027 to calculate order totals
    // Oracle: (SELECT NVL(SUM(qty * price), 0) FROM JSON_TABLE(...))
    var multiplyExpr =
        new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            List.of(
                FieldPathExpression.of("$this.qty"),
                FieldPathExpression.of("$this.price")));
    var addExpr =
        new ArithmeticExpression(
            ArithmeticOp.ADD,
            List.of(FieldPathExpression.of("$value"), multiplyExpr));
    var expr =
        ArrayExpression.reduce(FieldPathExpression.of("items"), LiteralExpression.of(0), addExpr);

    expr.render(context);

    String sql = context.toSql();
    // Should translate $reduce with nested arithmetic to SQL SUM
    assertThat(sql).containsIgnoringCase("SUM");
    assertThat(sql).contains("JSON_TABLE");
    assertThat(sql).contains("$.items");
    // Should extract both qty and price fields
    assertThat(sql).contains("qty");
    assertThat(sql).contains("price");
    // Should NOT contain the "not supported" message
    assertThat(sql).doesNotContain("not supported");
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

  // Tests for new array operators: $reverseArray, $sortArray, $in, $isArray, $indexOfArray

  @Test
  void shouldRenderReverseArray() {
    // MongoDB: {$reverseArray: "$items"}
    // Oracle: Uses JSON_QUERY with array reversal via subquery
    var expr = ArrayExpression.reverseArray(FieldPathExpression.of("items"));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("ORDER BY");
    assertThat(sql).contains("DESC");
  }

  @Test
  void shouldRenderSortArrayAscending() {
    // MongoDB: {$sortArray: {input: "$scores", sortBy: 1}}
    var expr = ArrayExpression.sortArray(FieldPathExpression.of("scores"), true);

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("ORDER BY");
    assertThat(sql).containsIgnoringCase("ASC");
  }

  @Test
  void shouldRenderSortArrayDescending() {
    // MongoDB: {$sortArray: {input: "$scores", sortBy: -1}}
    var expr = ArrayExpression.sortArray(FieldPathExpression.of("scores"), false);

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("ORDER BY");
    assertThat(sql).containsIgnoringCase("DESC");
  }

  @Test
  void shouldRenderInOperator() {
    // MongoDB: {$in: ["apple", "$fruits"]} - checks if "apple" is in the fruits array
    var expr = ArrayExpression.in(LiteralExpression.of("apple"), FieldPathExpression.of("fruits"));

    expr.render(context);

    String sql = context.toSql();
    // Should produce a check for element existence in JSON array
    assertThat(sql).contains("JSON_EXISTS");
  }

  @Test
  void shouldRenderInOperatorWithFieldValue() {
    // MongoDB: {$in: ["$item", "$validItems"]} - checks if field value is in array
    var expr =
        ArrayExpression.in(FieldPathExpression.of("item"), FieldPathExpression.of("validItems"));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_EXISTS");
  }

  @Test
  void shouldRenderIsArray() {
    // MongoDB: {$isArray: "$items"}
    var expr = ArrayExpression.isArray(FieldPathExpression.of("items"));

    expr.render(context);

    String sql = context.toSql();
    // Oracle: JSON_VALUE with type check or JSON_EXISTS
    assertThat(sql).containsAnyOf("JSON_VALUE", "JSON_EXISTS", "JSON_QUERY");
  }

  @Test
  void shouldRenderIndexOfArray() {
    // MongoDB: {$indexOfArray: ["$items", "needle"]}
    var expr =
        ArrayExpression.indexOfArray(
            FieldPathExpression.of("items"), LiteralExpression.of("needle"));

    expr.render(context);

    String sql = context.toSql();
    // Should search for element position in array
    assertThat(sql).contains("JSON_TABLE");
  }

  @Test
  void shouldRenderIndexOfArrayWithStartEnd() {
    // MongoDB: {$indexOfArray: ["$items", "needle", 2, 5]} - search from index 2 to 5
    var expr =
        ArrayExpression.indexOfArrayWithRange(
            FieldPathExpression.of("items"),
            LiteralExpression.of("needle"),
            LiteralExpression.of(2),
            LiteralExpression.of(5));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_TABLE");
  }

  @Test
  void shouldReturnReverseArrayOp() {
    var expr = ArrayExpression.reverseArray(FieldPathExpression.of("x"));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.REVERSE_ARRAY);
  }

  @Test
  void shouldReturnSortArrayOp() {
    var expr = ArrayExpression.sortArray(FieldPathExpression.of("x"), true);
    assertThat(expr.getOp()).isEqualTo(ArrayOp.SORT_ARRAY);
  }

  @Test
  void shouldReturnInOp() {
    var expr = ArrayExpression.in(LiteralExpression.of("x"), FieldPathExpression.of("arr"));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.IN);
  }

  @Test
  void shouldReturnIsArrayOp() {
    var expr = ArrayExpression.isArray(FieldPathExpression.of("x"));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.IS_ARRAY);
  }

  @Test
  void shouldReturnIndexOfArrayOp() {
    var expr = ArrayExpression.indexOfArray(FieldPathExpression.of("x"), LiteralExpression.of("a"));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.INDEX_OF_ARRAY);
  }

  // Tests for set operators: $setUnion, $setIntersection, $setDifference, $setEquals, $setIsSubset

  @Test
  void shouldRenderSetUnion() {
    // MongoDB: {$setUnion: ["$arr1", "$arr2"]}
    var expr =
        ArrayExpression.setUnion(
            List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION");
    assertThat(sql).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldRenderSetIntersection() {
    // MongoDB: {$setIntersection: ["$arr1", "$arr2"]}
    var expr =
        ArrayExpression.setIntersection(
            List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("INTERSECT");
    assertThat(sql).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldRenderSetDifference() {
    // MongoDB: {$setDifference: ["$arr1", "$arr2"]}
    var expr =
        ArrayExpression.setDifference(
            FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2"));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).containsAnyOf("MINUS", "EXCEPT");
    assertThat(sql).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldRenderSetEquals() {
    // MongoDB: {$setEquals: ["$arr1", "$arr2"]}
    var expr =
        ArrayExpression.setEquals(
            List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")));

    expr.render(context);

    String sql = context.toSql();
    // Check for symmetric difference = 0 or equivalent logic
    assertThat(sql).containsAnyOf("=", "CASE");
  }

  @Test
  void shouldRenderSetIsSubset() {
    // MongoDB: {$setIsSubset: ["$arr1", "$arr2"]}
    var expr =
        ArrayExpression.setIsSubset(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2"));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).containsAnyOf("MINUS", "EXCEPT", "NOT EXISTS", "COUNT");
  }

  @Test
  void shouldReturnSetUnionOp() {
    var expr = ArrayExpression.setUnion(List.of(FieldPathExpression.of("x")));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.SET_UNION);
  }

  @Test
  void shouldReturnSetIntersectionOp() {
    var expr = ArrayExpression.setIntersection(List.of(FieldPathExpression.of("x")));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.SET_INTERSECTION);
  }

  @Test
  void shouldReturnSetDifferenceOp() {
    var expr =
        ArrayExpression.setDifference(FieldPathExpression.of("x"), FieldPathExpression.of("y"));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.SET_DIFFERENCE);
  }

  @Test
  void shouldReturnSetEqualsOp() {
    var expr =
        ArrayExpression.setEquals(
            List.of(FieldPathExpression.of("x"), FieldPathExpression.of("y")));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.SET_EQUALS);
  }

  @Test
  void shouldReturnSetIsSubsetOp() {
    var expr =
        ArrayExpression.setIsSubset(FieldPathExpression.of("x"), FieldPathExpression.of("y"));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.SET_IS_SUBSET);
  }

  // ==================== $filter/$map General Case Tests ====================

  @Test
  void shouldRenderFilterOnExpressionArray() {
    // MongoDB: {$filter: {input: {$split: ["$name", ","]}, cond: {$ne: ["$$this", ""]}}}
    // When input is not a field path, we need a fallback
    var splitExpr =
        StringExpression.split(FieldPathExpression.of("tags"), LiteralExpression.of(","));
    var conditionExpr =
        new ComparisonExpression(
            ComparisonOp.NE, FieldPathExpression.of("$this"), LiteralExpression.of(""));
    var expr = ArrayExpression.filter(splitExpr, conditionExpr);

    expr.render(context);

    // Should indicate that this case is not fully supported
    assertThat(context.toSql()).containsIgnoringCase("filter");
  }

  @Test
  void shouldRenderMapOnExpressionArray() {
    // MongoDB: {$map: {input: {$split: ["$tags", ","]}, in: {$toUpper: "$$this"}}}
    // When input is not a field path, we need a fallback
    var splitExpr =
        StringExpression.split(FieldPathExpression.of("tags"), LiteralExpression.of(","));
    var upperExpr = StringExpression.toUpper(FieldPathExpression.of("$this"));
    var expr = ArrayExpression.map(splitExpr, upperExpr);

    expr.render(context);

    // Should indicate that this case is not fully supported
    assertThat(context.toSql()).containsIgnoringCase("map");
  }

  // ==================== $range Tests ====================

  @Test
  void shouldRenderRangeWithStartAndEnd() {
    // MongoDB: {$range: [0, 10]} - generates [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    // Oracle: SELECT JSON_ARRAYAGG(n ORDER BY n) FROM (SELECT 0 + LEVEL - 1 AS n FROM DUAL
    //         CONNECT BY LEVEL <= 10 - 0)
    var expr = ArrayExpression.range(LiteralExpression.of(0), LiteralExpression.of(10));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("CONNECT BY");
    assertThat(sql).contains("LEVEL");
  }

  @Test
  void shouldRenderRangeWithStep() {
    // MongoDB: {$range: [0, 10, 2]} - generates [0, 2, 4, 6, 8]
    // Oracle: SELECT JSON_ARRAYAGG(n ORDER BY n) FROM (SELECT 0 + (LEVEL - 1) * 2 AS n FROM DUAL
    //         CONNECT BY 0 + (LEVEL - 1) * 2 < 10)
    var expr =
        ArrayExpression.range(
            LiteralExpression.of(0), LiteralExpression.of(10), LiteralExpression.of(2));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("CONNECT BY");
    assertThat(sql).containsAnyOf("* 2", "*2");
  }

  @Test
  void shouldRenderRangeWithNegativeStep() {
    // MongoDB: {$range: [10, 0, -1]} - generates [10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
    var expr =
        ArrayExpression.range(
            LiteralExpression.of(10), LiteralExpression.of(0), LiteralExpression.of(-1));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("CONNECT BY");
  }

  @Test
  void shouldRenderRangeWithFieldPaths() {
    // MongoDB: {$range: ["$start", "$end"]}
    var expr =
        ArrayExpression.range(FieldPathExpression.of("start"), FieldPathExpression.of("end"));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("CONNECT BY");
    assertThat(sql).contains("JSON_VALUE");
  }

  @Test
  void shouldReturnRangeOp() {
    var expr = ArrayExpression.range(LiteralExpression.of(0), LiteralExpression.of(10));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.RANGE);
  }

  @Test
  void shouldProvideReadableRangeToString() {
    var expr = ArrayExpression.range(LiteralExpression.of(0), LiteralExpression.of(10));
    assertThat(expr.toString()).contains("$range");
  }

  // === $zip Tests (TDD RED phase) ===

  @Test
  void shouldRenderZipWithTwoArrays() {
    // MongoDB: {$zip: {inputs: ["$arr1", "$arr2"]}}
    // Creates array of pairs: [[arr1[0], arr2[0]], [arr1[1], arr2[1]], ...]
    var expr =
        ArrayExpression.zip(
            List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("JSON_TABLE");
  }

  @Test
  void shouldRenderZipWithThreeArrays() {
    // MongoDB: {$zip: {inputs: ["$arr1", "$arr2", "$arr3"]}}
    var expr =
        ArrayExpression.zip(
            List.of(
                FieldPathExpression.of("arr1"),
                FieldPathExpression.of("arr2"),
                FieldPathExpression.of("arr3")));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldRenderZipWithUseLongestLength() {
    // MongoDB: {$zip: {inputs: ["$arr1", "$arr2"], useLongestLength: true}}
    var expr =
        ArrayExpression.zip(
            List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")),
            true,
            null);

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    // Should use FULL OUTER JOIN to preserve longest array
    assertThat(sql.toUpperCase()).contains("OUTER");
  }

  @Test
  void shouldRenderZipWithDefaults() {
    // MongoDB: {$zip: {inputs: ["$arr1", "$arr2"], useLongestLength: true, defaults: [0, "N/A"]}}
    var expr =
        ArrayExpression.zip(
            List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")),
            true,
            List.of(LiteralExpression.of(0), LiteralExpression.of("N/A")));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    // Should use COALESCE for defaults
    assertThat(sql.toUpperCase()).contains("COALESCE");
  }

  @Test
  void shouldReturnZipOp() {
    var expr =
        ArrayExpression.zip(
            List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")));
    assertThat(expr.getOp()).isEqualTo(ArrayOp.ZIP);
  }

  @Test
  void shouldProvideReadableZipToString() {
    var expr =
        ArrayExpression.zip(
            List.of(FieldPathExpression.of("arr1"), FieldPathExpression.of("arr2")));
    assertThat(expr.toString()).contains("$zip");
  }

  // ==================== $sortArray with Field-Based sortBy Tests ====================

  @Test
  void shouldRenderSortArrayByFieldDescending() {
    // MongoDB: {$sortArray: {input: "$products", sortBy: {totalRevenue: -1}}}
    // Oracle: (SELECT JSON_ARRAYAGG(val FORMAT JSON ORDER BY totalRevenue DESC)
    //         FROM JSON_TABLE(data, '$.products[*]'
    //         COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$',
    //                  totalRevenue NUMBER PATH '$.totalRevenue')))
    var expr =
        ArrayExpression.sortArrayByField(
            FieldPathExpression.of("products"), "totalRevenue", false);

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("JSON_TABLE");
    // Should order by the field name
    assertThat(sql).containsIgnoringCase("ORDER BY");
    assertThat(sql).contains("totalRevenue");
    assertThat(sql).containsIgnoringCase("DESC");
    // Should preserve the full JSON object, not just extract the sort field
    assertThat(sql).containsIgnoringCase("FORMAT JSON");
  }

  @Test
  void shouldRenderSortArrayByFieldAscending() {
    // MongoDB: {$sortArray: {input: "$items", sortBy: {price: 1}}}
    var expr =
        ArrayExpression.sortArrayByField(FieldPathExpression.of("items"), "price", true);

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("ORDER BY");
    assertThat(sql).contains("price");
    assertThat(sql).containsIgnoringCase("ASC");
  }

  @Test
  void shouldReturnSortFieldFromArrayExpression() {
    var expr =
        ArrayExpression.sortArrayByField(
            FieldPathExpression.of("products"), "totalRevenue", false);

    assertThat(expr.getSortField()).isEqualTo("totalRevenue");
  }

  // ==================== $map with Object Transformation Tests ====================

  @Test
  void shouldRenderMapWithObjectTransformation() {
    // MongoDB: {$map: {input: "$items", as: "item", in: {
    //   product: "$$item.product",
    //   qty: "$$item.qty",
    //   price: "$$item.price",
    //   lineTotal: {$multiply: ["$$item.qty", "$$item.price"]}
    // }}}
    // This is used in COMPLEX027 to transform items array
    // Oracle: Should use JSON_TABLE with columns for each referenced field

    // Build the object transformation expression
    Map<String, Expression> fields = new java.util.LinkedHashMap<>();
    fields.put("product", FieldPathExpression.of("$item.product"));
    fields.put("qty", FieldPathExpression.of("$item.qty"));
    fields.put("price", FieldPathExpression.of("$item.price"));
    fields.put(
        "lineTotal",
        new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            List.of(
                FieldPathExpression.of("$item.qty"),
                FieldPathExpression.of("$item.price"))));

    var inlineObj = new InlineObjectExpression(fields);
    var expr = ArrayExpression.map(FieldPathExpression.of("items"), inlineObj);

    var contextWithAlias = new DefaultSqlGenerationContext(false, null, "base");
    expr.render(contextWithAlias);

    String sql = contextWithAlias.toSql();
    // Should generate JSON_TABLE with columns
    assertThat(sql).contains("JSON_TABLE");
    assertThat(sql).contains("$.items");
    // Should have columns for each referenced field
    assertThat(sql).containsIgnoringCase("product");
    assertThat(sql).containsIgnoringCase("qty");
    assertThat(sql).containsIgnoringCase("price");
    // Should generate JSON_OBJECT
    assertThat(sql).contains("JSON_OBJECT");
    // Should NOT have invalid Oracle dot notation
    assertThat(sql).doesNotContain("base.data.item.product");
    assertThat(sql).doesNotContain("base.data.item.qty");
    assertThat(sql).doesNotContain("base.data.item.price");
  }

  @Test
  void shouldRenderFilterWithNestedArithmeticCondition() {
    // MongoDB: {$filter: {input: "$items", as: "item", cond: {$gte: [{$multiply:
    //   ["$$item.qty", "$$item.price"]}, 100]}}}
    // This is used in COMPLEX027 to filter high-value items
    // Oracle: Should use JSON_TABLE with columns for qty and price, then
    //   WHERE (qty * price) >= 100

    // Build the condition: {$gte: [{$multiply: ["$item.qty", "$item.price"]}, 100]}
    var multiplyExpr =
        new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            List.of(
                FieldPathExpression.of("$item.qty"), FieldPathExpression.of("$item.price")));
    var condition =
        new ComparisonExpression(ComparisonOp.GTE, multiplyExpr, LiteralExpression.of(100));

    var expr = ArrayExpression.filter(FieldPathExpression.of("items"), condition);

    var contextWithAlias = new DefaultSqlGenerationContext(false, null, "base");
    expr.render(contextWithAlias);

    String sql = contextWithAlias.toSql();

    // Should generate JSON_TABLE with columns for qty and price
    assertThat(sql).contains("JSON_TABLE");
    assertThat(sql).contains("$.items");
    assertThat(sql).containsIgnoringCase("qty");
    assertThat(sql).containsIgnoringCase("price");

    // Should use column references in WHERE clause (qty * price) >= 100
    // Note: the arithmetic should use the column names, not base.data.item paths
    assertThat(sql).doesNotContain("base.data.item.qty");
    assertThat(sql).doesNotContain("base.data.item.price");

    // Should have WHERE clause with the arithmetic condition
    assertThat(sql).containsIgnoringCase("WHERE");
  }

  // ==================== CTE Context Tests ====================

  @Test
  void shouldRenderSortArrayByFieldInCteContext() {
    // In CTE context, "products" is a plain column name (an array from $push accumulator)
    // $sortArray: {input: "$products", sortBy: {totalRevenue: -1}}
    // Should NOT generate: JSON_TABLE(cte.data, '$.products[*]' ...)
    // SHOULD generate: JSON_TABLE(products, '$[*]' ...)
    var expr =
        ArrayExpression.sortArrayByField(
            FieldPathExpression.of("products"), "totalRevenue", false);

    // Create context in CTE mode
    var cteContext = new DefaultSqlGenerationContext(true, null, "cte_group_1");
    cteContext.setInCteContext(true);

    expr.render(cteContext);

    String sql = cteContext.toSql();

    // In CTE context, the column is the array, so JSON_TABLE path should be '$[*]'
    // NOT '$.products[*]' since we're not accessing data.products, just the column itself
    assertThat(sql)
        .as("CTE context should reference column directly, not via .data")
        .doesNotContain("cte_group_1.data");
    assertThat(sql)
        .as("CTE context should use '$[*]' path since column IS the array")
        .contains("'$[*]'");
    assertThat(sql)
        .as("Should still have ORDER BY")
        .containsIgnoringCase("ORDER BY");
    assertThat(sql)
        .as("Should order by totalRevenue")
        .contains("totalRevenue");
  }

  @Test
  void shouldRenderSliceOnSortArrayInCteContext() {
    // $slice($sortArray($products, {totalRevenue: -1}), 3) in CTE context
    var sortedProducts =
        ArrayExpression.sortArrayByField(
            FieldPathExpression.of("products"), "totalRevenue", false);
    var slicedProducts = ArrayExpression.slice(sortedProducts, LiteralExpression.of(3));

    var cteContext = new DefaultSqlGenerationContext(true, null, "cte_group_1");
    cteContext.setInCteContext(true);

    slicedProducts.render(cteContext);

    String sql = cteContext.toSql();

    // Should not reference cte_group_1.data
    assertThat(sql)
        .as("CTE context should not reference .data column")
        .doesNotContain(".data");
    // Should limit to 3 rows
    assertThat(sql)
        .as("Should limit results")
        .containsIgnoringCase("FETCH FIRST 3 ROWS ONLY");
  }

  @Test
  void shouldRenderSizeInCteContext() {
    // In CTE context, "states" is a plain column name (an array from $addToSet accumulator)
    // $size: "$states" should generate: JSON_VALUE(states, '$.size()' RETURNING NUMBER)
    // NOT: JSON_VALUE(cte_group_0.data, '$.states.size()' RETURNING NUMBER)
    var expr = ArrayExpression.size(FieldPathExpression.of("states"));

    // Create context in CTE mode
    var cteContext = new DefaultSqlGenerationContext(true, null, "cte_group_0");
    cteContext.setInCteContext(true);

    expr.render(cteContext);

    String sql = cteContext.toSql();

    // In CTE context, the column IS the array, so we reference it directly
    assertThat(sql)
        .as("CTE context should NOT reference .data column")
        .doesNotContain(".data");
    assertThat(sql)
        .as("CTE context should reference column name directly")
        .contains("JSON_VALUE(states,");
    assertThat(sql)
        .as("Should use $.size() path since column IS the array")
        .contains("'$.size()'");
  }

  @Test
  void shouldRenderSumOnLookupResultWithNestedPath() {
    // Test for $sum on lookup result field with nested path
    // MongoDB: {$sum: "$orders.payment.amount"} where "orders" is from $lookup
    // Should generate correlated subquery to sum from the joined table
    DefaultSqlGenerationContext ctx = new DefaultSqlGenerationContext(false, null, "base");
    // Register a lookup: orders comes from orders_detailed table
    ctx.registerLookupField("orders", "orders_detailed", "_id", "customerId");

    // $sum: "$orders.payment.amount"
    var expr = ArrayExpression.sumArray(FieldPathExpression.of("orders.payment.amount"));
    expr.render(ctx);

    String sql = ctx.toSql();

    // Should generate correlated subquery, NOT JSON_TABLE on base.data
    assertThat(sql)
        .as("Should NOT reference base.data.orders (lookup result doesn't exist in source)")
        .doesNotContain("base.data, '$.orders");
    assertThat(sql)
        .as("Should query from the foreign table")
        .contains("orders_detailed");
    assertThat(sql)
        .as("Should sum the nested field")
        .containsIgnoringCase("SUM");
  }

  @Test
  void shouldRenderIsArrayUsingTypeFunction() {
    // $isArray should use JSON_VALUE with .type() to properly detect arrays
    // The previous implementation using JSON_EXISTS with [0] was incorrect because
    // Oracle treats scalars as single-element arrays
    var expr = ArrayExpression.isArray(FieldPathExpression.of("tags"));
    expr.render(context);
    String sql = context.toSql();

    // Should use .type() = 'array' for proper detection
    assertThat(sql)
        .as("Should use .type() JSON path function for array detection")
        .contains(".type()");
    assertThat(sql)
        .as("Should compare type to 'array'")
        .contains("'array'");
    // Must return SQL boolean literals TRUE/FALSE for proper JSON serialization
    // as JSON booleans instead of strings
    assertThat(sql)
        .as("Should return SQL TRUE for JSON boolean serialization")
        .contains("THEN TRUE");
    assertThat(sql)
        .as("Should return SQL FALSE for JSON boolean serialization")
        .contains("ELSE FALSE");
  }

  @Test
  void shouldRenderReduceConcatWithJsonTypeForEmptyArrays() {
    // $reduce with concat pattern should return empty string (not null) for empty arrays
    // MongoDB: $reduce: {input: "$tags", initialValue: "", in: {$concat: ["$$value", "$$this"]}}
    // Oracle treats '' as NULL, so we use JSON('""') for proper empty string output
    var inExpr =
        new StringExpression(
            StringOp.CONCAT,
            List.of(FieldPathExpression.of("$value"), FieldPathExpression.of("$this")));
    var expr =
        new ArrayExpression(
            ArrayOp.REDUCE,
            FieldPathExpression.of("tags"),
            LiteralExpression.of(""),
            List.of(inExpr));

    expr.render(context);
    String sql = context.toSql();

    // Should use JSON type to ensure empty string is properly serialized
    assertThat(sql)
        .as("Should use CASE with JSON for empty arrays")
        .contains("CASE WHEN listagg_result IS NULL THEN JSON");
    assertThat(sql)
        .as("Should output JSON empty string literal for NULL")
        .contains("JSON('\"\"')");
    assertThat(sql)
        .as("Should wrap non-null result in JSON quotes")
        .contains("JSON('\"' || listagg_result || '\"')");
  }

  @Test
  void shouldRenderAvgOnPipelineLookupResultWithNestedPath() {
    // COMPLEX017: $avg on $graphLookup result field with nested path
    // MongoDB: {$avg: "$directAndIndirectReports.rating"} where the array is from $graphLookup
    // Should use the pipeline lookup alias, NOT base.data
    DefaultSqlGenerationContext ctx = new DefaultSqlGenerationContext(false, null, "base");
    // Register pipeline lookup: directAndIndirectReports comes from graphLookup CTE
    ctx.registerPipelineLookupAlias("directAndIndirectReports", "directAndIndirectReports_cte");

    // $avg: "$directAndIndirectReports.rating" (with $ prefix as created by parser)
    var expr =
        ArrayExpression.avgArray(FieldPathExpression.of("$directAndIndirectReports.rating"));
    expr.render(ctx);

    String sql = ctx.toSql();

    // Should use the CTE alias, NOT base.data
    assertThat(sql)
        .as("Should NOT reference base.data (graphLookup result doesn't exist in source)")
        .doesNotContain("base.data");
    assertThat(sql)
        .as("Should use the pipeline lookup alias")
        .contains("directAndIndirectReports_cte.directAndIndirectReports");
    assertThat(sql).as("Should calculate average").containsIgnoringCase("AVG");
  }

  @Test
  void shouldRenderAvgOnSimplePipelineLookupResult() {
    // Test $avg on a simple pipeline lookup array (no nested path)
    // MongoDB: {$avg: "$scores"} where "scores" is a numeric array from $graphLookup
    DefaultSqlGenerationContext ctx = new DefaultSqlGenerationContext(false, null, "base");
    ctx.registerPipelineLookupAlias("scores", "scores_cte");

    // With $ prefix as created by parser
    var expr = ArrayExpression.avgArray(FieldPathExpression.of("$scores"));
    expr.render(ctx);

    String sql = ctx.toSql();

    assertThat(sql)
        .as("Should NOT reference base.data")
        .doesNotContain("base.data");
    assertThat(sql)
        .as("Should use the pipeline lookup alias")
        .contains("scores_cte.scores");
    assertThat(sql).as("Should calculate average").containsIgnoringCase("AVG");
  }
}
