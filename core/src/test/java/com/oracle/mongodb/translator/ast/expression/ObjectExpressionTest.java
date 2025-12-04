/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for ObjectExpression. */
class ObjectExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  // $mergeObjects tests

  @Test
  void shouldRenderMergeObjectsWithTwoObjects() {
    // MongoDB: {$mergeObjects: ["$obj1", "$obj2"]}
    // Oracle: JSON_MERGEPATCH or JSON_OBJECT combination
    var expr =
        ObjectExpression.mergeObjects(
            List.of(FieldPathExpression.of("obj1"), FieldPathExpression.of("obj2")));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).containsAnyOf("JSON_MERGEPATCH", "JSON_OBJECT");
  }

  @Test
  void shouldRenderMergeObjectsWithMultipleObjects() {
    // MongoDB: {$mergeObjects: ["$obj1", "$obj2", "$obj3"]}
    var expr =
        ObjectExpression.mergeObjects(
            List.of(
                FieldPathExpression.of("obj1"),
                FieldPathExpression.of("obj2"),
                FieldPathExpression.of("obj3")));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).containsAnyOf("JSON_MERGEPATCH", "JSON_OBJECT");
  }

  @Test
  void shouldRenderMergeObjectsWithSingleObject() {
    // MongoDB: {$mergeObjects: "$obj"}
    var expr = ObjectExpression.mergeObjects(List.of(FieldPathExpression.of("obj")));

    expr.render(context);

    String sql = context.toSql();
    // Single object just returns itself
    assertThat(sql).contains("JSON_QUERY");
  }

  // $objectToArray tests

  @Test
  void shouldRenderObjectToArray() {
    // MongoDB: {$objectToArray: "$obj"}
    // Oracle: Convert object to array of {k, v} pairs
    var expr = ObjectExpression.objectToArray(FieldPathExpression.of("obj"));

    expr.render(context);

    String sql = context.toSql();
    // Should produce array structure with keys and values
    assertThat(sql).containsAnyOf("JSON_ARRAYAGG", "JSON_TABLE", "JSON_QUERY");
  }

  @Test
  void shouldRenderObjectToArrayWithNestedField() {
    // MongoDB: {$objectToArray: "$nested.obj"}
    var expr = ObjectExpression.objectToArray(FieldPathExpression.of("nested.obj"));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("nested.obj");
  }

  // $arrayToObject tests

  @Test
  void shouldRenderArrayToObjectFromKvPairs() {
    // MongoDB: {$arrayToObject: "$arr"} where arr contains [{k: "key", v: "value"}, ...]
    var expr = ObjectExpression.arrayToObject(FieldPathExpression.of("arr"));

    expr.render(context);

    String sql = context.toSql();
    // Should construct object from key-value pairs
    assertThat(sql).containsAnyOf("JSON_OBJECT", "JSON_ARRAYAGG", "JSON_TABLE");
  }

  // Operator type tests

  @Test
  void shouldReturnMergeObjectsOp() {
    var expr = ObjectExpression.mergeObjects(List.of(FieldPathExpression.of("x")));
    assertThat(expr.getOp()).isEqualTo(ObjectOp.MERGE_OBJECTS);
  }

  @Test
  void shouldReturnObjectToArrayOp() {
    var expr = ObjectExpression.objectToArray(FieldPathExpression.of("x"));
    assertThat(expr.getOp()).isEqualTo(ObjectOp.OBJECT_TO_ARRAY);
  }

  @Test
  void shouldReturnArrayToObjectOp() {
    var expr = ObjectExpression.arrayToObject(FieldPathExpression.of("x"));
    assertThat(expr.getOp()).isEqualTo(ObjectOp.ARRAY_TO_OBJECT);
  }

  // toString tests

  @Test
  void shouldProvideReadableToString() {
    var expr = ObjectExpression.mergeObjects(List.of(FieldPathExpression.of("obj")));
    assertThat(expr.toString()).contains("$mergeObjects");
  }
}
