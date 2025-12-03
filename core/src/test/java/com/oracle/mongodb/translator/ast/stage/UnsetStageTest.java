/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for UnsetStage. */
class UnsetStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderUnsetSingleField() {
    // MongoDB: {$unset: "fieldToRemove"}
    // Should use JSON_TRANSFORM or similar to remove field
    var stage = new UnsetStage(List.of("fieldToRemove"));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).containsAnyOf("JSON_TRANSFORM", "REMOVE", "fieldToRemove");
  }

  @Test
  void shouldRenderUnsetMultipleFields() {
    // MongoDB: {$unset: ["field1", "field2", "field3"]}
    var stage = new UnsetStage(List.of("field1", "field2", "field3"));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
  }

  @Test
  void shouldRenderUnsetNestedField() {
    // MongoDB: {$unset: "parent.child"}
    var stage = new UnsetStage(List.of("parent.child"));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("parent.child");
  }

  @Test
  void shouldReturnCorrectOperatorName() {
    var stage = new UnsetStage(List.of("field"));
    assertThat(stage.getOperatorName()).isEqualTo("$unset");
  }

  @Test
  void shouldProvideReadableToString() {
    var stage = new UnsetStage(List.of("field1", "field2"));
    assertThat(stage.toString()).contains("UnsetStage");
    assertThat(stage.toString()).containsAnyOf("field1", "field2");
  }

  @Test
  void shouldReturnFieldsToUnset() {
    var fields = List.of("a", "b", "c");
    var stage = new UnsetStage(fields);
    assertThat(stage.getFields()).containsExactly("a", "b", "c");
  }

  @Test
  void shouldRenderUnsetMultipleNestedFields() {
    // MongoDB: {$unset: ["parent.child1", "parent.child2", "other.nested.field"]}
    var stage = new UnsetStage(List.of("parent.child1", "parent.child2", "other.nested.field"));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).containsAnyOf("parent.child1", "parent.child2", "other.nested.field");
  }

  @Test
  void shouldRenderUnsetDeeplyNestedField() {
    // MongoDB: {$unset: "level1.level2.level3.level4"}
    var stage = new UnsetStage(List.of("level1.level2.level3.level4"));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("level1.level2.level3.level4");
  }
}
