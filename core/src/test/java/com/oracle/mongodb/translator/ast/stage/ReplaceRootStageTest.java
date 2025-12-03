/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.ObjectExpression;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for ReplaceRootStage. */
class ReplaceRootStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderReplaceRootWithFieldPath() {
    // MongoDB: {$replaceRoot: {newRoot: "$subdocument"}}
    // Should extract the subdocument as the new root
    var stage = new ReplaceRootStage(FieldPathExpression.of("subdocument"));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).contains("subdocument");
  }

  @Test
  void shouldRenderReplaceRootWithNestedField() {
    // MongoDB: {$replaceRoot: {newRoot: "$nested.document"}}
    var stage = new ReplaceRootStage(FieldPathExpression.of("nested.document"));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("nested.document");
  }

  @Test
  void shouldRenderReplaceRootWithMergeObjects() {
    // MongoDB: {$replaceRoot: {newRoot: {$mergeObjects: ["$defaults", "$overrides"]}}}
    var stage =
        new ReplaceRootStage(
            ObjectExpression.mergeObjects(
                List.of(FieldPathExpression.of("defaults"), FieldPathExpression.of("overrides"))));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).containsAnyOf("JSON_MERGEPATCH", "defaults", "overrides");
  }

  @Test
  void shouldReturnCorrectOperatorName() {
    var stage = new ReplaceRootStage(FieldPathExpression.of("doc"));
    assertThat(stage.getOperatorName()).isEqualTo("$replaceRoot");
  }

  @Test
  void shouldProvideReadableToString() {
    var stage = new ReplaceRootStage(FieldPathExpression.of("subdoc"));
    assertThat(stage.toString()).contains("ReplaceRootStage");
  }

  @Test
  void shouldReturnNewRootExpression() {
    var expr = FieldPathExpression.of("doc");
    var stage = new ReplaceRootStage(expr);
    assertThat(stage.getNewRoot()).isEqualTo(expr);
  }

  @Test
  void shouldRenderReplaceRootWithLiteralObject() {
    // MongoDB: {$replaceRoot: {newRoot: {name: "default", value: 0}}}
    // Using literal values to create a new document
    var stage = new ReplaceRootStage(LiteralExpression.of("{\"name\": \"default\", \"value\": 0}"));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
  }

  @Test
  void shouldRenderReplaceRootWithComputedExpression() {
    // $replaceRoot with a computed expression combining multiple fields
    var stage =
        new ReplaceRootStage(
            ObjectExpression.mergeObjects(
                List.of(
                    FieldPathExpression.of("profile"),
                    FieldPathExpression.of("settings"),
                    FieldPathExpression.of("permissions"))));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).containsAnyOf("profile", "settings", "permissions");
  }
}
