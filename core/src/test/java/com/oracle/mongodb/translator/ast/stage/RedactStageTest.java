/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.ConditionalExpression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RedactStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldCreateWithExpression() {
    var expr = LiteralExpression.of("$$DESCEND");
    var stage = new RedactStage(expr);

    assertThat(stage.getExpression()).isEqualTo(expr);
  }

  @Test
  void shouldThrowOnNullExpression() {
    assertThatNullPointerException()
        .isThrownBy(() -> new RedactStage(null))
        .withMessageContaining("expression");
  }

  @Test
  void shouldReturnCorrectOperatorName() {
    var stage = new RedactStage(LiteralExpression.of("$$PRUNE"));

    assertThat(stage.getOperatorName()).isEqualTo("$redact");
  }

  @Test
  void shouldRenderSimplePruneCondition() {
    // {$redact: {$cond: {if: {$eq: ["$level", 5]}, then: "$$PRUNE", else: "$$DESCEND"}}}
    var condition =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("level"), LiteralExpression.of(5));
    var expr =
        ConditionalExpression.cond(
            condition, LiteralExpression.of("$$PRUNE"), LiteralExpression.of("$$DESCEND"));
    var stage = new RedactStage(expr);

    stage.render(context);

    // $redact is complex and will generate conditional filtering SQL
    String sql = context.toSql();
    assertThat(sql).contains("CASE WHEN");
  }

  @Test
  void shouldProvideReadableToString() {
    var stage = new RedactStage(LiteralExpression.of("$$KEEP"));

    assertThat(stage.toString()).contains("RedactStage").contains("KEEP");
  }
}
