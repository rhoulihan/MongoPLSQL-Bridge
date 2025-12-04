/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.JsonReturnType;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MatchStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderWhereClause() {
    var filter =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));
    var stage = new MatchStage(filter);

    stage.render(context);

    assertThat(context.toSql()).isEqualTo("WHERE data.status = :1");
  }

  @Test
  void shouldRenderComplexFilter() {
    var filter =
        new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(
                    ComparisonOp.EQ,
                    FieldPathExpression.of("status"),
                    LiteralExpression.of("active")),
                new ComparisonExpression(
                    ComparisonOp.GT,
                    FieldPathExpression.of("amount", JsonReturnType.NUMBER),
                    LiteralExpression.of(100))));
    var stage = new MatchStage(filter);

    stage.render(context);

    assertThat(context.toSql())
        .startsWith("WHERE ")
        .contains("AND")
        .contains("data.status")
        .contains("data.amount");
  }

  @Test
  void shouldReturnOperatorName() {
    var stage =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("x"), LiteralExpression.of(1)));

    assertThat(stage.getOperatorName()).isEqualTo("$match");
  }

  @Test
  void shouldReturnFilterExpression() {
    var filter =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));
    var stage = new MatchStage(filter);

    assertThat(stage.getFilter()).isEqualTo(filter);
  }

  @Test
  void shouldRenderOrCondition() {
    var filter =
        new LogicalExpression(
            LogicalOp.OR,
            List.of(
                new ComparisonExpression(
                    ComparisonOp.EQ, FieldPathExpression.of("type"), LiteralExpression.of("A")),
                new ComparisonExpression(
                    ComparisonOp.EQ, FieldPathExpression.of("type"), LiteralExpression.of("B"))));
    var stage = new MatchStage(filter);

    stage.render(context);

    assertThat(context.toSql()).startsWith("WHERE ").contains("OR");
  }

  @Test
  void shouldProvideReadableToString() {
    var stage =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));

    assertThat(stage.toString()).contains("MatchStage");
  }
}
