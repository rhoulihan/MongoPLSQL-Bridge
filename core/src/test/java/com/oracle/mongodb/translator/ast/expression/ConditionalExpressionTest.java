/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.expression.ConditionalExpression.ConditionalType;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConditionalExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderCondExpression() {
    // { $cond: [ { $gt: ["$qty", 250] }, 30, 20 ] }
    var condition =
        new ComparisonExpression(
            ComparisonOp.GT,
            FieldPathExpression.of("qty", JsonReturnType.NUMBER),
            LiteralExpression.of(250));
    var expr =
        ConditionalExpression.cond(condition, LiteralExpression.of(30), LiteralExpression.of(20));

    expr.render(context);

    assertThat(context.toSql())
        .startsWith("CASE WHEN ")
        .contains(" THEN ")
        .contains(" ELSE ")
        .endsWith(" END");
  }

  @Test
  void shouldRenderIfNullExpression() {
    // { $ifNull: [ "$description", "No description" ] }
    var expr =
        ConditionalExpression.ifNull(
            FieldPathExpression.of("description"), LiteralExpression.of("No description"));

    expr.render(context);

    assertThat(context.toSql()).startsWith("NVL(").contains("data.description").endsWith(")");
  }

  @Test
  void shouldRenderNestedConditional() {
    // { $cond: [ condition1, { $cond: [ condition2, a, b ] }, c ] }
    var innerCond =
        ConditionalExpression.cond(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("type"), LiteralExpression.of("A")),
            LiteralExpression.of(1),
            LiteralExpression.of(2));

    var outerCond =
        ConditionalExpression.cond(
            new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("qty", JsonReturnType.NUMBER),
                LiteralExpression.of(100)),
            innerCond,
            LiteralExpression.of(0));

    outerCond.render(context);

    assertThat(context.toSql()).contains("CASE WHEN").contains("THEN CASE WHEN"); // nested
  }

  @Test
  void shouldRenderCondWithFieldExpressions() {
    var expr =
        ConditionalExpression.cond(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")),
            FieldPathExpression.of("activePrice", JsonReturnType.NUMBER),
            FieldPathExpression.of("defaultPrice", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql())
        .contains("data.status")
        .contains("data.activePrice")
        .contains("data.defaultPrice");
  }

  @Test
  void shouldReturnCondType() {
    var expr =
        ConditionalExpression.cond(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("x"), LiteralExpression.of(1)),
            LiteralExpression.of(1),
            LiteralExpression.of(0));

    assertThat(expr.getType()).isEqualTo(ConditionalType.COND);
  }

  @Test
  void shouldReturnIfNullType() {
    var expr =
        ConditionalExpression.ifNull(
            FieldPathExpression.of("field"), LiteralExpression.of("default"));

    assertThat(expr.getType()).isEqualTo(ConditionalType.IF_NULL);
  }

  @Test
  void shouldReturnCondition() {
    var condition =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("x"), LiteralExpression.of(1));
    var expr =
        ConditionalExpression.cond(condition, LiteralExpression.of(1), LiteralExpression.of(0));

    assertThat(expr.getCondition()).isEqualTo(condition);
  }

  @Test
  void shouldReturnNullConditionForIfNull() {
    var expr =
        ConditionalExpression.ifNull(
            FieldPathExpression.of("field"), LiteralExpression.of("default"));

    assertThat(expr.getCondition()).isNull();
  }

  @Test
  void shouldProvideReadableToStringForCond() {
    var expr =
        ConditionalExpression.cond(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("x"), LiteralExpression.of(1)),
            LiteralExpression.of(1),
            LiteralExpression.of(0));

    assertThat(expr.toString()).contains("Cond");
  }

  @Test
  void shouldProvideReadableToStringForIfNull() {
    var expr =
        ConditionalExpression.ifNull(
            FieldPathExpression.of("field"), LiteralExpression.of("default"));

    assertThat(expr.toString()).contains("IfNull");
  }

  @Test
  void shouldMapConditionalTypeFromMongo() {
    assertThat(ConditionalType.fromMongo("$cond")).isEqualTo(ConditionalType.COND);
    assertThat(ConditionalType.fromMongo("$ifNull")).isEqualTo(ConditionalType.IF_NULL);
  }

  @Test
  void shouldThrowForUnknownConditionalOperator() {
    assertThatThrownBy(() -> ConditionalType.fromMongo("$unknown"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldDetectConditionalOperator() {
    assertThat(ConditionalType.isConditional("$cond")).isTrue();
    assertThat(ConditionalType.isConditional("$ifNull")).isTrue();
    assertThat(ConditionalType.isConditional("$eq")).isFalse();
  }
}
