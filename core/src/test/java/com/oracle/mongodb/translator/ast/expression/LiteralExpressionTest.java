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

class LiteralExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderStringLiteralAsBindVariable() {
    var expr = LiteralExpression.of("active");

    expr.render(context);

    assertThat(context.toSql()).isEqualTo(":1");
    assertThat(context.getBindVariables()).containsExactly("active");
  }

  @Test
  void shouldRenderNumberLiteral() {
    var expr = LiteralExpression.of(42);

    expr.render(context);

    assertThat(context.toSql()).isEqualTo(":1");
    assertThat(context.getBindVariables()).containsExactly(42);
  }

  @Test
  void shouldRenderNullLiteral() {
    var expr = LiteralExpression.ofNull();

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("NULL");
    assertThat(context.getBindVariables()).isEmpty();
  }

  @Test
  void shouldRenderBooleanLiteral() {
    var expr = LiteralExpression.of(true);

    expr.render(context);

    assertThat(context.toSql()).isEqualTo(":1");
    assertThat(context.getBindVariables()).containsExactly(true);
  }

  @Test
  void shouldReturnValue() {
    var expr = LiteralExpression.of("test");

    assertThat(expr.getValue()).isEqualTo("test");
  }

  @Test
  void shouldIndicateIfNull() {
    var nullExpr = LiteralExpression.ofNull();
    var nonNullExpr = LiteralExpression.of("value");

    assertThat(nullExpr.isNull()).isTrue();
    assertThat(nonNullExpr.isNull()).isFalse();
  }

  @Test
  void shouldSupportEquality() {
    var expr1 = LiteralExpression.of("test");
    var expr2 = LiteralExpression.of("test");
    var expr3 = LiteralExpression.of("other");

    assertThat(expr1).isEqualTo(expr2);
    assertThat(expr1).isNotEqualTo(expr3);
  }
}
