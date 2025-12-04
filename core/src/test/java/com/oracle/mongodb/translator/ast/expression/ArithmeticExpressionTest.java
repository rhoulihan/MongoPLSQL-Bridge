/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ArithmeticExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderAddition() {
    var expr =
        ArithmeticExpression.add(
            FieldPathExpression.of("price", JsonReturnType.NUMBER),
            FieldPathExpression.of("tax", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql()).startsWith("(").endsWith(")").contains(" + ");
  }

  @Test
  void shouldRenderSubtraction() {
    var expr =
        ArithmeticExpression.subtract(
            FieldPathExpression.of("total", JsonReturnType.NUMBER),
            FieldPathExpression.of("discount", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql()).contains(" - ");
  }

  @Test
  void shouldRenderMultiplication() {
    var expr =
        ArithmeticExpression.multiply(
            FieldPathExpression.of("qty", JsonReturnType.NUMBER),
            FieldPathExpression.of("price", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql()).contains(" * ");
  }

  @Test
  void shouldRenderDivision() {
    var expr =
        ArithmeticExpression.divide(
            FieldPathExpression.of("total", JsonReturnType.NUMBER),
            FieldPathExpression.of("count", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql()).contains(" / ");
  }

  @Test
  void shouldRenderModulo() {
    var expr =
        ArithmeticExpression.mod(
            FieldPathExpression.of("value", JsonReturnType.NUMBER), LiteralExpression.of(10));

    expr.render(context);

    assertThat(context.toSql()).startsWith("MOD(").contains(", ");
  }

  @Test
  void shouldRenderMultipleOperands() {
    var expr =
        ArithmeticExpression.add(
            FieldPathExpression.of("a", JsonReturnType.NUMBER),
            FieldPathExpression.of("b", JsonReturnType.NUMBER),
            FieldPathExpression.of("c", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql())
        .contains("data.a")
        .contains("data.b")
        .contains("data.c")
        .contains(" + ");
  }

  @Test
  void shouldRenderWithLiterals() {
    var expr =
        ArithmeticExpression.multiply(
            FieldPathExpression.of("price", JsonReturnType.NUMBER), LiteralExpression.of(1.1));

    expr.render(context);

    assertThat(context.toSql()).contains(" * ");
    assertThat(context.getBindVariables()).contains(1.1);
  }

  @Test
  void shouldRejectSingleOperand() {
    assertThatThrownBy(
            () ->
                new ArithmeticExpression(
                    ArithmeticOp.ADD, java.util.List.of(LiteralExpression.of(1))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least 2 operands");
  }

  @Test
  void shouldReturnOp() {
    var expr = ArithmeticExpression.add(LiteralExpression.of(1), LiteralExpression.of(2));
    assertThat(expr.getOp()).isEqualTo(ArithmeticOp.ADD);
  }

  @Test
  void shouldReturnOperands() {
    var left = LiteralExpression.of(1);
    var right = LiteralExpression.of(2);
    var expr = ArithmeticExpression.add(left, right);

    assertThat(expr.getOperands()).containsExactly(left, right);
  }

  @Test
  void shouldProvideReadableToString() {
    var expr = ArithmeticExpression.add(LiteralExpression.of(1), LiteralExpression.of(2));
    assertThat(expr.toString()).contains("$add");
  }

  @Test
  void shouldMapArithmeticOpFromMongo() {
    assertThat(ArithmeticOp.fromMongo("$add")).isEqualTo(ArithmeticOp.ADD);
    assertThat(ArithmeticOp.fromMongo("$subtract")).isEqualTo(ArithmeticOp.SUBTRACT);
    assertThat(ArithmeticOp.fromMongo("$multiply")).isEqualTo(ArithmeticOp.MULTIPLY);
    assertThat(ArithmeticOp.fromMongo("$divide")).isEqualTo(ArithmeticOp.DIVIDE);
    assertThat(ArithmeticOp.fromMongo("$mod")).isEqualTo(ArithmeticOp.MOD);
  }

  @Test
  void shouldDetectArithmeticOperator() {
    assertThat(ArithmeticOp.isArithmetic("$add")).isTrue();
    assertThat(ArithmeticOp.isArithmetic("$subtract")).isTrue();
    assertThat(ArithmeticOp.isArithmetic("$eq")).isFalse();
  }
}
