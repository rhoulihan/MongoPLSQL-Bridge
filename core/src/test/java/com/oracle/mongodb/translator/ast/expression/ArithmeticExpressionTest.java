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
        .contains("'$.a'")
        .contains("'$.b'")
        .contains("'$.c'")
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

  @Test
  void shouldRenderDateSubtractionAsMilliseconds() {
    // COMPLEX015: When subtracting two explicitly typed date fields,
    // MongoDB returns milliseconds. Use EXTRACT to convert INTERVAL to ms.
    var expr =
        ArithmeticExpression.subtract(
            FieldPathExpression.of("resolvedAt", JsonReturnType.TIMESTAMP),
            FieldPathExpression.of("createdAt", JsonReturnType.TIMESTAMP));

    expr.render(context);

    String sql = context.toSql();
    // Should use EXTRACT to convert INTERVAL to milliseconds
    assertThat(sql)
        .as("Date subtraction should use EXTRACT for DAY")
        .containsIgnoringCase("EXTRACT(DAY FROM");
    assertThat(sql)
        .as("Date subtraction should use EXTRACT for HOUR")
        .containsIgnoringCase("EXTRACT(HOUR FROM");
    // Should access plain date path (Oracle stores dates as ISO strings)
    assertThat(sql)
        .as("Should access resolvedAt path")
        .contains("$.resolvedAt'");
    assertThat(sql)
        .as("Should access createdAt path")
        .contains("$.createdAt'");
    // Should use TIMESTAMP for time precision
    assertThat(sql)
        .as("Should use TIMESTAMP")
        .containsIgnoringCase("RETURNING TIMESTAMP");
  }

  @Test
  void shouldUseRuntimeTypeDetectionForUntypedFieldSubtraction() {
    // When subtracting two untyped fields, use runtime detection:
    // CASE WHEN both are numbers THEN numeric ELSE date END
    var expr =
        ArithmeticExpression.subtract(
            FieldPathExpression.of("field1"),
            FieldPathExpression.of("field2"));

    expr.render(context);

    String sql = context.toSql();
    // Should use CASE for runtime type detection
    assertThat(sql)
        .as("Should use CASE for runtime type detection")
        .containsIgnoringCase("CASE WHEN");
    // Should check if values are numeric
    assertThat(sql)
        .as("Should check for numeric type")
        .containsIgnoringCase("RETURNING NUMBER");
    // Should have date subtraction path with EXTRACT for milliseconds
    assertThat(sql)
        .as("Should use EXTRACT for date conversion")
        .containsIgnoringCase("EXTRACT(DAY FROM");
    // Date path uses plain paths (Oracle stores dates as ISO strings)
    assertThat(sql)
        .as("Should use TIMESTAMP for date subtraction")
        .containsIgnoringCase("RETURNING TIMESTAMP");
  }

  @Test
  void shouldUseNumericSubtractionForExplicitlyTypedNumbers() {
    // When both operands are explicitly NUMBER typed, use simple subtraction
    var expr =
        ArithmeticExpression.subtract(
            FieldPathExpression.of("amount", JsonReturnType.NUMBER),
            FieldPathExpression.of("discount", JsonReturnType.NUMBER));

    expr.render(context);

    String sql = context.toSql();
    // Should NOT use date detection - just regular subtraction
    assertThat(sql)
        .as("Should not use CASE for explicitly typed numbers")
        .doesNotContainIgnoringCase("CASE WHEN");
    assertThat(sql)
        .as("Should not convert to milliseconds for numbers")
        .doesNotContainIgnoringCase("86400000");
  }

  @Test
  void shouldUsePlainPathForDateSubtraction() {
    // COMPLEX015: Oracle stores dates as plain ISO strings (not Extended JSON)
    // Use plain path with TIMESTAMP return type for proper time precision
    var expr =
        ArithmeticExpression.subtract(
            FieldPathExpression.of("resolvedAt", JsonReturnType.TIMESTAMP),
            FieldPathExpression.of("createdAt", JsonReturnType.TIMESTAMP));

    expr.render(context);

    String sql = context.toSql();
    // Should access plain date paths (Oracle stores dates as ISO strings)
    assertThat(sql)
        .as("Should access plain resolvedAt path")
        .contains("$.resolvedAt'");
    assertThat(sql)
        .as("Should access plain createdAt path")
        .contains("$.createdAt'");
    // Should use TIMESTAMP for time precision (EXTRACT handles interval)
    assertThat(sql)
        .as("Should use TIMESTAMP for time precision")
        .containsIgnoringCase("RETURNING TIMESTAMP");
  }
}
