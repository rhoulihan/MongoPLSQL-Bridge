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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ComparisonExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @ParameterizedTest
  @CsvSource({"EQ, =", "NE, <>", "GT, >", "GTE, >=", "LT, <", "LTE, <="})
  void shouldRenderComparisonOperator(ComparisonOp op, String expectedSql) {
    var expr =
        new ComparisonExpression(
            op, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    expr.render(context);

    assertThat(context.toSql()).contains(expectedSql);
  }

  @Test
  void shouldRenderFieldPathEquality() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("data.status = :1");
    assertThat(context.getBindVariables()).containsExactly("active");
  }

  @Test
  void shouldRenderNumericComparison() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.GT,
            FieldPathExpression.of("age", JsonReturnType.NUMBER),
            LiteralExpression.of(21));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("CAST(data.age AS NUMBER) > :1");
    assertThat(context.getBindVariables()).containsExactly(21);
  }

  @Test
  void shouldRenderNestedFieldComparison() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("customer.address.city"),
            LiteralExpression.of("New York"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("data.customer.address.city = :1");
  }

  @Test
  void shouldHandleNullEqualityComparison() {
    // For FieldPathExpression, uses JSON_EXISTS to properly handle JSON null vs missing field
    var expr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("deletedAt"), LiteralExpression.ofNull());

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("NOT JSON_EXISTS(data, '$.deletedAt?(@ != null)')");
  }

  @Test
  void shouldHandleNullInequalityComparison() {
    // For FieldPathExpression, uses JSON_EXISTS to properly handle JSON null vs missing field
    var expr =
        new ComparisonExpression(
            ComparisonOp.NE, FieldPathExpression.of("deletedAt"), LiteralExpression.ofNull());

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_EXISTS(data, '$.deletedAt?(@ != null)')");
  }

  @Test
  void shouldReturnOperator() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.GT, FieldPathExpression.of("amount"), LiteralExpression.of(100));

    assertThat(expr.getOp()).isEqualTo(ComparisonOp.GT);
  }

  @Test
  void shouldReturnLeftExpression() {
    var left = FieldPathExpression.of("status");
    var expr = new ComparisonExpression(ComparisonOp.EQ, left, LiteralExpression.of("active"));

    assertThat(expr.getLeft()).isEqualTo(left);
  }

  @Test
  void shouldReturnRightExpression() {
    var right = LiteralExpression.of("active");
    var expr = new ComparisonExpression(ComparisonOp.EQ, FieldPathExpression.of("status"), right);

    assertThat(expr.getRight()).isEqualTo(right);
  }

  @Test
  void shouldRenderLessThanOrEqual() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.LTE,
            FieldPathExpression.of("price", JsonReturnType.NUMBER),
            LiteralExpression.of(99.99));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("CAST(data.price AS NUMBER) <= :1");
  }

  @Test
  void shouldProvideReadableToString() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    assertThat(expr.toString()).contains("EQ");
  }

  @Test
  void shouldRenderInOperator() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.IN,
            FieldPathExpression.of("status"),
            LiteralExpression.of(java.util.List.of("active", "pending", "approved")));

    expr.render(context);

    assertThat(context.toSql()).contains("IN (").contains(":1, :2, :3");
    assertThat(context.getBindVariables()).containsExactly("active", "pending", "approved");
  }

  @Test
  void shouldRenderNinOperator() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.NIN,
            FieldPathExpression.of("status"),
            LiteralExpression.of(java.util.List.of("deleted", "archived")));

    expr.render(context);

    assertThat(context.toSql()).contains("NOT IN (").contains(":1, :2");
    assertThat(context.getBindVariables()).containsExactly("deleted", "archived");
  }

  @Test
  void shouldThrowOnInvalidNullComparison() {
    var expr =
        new ComparisonExpression(
            ComparisonOp.GT, FieldPathExpression.of("amount"), LiteralExpression.ofNull());

    assertThat(org.assertj.core.api.Assertions.catchThrowable(() -> expr.render(context)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Invalid NULL comparison");
  }

  @Test
  void shouldImplementEquals() {
    var expr1 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));
    var expr2 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));
    var expr3 =
        new ComparisonExpression(
            ComparisonOp.NE, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    assertThat(expr1).isEqualTo(expr2);
    assertThat(expr1).isNotEqualTo(expr3);
    assertThat(expr1).isNotEqualTo(null);
    assertThat(expr1).isNotEqualTo("not a comparison");
    assertThat(expr1).isEqualTo(expr1); // same instance
  }

  @Test
  void shouldImplementHashCode() {
    var expr1 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));
    var expr2 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    assertThat(expr1.hashCode()).isEqualTo(expr2.hashCode());
  }
}
