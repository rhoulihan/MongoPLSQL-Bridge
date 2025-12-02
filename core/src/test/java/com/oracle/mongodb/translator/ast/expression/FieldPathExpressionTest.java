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

class FieldPathExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderSimpleFieldPath() {
    var expr = FieldPathExpression.of("status");

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.status')");
  }

  @Test
  void shouldRenderNestedFieldPath() {
    var expr = FieldPathExpression.of("customer.address.city");

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.customer.address.city')");
  }

  @Test
  void shouldHandleDollarPrefixedPath() {
    var expr = FieldPathExpression.of("$status");

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.status')");
  }

  @ParameterizedTest
  @CsvSource({
    "name, $.name",
    "$name, $.name",
    "user.email, $.user.email",
    "$user.email, $.user.email"
  })
  void shouldConvertToJsonPath(String input, String expectedPath) {
    var expr = FieldPathExpression.of(input);

    assertThat(expr.getJsonPath()).isEqualTo(expectedPath);
  }

  @Test
  void shouldRenderWithReturningClauseForNumbers() {
    var expr = FieldPathExpression.of("amount", JsonReturnType.NUMBER);

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.amount' RETURNING NUMBER)");
  }

  @Test
  void shouldRenderWithCustomDataColumn() {
    var expr = FieldPathExpression.of("name", null, "doc");

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(doc, '$.name')");
  }

  @Test
  void shouldSupportEquality() {
    var expr1 = FieldPathExpression.of("status");
    var expr2 = FieldPathExpression.of("status");
    var expr3 = FieldPathExpression.of("other");

    assertThat(expr1).isEqualTo(expr2);
    assertThat(expr1).isNotEqualTo(expr3);
  }

  @Test
  void shouldSupportHashCode() {
    var expr1 = FieldPathExpression.of("status");
    var expr2 = FieldPathExpression.of("status");

    assertThat(expr1.hashCode()).isEqualTo(expr2.hashCode());
  }

  @Test
  void shouldHandleEqualityWithDifferentReturnTypes() {
    var expr1 = FieldPathExpression.of("amount", JsonReturnType.NUMBER);
    var expr2 = FieldPathExpression.of("amount", JsonReturnType.VARCHAR);

    assertThat(expr1).isNotEqualTo(expr2);
  }

  @Test
  void shouldHandleEqualityWithDifferentDataColumns() {
    var expr1 = FieldPathExpression.of("name", null, "data");
    var expr2 = FieldPathExpression.of("name", null, "doc");

    assertThat(expr1).isNotEqualTo(expr2);
  }

  @Test
  void shouldHandleSelfEquality() {
    var expr = FieldPathExpression.of("status");
    assertThat(expr).isEqualTo(expr);
  }

  @Test
  void shouldNotEqualNull() {
    var expr = FieldPathExpression.of("status");
    assertThat(expr).isNotEqualTo(null);
  }

  @Test
  void shouldNotEqualDifferentClass() {
    var expr = FieldPathExpression.of("status");
    assertThat(expr).isNotEqualTo("status");
  }

  @Test
  void shouldRenderVirtualField() {
    // When a field is defined via $addFields, it should inline the expression
    // Use inlineValues=true so literals are rendered directly instead of as bind variables
    var inlineContext = new DefaultSqlGenerationContext(true);
    inlineContext.registerVirtualField("total", LiteralExpression.of(100));

    var expr = FieldPathExpression.of("total");
    expr.render(inlineContext);

    assertThat(inlineContext.toSql()).isEqualTo("100");
  }

  @Test
  void shouldRenderLookupFieldPath() {
    // When accessing a field from a $lookup result, redirect to joined table
    context.registerLookupTableAlias("customer", "customers_1");

    var expr = FieldPathExpression.of("customer.tier");
    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(customers_1.data, '$.tier')");
  }

  @Test
  void shouldRenderLookupFieldPathWithReturnType() {
    context.registerLookupTableAlias("inventory", "inv_1");

    var expr = FieldPathExpression.of("inventory.quantity", JsonReturnType.NUMBER);
    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(inv_1.data, '$.quantity' RETURNING NUMBER)");
  }

  @Test
  void shouldRenderLookupFieldPathForRootAccess() {
    // Access the lookup result root (just the alias, no nested field)
    context.registerLookupTableAlias("order", "orders_1");

    var expr = FieldPathExpression.of("order");
    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_VALUE(orders_1.data, '$')");
  }

  @Test
  void shouldRenderWithBaseTableAliasQualified() {
    var contextWithAlias = new DefaultSqlGenerationContext(false, null, "base");

    var expr = FieldPathExpression.of("status");
    expr.render(contextWithAlias);

    assertThat(contextWithAlias.toSql()).isEqualTo("JSON_VALUE(base.data, '$.status')");
  }

  @Test
  void shouldNotQualifyCustomDataColumn() {
    // Custom data column should not be qualified with base alias
    var contextWithAlias = new DefaultSqlGenerationContext(false, null, "base");

    var expr = FieldPathExpression.of("name", null, "doc");
    expr.render(contextWithAlias);

    // doc is not qualified because it's not "data"
    assertThat(contextWithAlias.toSql()).isEqualTo("JSON_VALUE(doc, '$.name')");
  }

  @Test
  void shouldHandlePathWithLeadingDot() {
    // Path like "$.field" after $ is removed becomes ".field"
    var expr = FieldPathExpression.of("$.name");

    assertThat(expr.getJsonPath()).isEqualTo("$.name");
  }

  @Test
  void shouldReturnPath() {
    var expr = FieldPathExpression.of("$status");
    assertThat(expr.getPath()).isEqualTo("$status");
  }

  @Test
  void shouldReturnDataColumn() {
    var expr = FieldPathExpression.of("name", null, "document");
    assertThat(expr.getDataColumn()).isEqualTo("document");
  }

  @Test
  void shouldReturnDefaultDataColumn() {
    var expr = FieldPathExpression.of("name");
    assertThat(expr.getDataColumn()).isEqualTo("data");
  }

  @Test
  void shouldProvideReadableToString() {
    var expr = FieldPathExpression.of("status");
    assertThat(expr.toString()).isEqualTo("FieldPath($status)");
  }
}
