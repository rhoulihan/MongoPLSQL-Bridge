/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CompoundIdExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext(true);
  }

  @Test
  void shouldCreateWithFields() {
    var fields = new LinkedHashMap<String, Expression>();
    fields.put("category", FieldPathExpression.of("category"));
    fields.put("brand", FieldPathExpression.of("brand"));

    var expr = new CompoundIdExpression(fields);

    assertThat(expr.getFields()).hasSize(2);
    assertThat(expr.getFields()).containsKey("category");
    assertThat(expr.getFields()).containsKey("brand");
  }

  @Test
  void shouldThrowOnNullFields() {
    assertThatNullPointerException()
        .isThrownBy(() -> new CompoundIdExpression(null))
        .withMessageContaining("fields");
  }

  @Test
  void shouldThrowOnEmptyFields() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new CompoundIdExpression(Map.of()))
        .withMessageContaining("at least one field");
  }

  @Test
  void shouldRenderSingleField() {
    var fields = new LinkedHashMap<String, Expression>();
    fields.put("category", FieldPathExpression.of("category"));

    var expr = new CompoundIdExpression(fields);
    expr.render(context);

    assertThat(context.toSql()).contains("$.category");
  }

  @Test
  void shouldRenderMultipleFields() {
    var fields = new LinkedHashMap<String, Expression>();
    fields.put("category", FieldPathExpression.of("category"));
    fields.put("brand", FieldPathExpression.of("brand"));

    var expr = new CompoundIdExpression(fields);
    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("$.category");
    assertThat(sql).contains("$.brand");
    assertThat(sql).contains(", ");
  }

  @Test
  void shouldRenderWithAliases() {
    var fields = new LinkedHashMap<String, Expression>();
    fields.put("cat", FieldPathExpression.of("category"));
    fields.put("br", FieldPathExpression.of("brand"));

    var expr = new CompoundIdExpression(fields);
    expr.renderWithAliases(context);

    String sql = context.toSql();
    assertThat(sql).contains("AS cat");
    assertThat(sql).contains("AS br");
  }

  @Test
  void shouldProvideReadableToString() {
    var fields = new LinkedHashMap<String, Expression>();
    fields.put("category", FieldPathExpression.of("category"));
    fields.put("brand", FieldPathExpression.of("brand"));

    var expr = new CompoundIdExpression(fields);

    assertThat(expr.toString()).contains("CompoundId").contains("category").contains("brand");
  }

  @Test
  void shouldReturnUnmodifiableFields() {
    var fields = new LinkedHashMap<String, Expression>();
    fields.put("category", FieldPathExpression.of("category"));

    var expr = new CompoundIdExpression(fields);

    assertThat(expr.getFields()).isUnmodifiable();
  }
}
