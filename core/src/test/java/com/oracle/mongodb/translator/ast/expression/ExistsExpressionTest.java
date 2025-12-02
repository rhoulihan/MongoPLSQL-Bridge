/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;

class ExistsExpressionTest {

  @Test
  void shouldRenderExistsTrue() {
    var context = new DefaultSqlGenerationContext();
    var expr = new ExistsExpression("tags", true);

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_EXISTS(data, '$.tags')");
  }

  @Test
  void shouldRenderExistsFalse() {
    var context = new DefaultSqlGenerationContext();
    var expr = new ExistsExpression("tags", false);

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("NOT JSON_EXISTS(data, '$.tags')");
  }

  @Test
  void shouldRenderWithBaseTableAlias() {
    var context = new DefaultSqlGenerationContext(false, null, "base");
    var expr = new ExistsExpression("status", true);

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_EXISTS(base.data, '$.status')");
  }

  @Test
  void shouldRenderNestedFieldPath() {
    var context = new DefaultSqlGenerationContext();
    var expr = new ExistsExpression("address.city", true);

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_EXISTS(data, '$.address.city')");
  }

  @Test
  void shouldThrowOnNullFieldPath() {
    assertThatThrownBy(() -> new ExistsExpression(null, true))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("fieldPath must not be null");
  }

  @Test
  void shouldReturnFieldPath() {
    var expr = new ExistsExpression("myField", true);
    assertThat(expr.getFieldPath()).isEqualTo("myField");
  }

  @Test
  void shouldReturnExistsValue() {
    var exprTrue = new ExistsExpression("field", true);
    var exprFalse = new ExistsExpression("field", false);

    assertThat(exprTrue.isExists()).isTrue();
    assertThat(exprFalse.isExists()).isFalse();
  }

  @Test
  void shouldImplementEquals() {
    var expr1 = new ExistsExpression("field", true);
    var expr2 = new ExistsExpression("field", true);
    var expr3 = new ExistsExpression("field", false);
    var expr4 = new ExistsExpression("other", true);

    assertThat(expr1).isEqualTo(expr2);
    assertThat(expr1).isNotEqualTo(expr3);
    assertThat(expr1).isNotEqualTo(expr4);
    assertThat(expr1).isNotEqualTo(null);
    assertThat(expr1).isNotEqualTo("string");
  }

  @Test
  void shouldImplementHashCode() {
    var expr1 = new ExistsExpression("field", true);
    var expr2 = new ExistsExpression("field", true);

    assertThat(expr1.hashCode()).isEqualTo(expr2.hashCode());
  }

  @Test
  void shouldImplementToString() {
    var expr = new ExistsExpression("tags", true);

    assertThat(expr.toString()).isEqualTo("Exists(tags, true)");
  }
}
