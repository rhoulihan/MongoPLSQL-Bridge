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

class AccumulatorExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderSum() {
    var expr = AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("SUM(CAST(data.amount AS NUMBER))");
  }

  @Test
  void shouldRenderAvg() {
    var expr = AccumulatorExpression.avg(FieldPathExpression.of("price", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("AVG(CAST(data.price AS NUMBER))");
  }

  @Test
  void shouldRenderCount() {
    var expr = AccumulatorExpression.count();

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("COUNT(*)");
  }

  @Test
  void shouldRenderMin() {
    var expr = AccumulatorExpression.min(FieldPathExpression.of("score", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("MIN(CAST(data.score AS NUMBER))");
  }

  @Test
  void shouldRenderMax() {
    var expr = AccumulatorExpression.max(FieldPathExpression.of("score", JsonReturnType.NUMBER));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("MAX(CAST(data.score AS NUMBER))");
  }

  @Test
  void shouldRenderFirst() {
    // $first uses MIN() in GROUP BY context because Oracle's FIRST_VALUE requires OVER clause
    var expr = AccumulatorExpression.first(FieldPathExpression.of("name"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("MIN(data.name)");
  }

  @Test
  void shouldRenderLast() {
    // $last uses MAX() in GROUP BY context because Oracle's LAST_VALUE requires OVER clause
    var expr = AccumulatorExpression.last(FieldPathExpression.of("name"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("MAX(data.name)");
  }

  @Test
  void shouldRenderSumWithLiteral() {
    // {$sum: 1} counts documents
    var expr = AccumulatorExpression.sum(LiteralExpression.of(1));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("SUM(:1)");
    assertThat(context.getBindVariables()).containsExactly(1);
  }

  @Test
  void shouldReturnOp() {
    var expr = AccumulatorExpression.sum(FieldPathExpression.of("amount"));
    assertThat(expr.getOp()).isEqualTo(AccumulatorOp.SUM);
  }

  @Test
  void shouldReturnArgument() {
    var field = FieldPathExpression.of("amount");
    var expr = AccumulatorExpression.sum(field);
    assertThat(expr.getArgument()).isEqualTo(field);
  }

  @Test
  void shouldReturnNullArgumentForCount() {
    var expr = AccumulatorExpression.count();
    assertThat(expr.getArgument()).isNull();
  }

  @Test
  void shouldProvideReadableToStringForCount() {
    var expr = AccumulatorExpression.count();
    assertThat(expr.toString()).contains("COUNT(*)");
  }

  @Test
  void shouldProvideReadableToStringForSum() {
    var expr = AccumulatorExpression.sum(FieldPathExpression.of("amount"));
    assertThat(expr.toString()).contains("$sum");
  }

  @Test
  void shouldRenderPush() {
    var expr = AccumulatorExpression.push(FieldPathExpression.of("name"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_ARRAYAGG(data.name)");
  }

  @Test
  void shouldRenderPushWithNestedField() {
    var expr = AccumulatorExpression.push(FieldPathExpression.of("order.item"));

    expr.render(context);

    assertThat(context.toSql()).isEqualTo("JSON_ARRAYAGG(data.order.item)");
  }

  @Test
  void shouldRenderAddToSet() {
    var expr = AccumulatorExpression.addToSet(FieldPathExpression.of("category"));

    expr.render(context);

    // Uses LISTAGG workaround since Oracle doesn't support JSON_ARRAYAGG(DISTINCT ...)
    assertThat(context.toSql())
        .isEqualTo(
            "JSON_QUERY('[' || LISTAGG(DISTINCT '\"' || data.category || '\"',"
                + " ',') WITHIN GROUP (ORDER BY data.category) || ']', '$'"
                + " RETURNING CLOB)");
  }

  @Test
  void shouldRenderAddToSetWithNestedField() {
    var expr = AccumulatorExpression.addToSet(FieldPathExpression.of("metadata.tag"));

    expr.render(context);

    // Uses LISTAGG workaround since Oracle doesn't support JSON_ARRAYAGG(DISTINCT ...)
    assertThat(context.toSql())
        .isEqualTo(
            "JSON_QUERY('[' || LISTAGG(DISTINCT '\"' || data.metadata.tag || '\"',"
                + " ',') WITHIN GROUP (ORDER BY data.metadata.tag) || ']', '$'"
                + " RETURNING CLOB)");
  }

  @Test
  void shouldReturnPushOp() {
    var expr = AccumulatorExpression.push(FieldPathExpression.of("name"));
    assertThat(expr.getOp()).isEqualTo(AccumulatorOp.PUSH);
  }

  @Test
  void shouldReturnAddToSetOp() {
    var expr = AccumulatorExpression.addToSet(FieldPathExpression.of("name"));
    assertThat(expr.getOp()).isEqualTo(AccumulatorOp.ADD_TO_SET);
  }
}
