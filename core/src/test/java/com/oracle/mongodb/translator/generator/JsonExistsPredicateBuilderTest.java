/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.ExistsExpression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.InExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for JsonExistsPredicateBuilder.
 */
class JsonExistsPredicateBuilderTest {

  @Test
  void shouldRenderNinOperatorWithNegatedInSyntax() {
    // Given: $nin expression with string values
    var inExpr = new InExpression(
        FieldPathExpression.of("region"),
        List.of("north", "south"),
        true  // negated = true for $nin
    );

    var ctx = new DefaultSqlGenerationContext(true);  // inline values
    var builder = new JsonExistsPredicateBuilder(ctx);

    // When: render the predicate
    builder.render(inExpr);

    // Then: should use negated syntax !(@.field in (...))
    String sql = ctx.toSql();
    // Oracle JSON path doesn't support "not in", must use !(@.field in (...))
    assertThat(sql)
        .contains("JSON_EXISTS")
        .contains("!(@.region.stringOnly() in ($B0, $B1))")
        .doesNotContain("not in");
  }

  @Test
  void shouldRenderInOperatorWithInSyntax() {
    // Given: $in expression with string values
    var inExpr = new InExpression(
        FieldPathExpression.of("status"),
        List.of("completed", "pending"),
        false  // negated = false for $in
    );

    var ctx = new DefaultSqlGenerationContext(true);  // inline values
    var builder = new JsonExistsPredicateBuilder(ctx);

    // When: render the predicate
    builder.render(inExpr);

    // Then: should use standard in syntax
    String sql = ctx.toSql();
    assertThat(sql)
        .contains("JSON_EXISTS")
        .contains("@.status.stringOnly() in ($B0, $B1)")
        .doesNotContain("!");
  }

  @Test
  void shouldRenderExistsExpressionTrue() {
    // Given: $exists: true expression
    var existsExpr = new ExistsExpression("metadata", true);

    var ctx = new DefaultSqlGenerationContext(true);  // inline values
    var builder = new JsonExistsPredicateBuilder(ctx);

    // When: render the predicate
    builder.render(existsExpr);

    // Then: should check if field exists using exists() function
    String sql = ctx.toSql();
    assertThat(sql)
        .contains("JSON_EXISTS")
        .contains("exists(@.metadata)")
        .doesNotContain("true");  // Should not fallback to "true"
  }

  @Test
  void shouldRenderExistsExpressionFalse() {
    // Given: $exists: false expression
    var existsExpr = new ExistsExpression("optionalField", false);

    var ctx = new DefaultSqlGenerationContext(true);  // inline values
    var builder = new JsonExistsPredicateBuilder(ctx);

    // When: render the predicate
    builder.render(existsExpr);

    // Then: should negate the existence check
    String sql = ctx.toSql();
    assertThat(sql)
        .contains("JSON_EXISTS")
        .contains("!(exists(@.optionalField))")
        .doesNotContain("true");  // Should not fallback to "true"
  }

  @Test
  void shouldRenderNorOperatorWithNegation() {
    // Given: $nor expression with two conditions
    // $nor: [{status: "completed"}, {status: "pending"}]
    var cond1 = new ComparisonExpression(
        ComparisonOp.EQ,
        FieldPathExpression.of("status"),
        LiteralExpression.of("completed")
    );
    var cond2 = new ComparisonExpression(
        ComparisonOp.EQ,
        FieldPathExpression.of("status"),
        LiteralExpression.of("pending")
    );
    var norExpr = new LogicalExpression(LogicalOp.NOR, List.of(cond1, cond2));

    var ctx = new DefaultSqlGenerationContext(true);  // inline values
    var builder = new JsonExistsPredicateBuilder(ctx);

    // When: render the predicate
    builder.render(norExpr);

    // Then: should negate the OR of conditions: !((cond1) || (cond2))
    String sql = ctx.toSql();
    assertThat(sql)
        .contains("JSON_EXISTS")
        .contains("!(")  // Should be negated
        .contains("||"); // Should use OR inside negation
  }
}
