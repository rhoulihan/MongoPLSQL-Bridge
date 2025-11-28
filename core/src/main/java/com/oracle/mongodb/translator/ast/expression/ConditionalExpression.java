/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a conditional expression like $cond or $ifNull. Translates to Oracle's CASE WHEN or
 * NVL.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$cond: [condition, thenValue, elseValue]}} becomes {@code CASE WHEN condition THEN
 *       thenValue ELSE elseValue END}
 *   <li>{@code {$ifNull: [field, default]}} becomes {@code NVL(field, default)}
 * </ul>
 */
public final class ConditionalExpression implements Expression {

  private final ConditionalType type;
  private final Expression condition;
  private final Expression thenExpr;
  private final Expression elseExpr;

  private ConditionalExpression(
      ConditionalType type, Expression condition, Expression thenExpr, Expression elseExpr) {
    this.type = Objects.requireNonNull(type, "type must not be null");
    this.condition = condition;
    this.thenExpr = Objects.requireNonNull(thenExpr, "thenExpr must not be null");
    this.elseExpr = Objects.requireNonNull(elseExpr, "elseExpr must not be null");
  }

  /**
   * Creates a $cond expression.
   *
   * @param condition the condition to evaluate
   * @param thenExpr the expression to return if condition is true
   * @param elseExpr the expression to return if condition is false
   */
  public static ConditionalExpression cond(
      Expression condition, Expression thenExpr, Expression elseExpr) {
    return new ConditionalExpression(
        ConditionalType.COND,
        Objects.requireNonNull(condition, "condition must not be null"),
        thenExpr,
        elseExpr);
  }

  /**
   * Creates a $ifNull expression.
   *
   * @param expression the expression to check for null
   * @param replacement the replacement value if expression is null
   */
  public static ConditionalExpression ifNull(Expression expression, Expression replacement) {
    return new ConditionalExpression(ConditionalType.IF_NULL, null, expression, replacement);
  }

  /** Returns the conditional type. */
  public ConditionalType getType() {
    return type;
  }

  /** Returns the condition expression (null for IF_NULL type). */
  public Expression getCondition() {
    return condition;
  }

  /** Returns the then expression (or the expression to check for IF_NULL). */
  public Expression getThenExpr() {
    return thenExpr;
  }

  /** Returns the else expression (or the replacement for IF_NULL). */
  public Expression getElseExpr() {
    return elseExpr;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    switch (type) {
      case COND:
        renderCond(ctx);
        break;
      case IF_NULL:
        renderIfNull(ctx);
        break;
      default:
        throw new IllegalStateException("Unknown conditional type: " + type);
    }
  }

  private void renderCond(SqlGenerationContext ctx) {
    ctx.sql("CASE WHEN ");
    ctx.visit(condition);
    ctx.sql(" THEN ");
    ctx.visit(thenExpr);
    ctx.sql(" ELSE ");
    ctx.visit(elseExpr);
    ctx.sql(" END");
  }

  private void renderIfNull(SqlGenerationContext ctx) {
    ctx.sql("NVL(");
    ctx.visit(thenExpr);
    ctx.sql(", ");
    ctx.visit(elseExpr);
    ctx.sql(")");
  }

  @Override
  public String toString() {
    if (type == ConditionalType.IF_NULL) {
      return "IfNull(" + thenExpr + ", " + elseExpr + ")";
    }
    return "Cond(" + condition + " ? " + thenExpr + " : " + elseExpr + ")";
  }

  /** Type of conditional expression. */
  public enum ConditionalType {
    COND("$cond"),
    IF_NULL("$ifNull");

    private final String mongoOperator;

    ConditionalType(String mongoOperator) {
      this.mongoOperator = mongoOperator;
    }

    public String getMongoOperator() {
      return mongoOperator;
    }

    /**
     * Returns the ConditionalType for the given MongoDB operator.
     *
     * @param mongoOp the MongoDB operator
     * @return the corresponding ConditionalType
     */
    public static ConditionalType fromMongo(String mongoOp) {
      switch (mongoOp) {
        case "$cond":
          return COND;
        case "$ifNull":
          return IF_NULL;
        default:
          throw new IllegalArgumentException("Unknown conditional operator: " + mongoOp);
      }
    }

    public static boolean isConditional(String mongoOp) {
      return "$cond".equals(mongoOp) || "$ifNull".equals(mongoOp);
    }
  }
}
