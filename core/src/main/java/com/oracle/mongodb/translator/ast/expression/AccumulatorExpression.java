/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents an accumulator expression used in $group stages. Translates to Oracle aggregate
 * functions (SUM, AVG, COUNT, MIN, MAX, etc.).
 */
public final class AccumulatorExpression implements Expression {

  private final AccumulatorOp op;
  private final Expression argument;

  /**
   * Creates an accumulator expression.
   *
   * @param op the accumulator operator
   * @param argument the argument expression (may be null for $count)
   */
  public AccumulatorExpression(AccumulatorOp op, Expression argument) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    // argument can be null for $count
    this.argument = argument;
  }

  /** Creates a $sum accumulator expression. */
  public static AccumulatorExpression sum(Expression argument) {
    return new AccumulatorExpression(AccumulatorOp.SUM, argument);
  }

  /** Creates a $avg accumulator expression. */
  public static AccumulatorExpression avg(Expression argument) {
    return new AccumulatorExpression(AccumulatorOp.AVG, argument);
  }

  /** Creates a $count accumulator expression. */
  public static AccumulatorExpression count() {
    return new AccumulatorExpression(AccumulatorOp.COUNT, null);
  }

  /** Creates a $min accumulator expression. */
  public static AccumulatorExpression min(Expression argument) {
    return new AccumulatorExpression(AccumulatorOp.MIN, argument);
  }

  /** Creates a $max accumulator expression. */
  public static AccumulatorExpression max(Expression argument) {
    return new AccumulatorExpression(AccumulatorOp.MAX, argument);
  }

  /** Creates a $first accumulator expression. */
  public static AccumulatorExpression first(Expression argument) {
    return new AccumulatorExpression(AccumulatorOp.FIRST, argument);
  }

  /** Creates a $last accumulator expression. */
  public static AccumulatorExpression last(Expression argument) {
    return new AccumulatorExpression(AccumulatorOp.LAST, argument);
  }

  /** Creates a $push accumulator expression. */
  public static AccumulatorExpression push(Expression argument) {
    return new AccumulatorExpression(AccumulatorOp.PUSH, argument);
  }

  /** Creates an $addToSet accumulator expression. */
  public static AccumulatorExpression addToSet(Expression argument) {
    return new AccumulatorExpression(AccumulatorOp.ADD_TO_SET, argument);
  }

  /** Returns the accumulator operator. */
  public AccumulatorOp getOp() {
    return op;
  }

  /** Returns the argument expression, or null for $count. */
  public Expression getArgument() {
    return argument;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    if (op == AccumulatorOp.COUNT) {
      // COUNT(*) for $count
      ctx.sql("COUNT(*)");
    } else if (op == AccumulatorOp.FIRST || op == AccumulatorOp.LAST) {
      // FIRST_VALUE and LAST_VALUE require OVER clause for analytic use
      // In GROUP BY context, we use KEEP (DENSE_RANK FIRST/LAST ORDER BY ...)
      // For simplicity, we output basic aggregate - optimization can improve later
      ctx.sql(op.getSqlFunction());
      ctx.sql("(");
      if (argument != null) {
        ctx.visit(argument);
      }
      ctx.sql(")");
    } else if (op == AccumulatorOp.PUSH) {
      // JSON_ARRAYAGG for $push - collects all values into array
      ctx.sql("JSON_ARRAYAGG(");
      if (argument != null) {
        ctx.visit(argument);
      }
      ctx.sql(")");
    } else if (op == AccumulatorOp.ADD_TO_SET) {
      // $addToSet collects unique values into an array
      // Oracle doesn't support JSON_ARRAYAGG(DISTINCT ...), so we use a workaround:
      // Build JSON array using LISTAGG with quoted string values
      ctx.sql("JSON_QUERY('[' || LISTAGG(DISTINCT '\"' || ");
      if (argument != null) {
        ctx.visit(argument);
      }
      ctx.sql(" || '\"', ',') WITHIN GROUP (ORDER BY ");
      if (argument != null) {
        ctx.visit(argument);
      }
      ctx.sql(") || ']', '$' RETURNING CLOB)");
    } else {
      ctx.sql(op.getSqlFunction());
      ctx.sql("(");
      if (argument != null) {
        ctx.visit(argument);
      }
      ctx.sql(")");
    }
  }

  @Override
  public String toString() {
    if (op == AccumulatorOp.COUNT) {
      return "Accumulator(COUNT(*))";
    }
    return "Accumulator(" + op.getMongoOperator() + ", " + argument + ")";
  }
}
