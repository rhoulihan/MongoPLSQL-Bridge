/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.List;
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
    } else if (op == AccumulatorOp.FIRST) {
      // $first gets the first value in sorted order within each group
      renderFirstOrLast(ctx, "FIRST");
    } else if (op == AccumulatorOp.LAST) {
      // $last gets the last value in sorted order within each group
      renderFirstOrLast(ctx, "LAST");
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

  /**
   * Renders $first or $last accumulator using Oracle's KEEP (DENSE_RANK ...) syntax when sort
   * context is available, or falls back to MIN/MAX otherwise.
   *
   * <p>Oracle syntax: MIN/MAX(expr) KEEP (DENSE_RANK FIRST/LAST ORDER BY sort_expr [ASC|DESC])
   */
  private void renderFirstOrLast(SqlGenerationContext ctx, String rankPosition) {
    List<SortStage.SortField> sortContext = ctx.getGroupSortContext();

    if (sortContext != null && !sortContext.isEmpty()) {
      // With sort context, use KEEP (DENSE_RANK FIRST/LAST ORDER BY ...) syntax
      // MIN is used as the aggregate since we want any value from the first/last ranked row
      ctx.sql("MIN(");
      if (argument != null) {
        ctx.visit(argument);
      }
      ctx.sql(") KEEP (DENSE_RANK ");
      ctx.sql(rankPosition);
      ctx.sql(" ORDER BY ");

      boolean first = true;
      for (SortStage.SortField sortField : sortContext) {
        if (!first) {
          ctx.sql(", ");
        }
        // Render sort field with explicit JSON_VALUE for KEEP ORDER BY clause
        // Use the field's return type if available, otherwise use generic JSON ordering
        renderSortFieldForKeepClause(ctx, sortField);
        first = false;
      }
      ctx.sql(")");
    } else {
      // Without sort context, use KEEP with ORDER BY _id to preserve document order
      // MongoDB processes documents in _id order (insertion order for ObjectIds)
      // For $first: DENSE_RANK FIRST ORDER BY _id ASC (get lowest _id = first document)
      // For $last: DENSE_RANK FIRST ORDER BY _id DESC (get highest _id = last document)
      ctx.sql("MIN(");
      if (argument != null) {
        ctx.visit(argument);
      }
      ctx.sql(") KEEP (DENSE_RANK FIRST ORDER BY ");
      // Order by _id to preserve document insertion order
      String baseAlias = ctx.getBaseTableAlias();
      ctx.sql("JSON_VALUE(");
      if (baseAlias != null && !baseAlias.isEmpty()) {
        ctx.sql(baseAlias);
        ctx.sql(".");
      }
      ctx.sql("data, '$._id')");
      if ("LAST".equals(rankPosition)) {
        ctx.sql(" DESC");
      }
      ctx.sql(")");
    }
  }

  /**
   * Renders a sort field for use in the KEEP ORDER BY clause. Always uses JSON_VALUE since
   * dot notation is not valid in KEEP ORDER BY clauses. Defaults to RETURNING NUMBER for
   * proper numeric sorting when no return type is specified.
   */
  private void renderSortFieldForKeepClause(
      SqlGenerationContext ctx, SortStage.SortField sortField) {
    FieldPathExpression fieldPath = sortField.getFieldPath();

    // Always use JSON_VALUE for KEEP ORDER BY clause (dot notation not supported here)
    String baseAlias = ctx.getBaseTableAlias();
    ctx.sql("JSON_VALUE(");
    if (baseAlias != null && !baseAlias.isEmpty()) {
      ctx.sql(baseAlias);
      ctx.sql(".");
    }
    ctx.sql(fieldPath.getDataColumn());
    ctx.sql(", '");
    ctx.sql(fieldPath.getJsonPath());
    ctx.sql("'");
    // Use explicit return type if specified, otherwise default to NUMBER for proper sorting
    // (MongoDB $first/$last are most commonly used with numeric ordering)
    JsonReturnType returnType = fieldPath.getReturnType();
    if (returnType != null) {
      ctx.sql(" RETURNING ");
      ctx.sql(returnType.getOracleSyntax());
    } else {
      ctx.sql(" RETURNING NUMBER");
    }
    ctx.sql(")");

    // Add sort direction
    if (sortField.getDirection() == SortStage.SortDirection.DESC) {
      ctx.sql(" DESC");
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
