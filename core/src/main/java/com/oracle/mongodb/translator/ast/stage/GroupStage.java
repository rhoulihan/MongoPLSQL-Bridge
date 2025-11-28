/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.CompoundIdExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a $group stage that groups documents by a specified _id expression. Translates to
 * Oracle's GROUP BY clause with aggregate functions.
 */
public final class GroupStage implements Stage {

  private final Expression idExpression;
  private final Map<String, AccumulatorExpression> accumulators;

  /**
   * Creates a group stage with the given _id expression and accumulators.
   *
   * @param idExpression the grouping key expression (null for grouping all documents)
   * @param accumulators map of field names to accumulator expressions
   */
  public GroupStage(Expression idExpression, Map<String, AccumulatorExpression> accumulators) {
    this.idExpression = idExpression;
    this.accumulators =
        accumulators != null ? new LinkedHashMap<>(accumulators) : new LinkedHashMap<>();
  }

  /** Creates a group stage with only an _id expression (no accumulators). */
  public GroupStage(Expression idExpression) {
    this(idExpression, null);
  }

  /** Returns the _id grouping expression. */
  public Expression getIdExpression() {
    return idExpression;
  }

  /** Returns the accumulators as an unmodifiable map. */
  public Map<String, AccumulatorExpression> getAccumulators() {
    return Collections.unmodifiableMap(accumulators);
  }

  @Override
  public String getOperatorName() {
    return "$group";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // SELECT clause with accumulators
    renderSelectClause(ctx);

    // GROUP BY clause
    renderGroupByClause(ctx);
  }

  private void renderSelectClause(SqlGenerationContext ctx) {
    ctx.sql("SELECT ");

    boolean first = true;

    // Render _id expression if present
    if (idExpression != null) {
      if (idExpression instanceof CompoundIdExpression compound) {
        // For compound _id, render each field with its alias
        compound.renderWithAliases(ctx);
      } else {
        ctx.visit(idExpression);
        ctx.sql(" AS ");
        ctx.identifier("_id");
      }
      first = false;
    }

    // Render accumulators
    for (Map.Entry<String, AccumulatorExpression> entry : accumulators.entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.visit(entry.getValue());
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
      first = false;
    }
  }

  private void renderGroupByClause(SqlGenerationContext ctx) {
    if (idExpression != null) {
      ctx.sql(" GROUP BY ");
      ctx.visit(idExpression);
    }
  }

  @Override
  public String toString() {
    return "GroupStage(_id=" + idExpression + ", accumulators=" + accumulators.keySet() + ")";
  }
}
