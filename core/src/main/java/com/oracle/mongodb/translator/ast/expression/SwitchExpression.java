/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents a $switch expression which evaluates a series of case expressions and executes the
 * first one that matches. Translates to Oracle's CASE WHEN ... THEN ... ELSE ... END.
 *
 * <p>MongoDB syntax:
 *
 * <pre>{@code
 * {$switch: {
 *   branches: [
 *     {case: <expression>, then: <expression>},
 *     {case: <expression>, then: <expression>},
 *     ...
 *   ],
 *   default: <expression>
 * }}
 * }</pre>
 *
 * <p>Oracle SQL equivalent:
 *
 * <pre>{@code
 * CASE
 *   WHEN <condition1> THEN <result1>
 *   WHEN <condition2> THEN <result2>
 *   ...
 *   ELSE <default>
 * END
 * }</pre>
 */
public final class SwitchExpression implements Expression {

  private final List<SwitchBranch> branches;
  private final Expression defaultExpr;

  private SwitchExpression(List<SwitchBranch> branches, Expression defaultExpr) {
    if (branches == null || branches.isEmpty()) {
      throw new IllegalArgumentException("$switch requires at least one branch");
    }
    this.branches = new ArrayList<>(branches);
    this.defaultExpr = defaultExpr;
  }

  /**
   * Creates a $switch expression.
   *
   * @param branches the list of case/then branches
   * @param defaultExpr the default expression (can be null if branches are exhaustive)
   * @return a new SwitchExpression
   */
  public static SwitchExpression of(List<SwitchBranch> branches, Expression defaultExpr) {
    return new SwitchExpression(branches, defaultExpr);
  }

  /** Returns the list of branches. */
  public List<SwitchBranch> getBranches() {
    return new ArrayList<>(branches);
  }

  /** Returns the default expression, or null if not specified. */
  public Expression getDefaultExpr() {
    return defaultExpr;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    ctx.sql("CASE");
    for (SwitchBranch branch : branches) {
      ctx.sql(" WHEN ");
      ctx.visit(branch.caseExpr());
      ctx.sql(" THEN ");
      ctx.visit(branch.thenExpr());
    }
    if (defaultExpr != null) {
      ctx.sql(" ELSE ");
      ctx.visit(defaultExpr);
    }
    ctx.sql(" END");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Switch(");
    for (int i = 0; i < branches.size(); i++) {
      if (i > 0) {
        sb.append(", ");
      }
      SwitchBranch b = branches.get(i);
      sb.append("case ").append(b.caseExpr()).append(" then ").append(b.thenExpr());
    }
    if (defaultExpr != null) {
      sb.append(", default ").append(defaultExpr);
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Represents a single branch in a $switch expression.
   *
   * @param caseExpr the condition to evaluate
   * @param thenExpr the result if the condition is true
   */
  public record SwitchBranch(Expression caseExpr, Expression thenExpr) {
    public SwitchBranch {
      Objects.requireNonNull(caseExpr, "case expression must not be null");
      Objects.requireNonNull(thenExpr, "then expression must not be null");
    }
  }
}
