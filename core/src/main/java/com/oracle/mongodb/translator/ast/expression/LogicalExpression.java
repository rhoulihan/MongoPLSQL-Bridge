/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.List;
import java.util.Objects;

/** Represents a logical expression ($and, $or, $not, $nor). */
public final class LogicalExpression implements Expression {

  private final LogicalOp op;
  private final List<Expression> operands;

  /**
   * Creates a logical expression.
   *
   * @param op the logical operator
   * @param operands the operand expressions
   */
  public LogicalExpression(LogicalOp op, List<Expression> operands) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.operands = List.copyOf(Objects.requireNonNull(operands, "operands must not be null"));

    if (operands.isEmpty()) {
      throw new IllegalArgumentException("Logical expression must have at least one operand");
    }
    if (op == LogicalOp.NOT && operands.size() != 1) {
      throw new IllegalArgumentException("NOT expression must have exactly one operand");
    }
  }

  /** Returns the logical operator. */
  public LogicalOp getOp() {
    return op;
  }

  /** Returns the list of operands. */
  public List<Expression> getOperands() {
    return operands;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    if (op == LogicalOp.NOT) {
      renderNot(ctx);
    } else if (op == LogicalOp.NOR) {
      renderNor(ctx);
    } else {
      renderBinaryLogical(ctx);
    }
  }

  private void renderNot(SqlGenerationContext ctx) {
    ctx.sql("NOT (");
    ctx.visit(operands.get(0));
    ctx.sql(")");
  }

  private void renderNor(SqlGenerationContext ctx) {
    // NOR is NOT (a OR b OR c)
    ctx.sql("NOT (");
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0) {
        ctx.sql(" OR ");
      }
      ctx.sql("(");
      ctx.visit(operands.get(i));
      ctx.sql(")");
    }
    ctx.sql(")");
  }

  private void renderBinaryLogical(SqlGenerationContext ctx) {
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0) {
        ctx.sql(" ");
        ctx.sql(op.getSqlOperator());
        ctx.sql(" ");
      }
      ctx.sql("(");
      ctx.visit(operands.get(i));
      ctx.sql(")");
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    LogicalExpression that = (LogicalExpression) obj;
    return op == that.op && Objects.equals(operands, that.operands);
  }

  @Override
  public int hashCode() {
    return Objects.hash(op, operands);
  }

  @Override
  public String toString() {
    return "Logical(" + op + ", " + operands + ")";
  }
}
