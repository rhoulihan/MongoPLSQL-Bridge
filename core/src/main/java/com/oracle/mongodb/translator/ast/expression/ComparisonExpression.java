/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.List;
import java.util.Objects;

/** Represents a comparison expression (e.g., $eq, $gt, $lt). */
public final class ComparisonExpression implements Expression {

  private final ComparisonOp op;
  private final Expression left;
  private final Expression right;

  /**
   * Creates a comparison expression.
   *
   * @param op the comparison operator
   * @param left the left operand
   * @param right the right operand
   */
  public ComparisonExpression(ComparisonOp op, Expression left, Expression right) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.left = Objects.requireNonNull(left, "left must not be null");
    this.right = Objects.requireNonNull(right, "right must not be null");
  }

  /** Returns the comparison operator. */
  public ComparisonOp getOp() {
    return op;
  }

  /** Returns the left operand. */
  public Expression getLeft() {
    return left;
  }

  /** Returns the right operand. */
  public Expression getRight() {
    return right;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Handle NULL comparisons specially
    if (right instanceof LiteralExpression lit && lit.isNull()) {
      ctx.visit(left);
      if (op == ComparisonOp.EQ) {
        ctx.sql(" IS NULL");
      } else if (op == ComparisonOp.NE) {
        ctx.sql(" IS NOT NULL");
      } else {
        throw new IllegalStateException("Invalid NULL comparison with operator: " + op);
      }
      return;
    }

    // Handle comparing field to empty array [] - use array size check
    if ((op == ComparisonOp.EQ || op == ComparisonOp.NE)
        && right instanceof LiteralExpression lit
        && lit.getValue() instanceof List<?> listVal
        && listVal.isEmpty()
        && left instanceof FieldPathExpression fieldPath) {
      // For field $ne [], check if array has elements: JSON_VALUE(data, '$.field.size()') > 0
      // For field $eq [], check if array is empty: JSON_VALUE(data, '$.field.size()') = 0
      ctx.sql("JSON_VALUE(");
      String alias = ctx.getBaseTableAlias();
      if (alias != null && !alias.isEmpty()) {
        ctx.sql(alias);
        ctx.sql(".");
      }
      ctx.sql("data, '$.");
      ctx.sql(fieldPath.getPath());
      ctx.sql(".size()')");
      if (op == ComparisonOp.NE) {
        ctx.sql(" > 0");
      } else {
        ctx.sql(" = 0");
      }
      return;
    }

    // Handle $in/$nin with array literal as right operand
    if ((op == ComparisonOp.IN || op == ComparisonOp.NIN)
        && right instanceof LiteralExpression lit
        && lit.getValue() instanceof List) {
      ctx.visit(left);
      ctx.sql(" ");
      ctx.sql(op.getSqlOperator());
      ctx.sql(" (");
      @SuppressWarnings("unchecked")
      List<Object> values = (List<Object>) lit.getValue();
      for (int i = 0; i < values.size(); i++) {
        if (i > 0) {
          ctx.sql(", ");
        }
        ctx.bind(values.get(i));
      }
      ctx.sql(")");
      return;
    }

    ctx.visit(left);
    ctx.sql(" ");
    ctx.sql(op.getSqlOperator());
    ctx.sql(" ");
    ctx.visit(right);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ComparisonExpression that = (ComparisonExpression) obj;
    return op == that.op && Objects.equals(left, that.left) && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(op, left, right);
  }

  @Override
  public String toString() {
    return "Comparison(" + left + " " + op + " " + right + ")";
  }
}
