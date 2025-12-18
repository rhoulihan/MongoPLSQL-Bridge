/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an arithmetic expression with one or more operands. Translates to Oracle arithmetic
 * operations or functions.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$add: ["$price", "$tax"]}} becomes {@code price + tax}
 *   <li>{@code {$multiply: ["$qty", "$price"]}} becomes {@code qty * price}
 *   <li>{@code {$mod: ["$value", 10]}} becomes {@code MOD(value, 10)}
 * </ul>
 */
public final class ArithmeticExpression implements Expression {

  private final ArithmeticOp op;
  private final List<Expression> operands;

  /**
   * Creates an arithmetic expression.
   *
   * @param op the arithmetic operator
   * @param operands the operand expressions (at least 1 for unary, 2 for binary operators)
   */
  public ArithmeticExpression(ArithmeticOp op, List<Expression> operands) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    if (operands == null || operands.isEmpty()) {
      throw new IllegalArgumentException("Arithmetic expression requires at least 1 operand");
    }
    if (operands.size() < 2 && !op.allowsSingleOperand()) {
      throw new IllegalArgumentException(op.getMongoOperator() + " requires at least 2 operands");
    }
    this.operands = new ArrayList<>(operands);
  }

  /** Creates an addition expression. */
  public static ArithmeticExpression add(Expression... operands) {
    return new ArithmeticExpression(ArithmeticOp.ADD, List.of(operands));
  }

  /** Creates a subtraction expression. */
  public static ArithmeticExpression subtract(Expression left, Expression right) {
    return new ArithmeticExpression(ArithmeticOp.SUBTRACT, List.of(left, right));
  }

  /** Creates a multiplication expression. */
  public static ArithmeticExpression multiply(Expression... operands) {
    return new ArithmeticExpression(ArithmeticOp.MULTIPLY, List.of(operands));
  }

  /** Creates a division expression. */
  public static ArithmeticExpression divide(Expression left, Expression right) {
    return new ArithmeticExpression(ArithmeticOp.DIVIDE, List.of(left, right));
  }

  /** Creates a modulo expression. */
  public static ArithmeticExpression mod(Expression left, Expression right) {
    return new ArithmeticExpression(ArithmeticOp.MOD, List.of(left, right));
  }

  /** Returns the arithmetic operator. */
  public ArithmeticOp getOp() {
    return op;
  }

  /** Returns the operands as an unmodifiable list. */
  public List<Expression> getOperands() {
    return Collections.unmodifiableList(operands);
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    if (op.requiresFunctionCall()) {
      renderAsFunction(ctx);
    } else if (op == ArithmeticOp.SUBTRACT && isPotentialDateSubtraction()) {
      renderTypeAwareSubtraction(ctx);
    } else {
      renderAsInfix(ctx);
    }
  }

  /**
   * Checks if this subtraction might involve dates (two field path expressions).
   * We use runtime type detection in SQL rather than heuristics.
   */
  private boolean isPotentialDateSubtraction() {
    if (operands.size() != 2) {
      return false;
    }
    // Check if both operands are field path expressions
    // Runtime SQL will determine if they're dates or numbers
    for (Expression operand : operands) {
      if (operand instanceof FieldPathExpression fieldExpr) {
        JsonReturnType rt = fieldExpr.getReturnType();
        // If explicitly typed as date/timestamp, definitely date subtraction
        if (rt == JsonReturnType.TIMESTAMP || rt == JsonReturnType.DATE) {
          continue;
        }
        // If explicitly typed as NUMBER, not date subtraction
        if (rt == JsonReturnType.NUMBER) {
          return false;
        }
        // Untyped field - could be date or number, use runtime detection
        continue;
      }
      // Non-field operands (literals, etc.) - use regular subtraction
      return false;
    }
    return true;
  }

  /**
   * Renders subtraction with runtime type detection.
   * If both values are numbers, does numeric subtraction.
   * If not numbers, tries date subtraction (returns milliseconds).
   * Pattern: CASE WHEN both are numbers THEN numeric ELSE date END
   */
  private void renderTypeAwareSubtraction(SqlGenerationContext ctx) {
    FieldPathExpression field1 = (FieldPathExpression) operands.get(0);
    FieldPathExpression field2 = (FieldPathExpression) operands.get(1);

    // If explicitly typed as dates, use simple date subtraction
    if (isExplicitDateType(field1) && isExplicitDateType(field2)) {
      renderSimpleDateSubtraction(ctx, field1, field2);
      return;
    }

    // Runtime detection: CASE WHEN numeric THEN num_sub ELSE date_sub END
    ctx.sql("CASE WHEN ");
    renderNumericCheck(ctx, field1);
    ctx.sql(" AND ");
    renderNumericCheck(ctx, field2);
    ctx.sql(" THEN (");
    renderNumericOperand(ctx, field1);
    ctx.sql(" - ");
    renderNumericOperand(ctx, field2);
    ctx.sql(") ELSE ");
    // Date subtraction branch - uses EXTRACT to convert interval to milliseconds
    renderSimpleDateSubtraction(ctx, field1, field2);
    ctx.sql(" END");
  }

  private boolean isExplicitDateType(FieldPathExpression field) {
    JsonReturnType rt = field.getReturnType();
    return rt == JsonReturnType.TIMESTAMP || rt == JsonReturnType.DATE;
  }

  private void renderSimpleDateSubtraction(
      SqlGenerationContext ctx, FieldPathExpression field1, FieldPathExpression field2) {
    // TIMESTAMP - TIMESTAMP yields INTERVAL, not a number.
    // We must extract components and compute milliseconds:
    // DAY * 86400000 + HOUR * 3600000 + MINUTE * 60000 + SECOND * 1000
    ctx.sql("(EXTRACT(DAY FROM (");
    renderDateOperand(ctx, field1);
    ctx.sql(" - ");
    renderDateOperand(ctx, field2);
    ctx.sql(")) * 86400000 + EXTRACT(HOUR FROM (");
    renderDateOperand(ctx, field1);
    ctx.sql(" - ");
    renderDateOperand(ctx, field2);
    ctx.sql(")) * 3600000 + EXTRACT(MINUTE FROM (");
    renderDateOperand(ctx, field1);
    ctx.sql(" - ");
    renderDateOperand(ctx, field2);
    ctx.sql(")) * 60000 + EXTRACT(SECOND FROM (");
    renderDateOperand(ctx, field1);
    ctx.sql(" - ");
    renderDateOperand(ctx, field2);
    ctx.sql(")) * 1000)");
  }

  /** Renders a check if the field value is numeric. */
  private void renderNumericCheck(SqlGenerationContext ctx, FieldPathExpression field) {
    String path = field.getPath();
    final String normalizedPath = path.startsWith("$") ? path.substring(1) : path;
    String baseAlias = ctx.getBaseTableAlias();

    ctx.sql("JSON_VALUE(");
    if (baseAlias != null && !baseAlias.isEmpty()) {
      ctx.sql(baseAlias);
      ctx.sql(".");
    }
    ctx.sql("data, '$.");
    ctx.sql(normalizedPath);
    ctx.sql("' RETURNING NUMBER NULL ON ERROR) IS NOT NULL");
  }

  /** Renders field as numeric value. */
  private void renderNumericOperand(SqlGenerationContext ctx, FieldPathExpression field) {
    FieldPathExpression numExpr = FieldPathExpression.of(field.getPath(), JsonReturnType.NUMBER);
    ctx.visit(numExpr);
  }

  /**
   * Renders field as date value for date arithmetic.
   * Oracle stores dates as plain ISO8601 strings (not MongoDB Extended JSON).
   * Uses TIMESTAMP type to preserve full time precision including hours/minutes.
   * Note: TIMESTAMP - TIMESTAMP yields INTERVAL which requires EXTRACT for math.
   */
  private void renderDateOperand(SqlGenerationContext ctx, FieldPathExpression field) {
    String path = field.getPath();
    final String normalizedPath = path.startsWith("$") ? path.substring(1) : path;
    String baseAlias = ctx.getBaseTableAlias();

    // Generate JSON_VALUE with plain path - dates are stored as ISO8601 strings
    // Use TIMESTAMP type to preserve full time precision
    ctx.sql("JSON_VALUE(");
    if (baseAlias != null && !baseAlias.isEmpty()) {
      ctx.sql(baseAlias);
      ctx.sql(".");
    }
    ctx.sql("data, '$.");
    ctx.sql(normalizedPath);
    ctx.sql("' RETURNING TIMESTAMP)");
  }

  private void renderAsFunction(SqlGenerationContext ctx) {
    // MOD(a, b)
    ctx.sql(op.getSqlOperator());
    ctx.sql("(");
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      ctx.visit(operands.get(i));
    }
    ctx.sql(")");
  }

  private void renderAsInfix(SqlGenerationContext ctx) {
    // (a + b + c) or (a - b)
    ctx.sql("(");
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0) {
        ctx.sql(" ");
        ctx.sql(op.getSqlOperator());
        ctx.sql(" ");
      }
      ctx.visit(operands.get(i));
    }
    ctx.sql(")");
  }

  @Override
  public String toString() {
    return "Arithmetic(" + op.getMongoOperator() + ", " + operands + ")";
  }
}
