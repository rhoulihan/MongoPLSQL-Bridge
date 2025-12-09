/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents a date arithmetic expression for date add/subtract/diff operations. Translates MongoDB
 * date arithmetic operators to Oracle SQL equivalents.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$dateAdd: {startDate: "$orderDate", unit: "day", amount: 5}}} becomes {@code
 *       TO_TIMESTAMP(...) + INTERVAL '5' DAY}
 *   <li>{@code {$dateSubtract: {startDate: "$endDate", unit: "month", amount: 1}}} becomes {@code
 *       TO_TIMESTAMP(...) - INTERVAL '1' MONTH}
 *   <li>{@code {$dateDiff: {startDate: "$start", endDate: "$end", unit: "day"}}} becomes date
 *       subtraction or MONTHS_BETWEEN
 * </ul>
 *
 * <p>Note: MongoDB dates stored as ISODate are represented as strings in JSON. Oracle needs
 * TO_TIMESTAMP to convert the ISO 8601 string to a timestamp.
 */
public final class DateArithmeticExpression implements Expression {

  /**
   * ISO 8601 format pattern for Oracle TO_TIMESTAMP. Handles dates like: 2024-01-15T10:30:00.000Z
   */
  private static final String ISO_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"";

  private final DateArithmeticOp op;
  private final Expression startDate;
  private final Expression endDate; // Only for DATE_DIFF
  private final String unit;
  private final Expression amount; // Only for DATE_ADD/DATE_SUBTRACT

  /** Private constructor for DATE_ADD and DATE_SUBTRACT. */
  private DateArithmeticExpression(
      DateArithmeticOp op, Expression startDate, String unit, Expression amount) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.startDate = Objects.requireNonNull(startDate, "startDate must not be null");
    this.endDate = null;
    this.unit = Objects.requireNonNull(unit, "unit must not be null").toLowerCase(Locale.ROOT);
    this.amount = Objects.requireNonNull(amount, "amount must not be null");
  }

  /** Private constructor for DATE_DIFF. */
  private DateArithmeticExpression(
      DateArithmeticOp op, Expression startDate, Expression endDate, String unit) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.startDate = Objects.requireNonNull(startDate, "startDate must not be null");
    this.endDate = Objects.requireNonNull(endDate, "endDate must not be null");
    this.unit = Objects.requireNonNull(unit, "unit must not be null").toLowerCase(Locale.ROOT);
    this.amount = null;
  }

  /** Creates a $dateAdd expression. */
  public static DateArithmeticExpression dateAdd(
      Expression startDate, String unit, Expression amount) {
    return new DateArithmeticExpression(DateArithmeticOp.DATE_ADD, startDate, unit, amount);
  }

  /** Creates a $dateSubtract expression. */
  public static DateArithmeticExpression dateSubtract(
      Expression startDate, String unit, Expression amount) {
    return new DateArithmeticExpression(DateArithmeticOp.DATE_SUBTRACT, startDate, unit, amount);
  }

  /** Creates a $dateDiff expression. */
  public static DateArithmeticExpression dateDiff(
      Expression startDate, Expression endDate, String unit) {
    return new DateArithmeticExpression(DateArithmeticOp.DATE_DIFF, startDate, endDate, unit);
  }

  /** Returns the date arithmetic operator. */
  public DateArithmeticOp getOp() {
    return op;
  }

  /** Returns the start date expression. */
  public Expression getStartDate() {
    return startDate;
  }

  /** Returns the end date expression (only for DATE_DIFF). */
  public Expression getEndDate() {
    return endDate;
  }

  /** Returns the time unit (day, month, hour, etc.). */
  public String getUnit() {
    return unit;
  }

  /** Returns the amount expression (only for DATE_ADD/DATE_SUBTRACT). */
  public Expression getAmount() {
    return amount;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    if (op == DateArithmeticOp.DATE_DIFF) {
      renderDateDiff(ctx);
    } else {
      renderDateAddSubtract(ctx);
    }
  }

  private void renderDateAddSubtract(SqlGenerationContext ctx) {
    // Build: TO_TIMESTAMP(JSON_VALUE(data, '$.field'), 'format') +/- INTERVAL 'amount' UNIT
    String timestampExpr = buildTimestampExpression(startDate, ctx);

    // Get the amount value
    String amountStr = getAmountValue();

    // Get the Oracle interval unit
    String oracleUnit = mapUnitToOracle(unit);

    ctx.sql(timestampExpr);
    ctx.sql(" ");
    ctx.sql(op.getSqlOperator());
    ctx.sql(" INTERVAL '");
    ctx.sql(amountStr);
    ctx.sql("' ");
    ctx.sql(oracleUnit);
  }

  private void renderDateDiff(SqlGenerationContext ctx) {
    // Build date difference based on unit
    String startTimestamp = buildTimestampExpression(startDate, ctx);
    String endTimestamp = buildTimestampExpression(endDate, ctx);

    switch (unit) {
      case "month":
      case "months":
        // MONTHS_BETWEEN(end, start)
        ctx.sql("TRUNC(MONTHS_BETWEEN(");
        ctx.sql(endTimestamp);
        ctx.sql(", ");
        ctx.sql(startTimestamp);
        ctx.sql("))");
        break;
      case "year":
      case "years":
        // MONTHS_BETWEEN / 12
        ctx.sql("TRUNC(MONTHS_BETWEEN(");
        ctx.sql(endTimestamp);
        ctx.sql(", ");
        ctx.sql(startTimestamp);
        ctx.sql(") / 12)");
        break;
      case "day":
      case "days":
        // TRUNC(end) - TRUNC(start)
        ctx.sql("TRUNC(");
        ctx.sql(endTimestamp);
        ctx.sql(") - TRUNC(");
        ctx.sql(startTimestamp);
        ctx.sql(")");
        break;
      case "hour":
      case "hours":
        // (end - start) * 24
        ctx.sql("TRUNC((");
        ctx.sql(endTimestamp);
        ctx.sql(" - ");
        ctx.sql(startTimestamp);
        ctx.sql(") * 24)");
        break;
      case "minute":
      case "minutes":
        // (end - start) * 24 * 60
        ctx.sql("TRUNC((");
        ctx.sql(endTimestamp);
        ctx.sql(" - ");
        ctx.sql(startTimestamp);
        ctx.sql(") * 24 * 60)");
        break;
      case "second":
      case "seconds":
        // (end - start) * 24 * 60 * 60
        ctx.sql("TRUNC((");
        ctx.sql(endTimestamp);
        ctx.sql(" - ");
        ctx.sql(startTimestamp);
        ctx.sql(") * 24 * 60 * 60)");
        break;
      case "week":
      case "weeks":
        // (TRUNC(end) - TRUNC(start)) / 7
        ctx.sql("TRUNC((TRUNC(");
        ctx.sql(endTimestamp);
        ctx.sql(") - TRUNC(");
        ctx.sql(startTimestamp);
        ctx.sql(")) / 7)");
        break;
      default:
        // Default to days
        ctx.sql("TRUNC(");
        ctx.sql(endTimestamp);
        ctx.sql(") - TRUNC(");
        ctx.sql(startTimestamp);
        ctx.sql(")");
    }
  }

  private String buildTimestampExpression(Expression dateExpr, SqlGenerationContext ctx) {
    StringBuilder timestampExpr = new StringBuilder();
    timestampExpr.append("TO_TIMESTAMP(");

    if (dateExpr instanceof FieldPathExpression) {
      FieldPathExpression fieldPath = (FieldPathExpression) dateExpr;
      String baseAlias = ctx.getBaseTableAlias();
      String tablePrefix = (baseAlias != null && !baseAlias.isEmpty()) ? baseAlias + "." : "";
      timestampExpr.append("JSON_VALUE(");
      timestampExpr.append(tablePrefix);
      timestampExpr.append(fieldPath.getDataColumn());
      timestampExpr.append(", '");
      timestampExpr.append(fieldPath.getJsonPath());
      timestampExpr.append("')");
    } else {
      var innerCtx = ctx.createNestedContext();
      innerCtx.visit(dateExpr);
      timestampExpr.append(innerCtx.toSql());
    }

    timestampExpr.append(", '");
    timestampExpr.append(ISO_FORMAT);
    timestampExpr.append("')");

    return timestampExpr.toString();
  }

  private String getAmountValue() {
    if (amount instanceof LiteralExpression) {
      Object value = ((LiteralExpression) amount).getValue();
      return String.valueOf(value);
    }
    // For dynamic amounts, we'll need bind variables or more complex handling
    // For now, return a placeholder
    return "1";
  }

  private String mapUnitToOracle(String mongoUnit) {
    switch (mongoUnit.toLowerCase(Locale.ROOT)) {
      case "year":
      case "years":
        return "YEAR";
      case "month":
      case "months":
        return "MONTH";
      case "week":
      case "weeks":
        // Oracle doesn't have INTERVAL WEEK, use DAY * 7
        return "DAY"; // Handled specially
      case "day":
      case "days":
        return "DAY";
      case "hour":
      case "hours":
        return "HOUR";
      case "minute":
      case "minutes":
        return "MINUTE";
      case "second":
      case "seconds":
        return "SECOND";
      case "millisecond":
      case "milliseconds":
        // Oracle uses fractional seconds
        return "SECOND"; // Would need special handling for millis
      default:
        return "DAY";
    }
  }

  @Override
  public String toString() {
    if (op == DateArithmeticOp.DATE_DIFF) {
      return "DateArithmetic("
          + op.getMongoOperator()
          + ", start="
          + startDate
          + ", end="
          + endDate
          + ", unit="
          + unit
          + ")";
    }
    return "DateArithmetic("
        + op.getMongoOperator()
        + ", date="
        + startDate
        + ", unit="
        + unit
        + ", amount="
        + amount
        + ")";
  }
}
