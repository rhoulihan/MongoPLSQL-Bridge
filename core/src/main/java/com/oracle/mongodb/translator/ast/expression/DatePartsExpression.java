/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents date parts expressions for constructing or deconstructing dates. Translates MongoDB
 * $dateFromParts and $dateToParts operators to Oracle SQL.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$dateFromParts: {year: 2023, month: 6, day: 15}}} becomes Oracle
 *       TO_TIMESTAMP('2023-06-15 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
 *   <li>{@code {$dateToParts: {date: "$timestamp"}}} becomes Oracle JSON_OBJECT with EXTRACT calls
 * </ul>
 */
public final class DatePartsExpression implements Expression {

  /**
   * ISO 8601 format pattern for Oracle TO_TIMESTAMP. Handles dates like: 2024-01-15T10:30:00.000Z
   */
  private static final String ISO_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"";

  private final DatePartsOp op;
  private final Expression dateExpression; // For dateToParts
  private final Expression year; // For dateFromParts
  private final Expression month;
  private final Expression day;
  private final Expression hour;
  private final Expression minute;
  private final Expression second;

  /** Enum for the operation type. */
  public enum DatePartsOp {
    DATE_FROM_PARTS,
    DATE_TO_PARTS
  }

  private DatePartsExpression(
      DatePartsOp op,
      Expression dateExpression,
      Expression year,
      Expression month,
      Expression day,
      Expression hour,
      Expression minute,
      Expression second) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.dateExpression = dateExpression;
    this.year = year;
    this.month = month;
    this.day = day;
    this.hour = hour;
    this.minute = minute;
    this.second = second;
  }

  /**
   * Creates a $dateFromParts expression to construct a date from individual parts.
   *
   * @param year the year expression
   * @param month the month expression (1-12)
   * @param day the day expression (1-31)
   * @param hour the hour expression (0-23), can be null
   * @param minute the minute expression (0-59), can be null
   * @param second the second expression (0-59), can be null
   * @return the date from parts expression
   */
  public static DatePartsExpression dateFromParts(
      Expression year,
      Expression month,
      Expression day,
      Expression hour,
      Expression minute,
      Expression second) {
    Objects.requireNonNull(year, "year must not be null");
    Objects.requireNonNull(month, "month must not be null");
    Objects.requireNonNull(day, "day must not be null");
    return new DatePartsExpression(
        DatePartsOp.DATE_FROM_PARTS, null, year, month, day, hour, minute, second);
  }

  /**
   * Creates a $dateToParts expression to extract date parts from a date.
   *
   * @param dateExpression the date expression to extract parts from
   * @return the date to parts expression
   */
  public static DatePartsExpression dateToParts(Expression dateExpression) {
    Objects.requireNonNull(dateExpression, "dateExpression must not be null");
    return new DatePartsExpression(
        DatePartsOp.DATE_TO_PARTS, dateExpression, null, null, null, null, null, null);
  }

  /** Returns the operation type. */
  public DatePartsOp getOp() {
    return op;
  }

  /** Returns the date expression (for dateToParts). */
  public Expression getDateExpression() {
    return dateExpression;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    if (op == DatePartsOp.DATE_FROM_PARTS) {
      renderDateFromParts(ctx);
    } else {
      renderDateToParts(ctx);
    }
  }

  private void renderDateFromParts(SqlGenerationContext ctx) {
    // Build: TO_TIMESTAMP(year || '-' || month || '-' || day || ' ' || hour || ':' || minute ||
    // ':' || second, 'YYYY-MM-DD HH24:MI:SS')
    ctx.sql("TO_TIMESTAMP(");

    // Year part
    renderPartExpression(ctx, year);
    ctx.sql(" || '-' || ");

    // Month part (with LPAD for 2 digits)
    ctx.sql("LPAD(");
    renderPartExpression(ctx, month);
    ctx.sql(", 2, '0') || '-' || ");

    // Day part (with LPAD for 2 digits)
    ctx.sql("LPAD(");
    renderPartExpression(ctx, day);
    ctx.sql(", 2, '0') || ' ' || ");

    // Hour part (default to 0 if null)
    ctx.sql("LPAD(COALESCE(");
    if (hour != null) {
      renderPartExpression(ctx, hour);
    } else {
      ctx.sql("0");
    }
    ctx.sql(", 0), 2, '0') || ':' || ");

    // Minute part (default to 0 if null)
    ctx.sql("LPAD(COALESCE(");
    if (minute != null) {
      renderPartExpression(ctx, minute);
    } else {
      ctx.sql("0");
    }
    ctx.sql(", 0), 2, '0') || ':' || ");

    // Second part (default to 0 if null)
    ctx.sql("LPAD(COALESCE(");
    if (second != null) {
      renderPartExpression(ctx, second);
    } else {
      ctx.sql("0");
    }
    ctx.sql(", 0), 2, '0')");

    ctx.sql(", 'YYYY-MM-DD HH24:MI:SS')");
  }

  private void renderPartExpression(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof LiteralExpression) {
      LiteralExpression lit = (LiteralExpression) expr;
      ctx.sql(String.valueOf(lit.getValue()));
    } else if (expr instanceof FieldPathExpression) {
      FieldPathExpression fieldPath = (FieldPathExpression) expr;
      String baseAlias = ctx.getBaseTableAlias();
      String tablePrefix = (baseAlias != null && !baseAlias.isEmpty()) ? baseAlias + "." : "";
      ctx.sql("JSON_VALUE(");
      ctx.sql(tablePrefix);
      ctx.sql(fieldPath.getDataColumn());
      ctx.sql(", '");
      ctx.sql(fieldPath.getJsonPath());
      ctx.sql("')");
    } else {
      var innerCtx = ctx.createNestedContext();
      innerCtx.visit(expr);
      ctx.sql(innerCtx.toSql());
    }
  }

  private void renderDateToParts(SqlGenerationContext ctx) {
    // Build: JSON_OBJECT('year' VALUE EXTRACT(YEAR FROM ts), 'month' VALUE EXTRACT(MONTH FROM ts),
    // ...)
    ctx.sql("JSON_OBJECT(");

    ctx.sql("'year' VALUE EXTRACT(YEAR FROM ");
    renderTimestampExpression(ctx);
    ctx.sql(")");

    ctx.sql(", 'month' VALUE EXTRACT(MONTH FROM ");
    renderTimestampExpression(ctx);
    ctx.sql(")");

    ctx.sql(", 'day' VALUE EXTRACT(DAY FROM ");
    renderTimestampExpression(ctx);
    ctx.sql(")");

    ctx.sql(", 'hour' VALUE EXTRACT(HOUR FROM ");
    renderTimestampExpression(ctx);
    ctx.sql(")");

    ctx.sql(", 'minute' VALUE EXTRACT(MINUTE FROM ");
    renderTimestampExpression(ctx);
    ctx.sql(")");

    ctx.sql(", 'second' VALUE EXTRACT(SECOND FROM ");
    renderTimestampExpression(ctx);
    ctx.sql(")");

    ctx.sql(")");
  }

  private void renderTimestampExpression(SqlGenerationContext ctx) {
    ctx.sql("TO_TIMESTAMP(");

    if (dateExpression instanceof FieldPathExpression) {
      FieldPathExpression fieldPath = (FieldPathExpression) dateExpression;
      String baseAlias = ctx.getBaseTableAlias();
      String tablePrefix = (baseAlias != null && !baseAlias.isEmpty()) ? baseAlias + "." : "";
      ctx.sql("JSON_VALUE(");
      ctx.sql(tablePrefix);
      ctx.sql(fieldPath.getDataColumn());
      ctx.sql(", '");
      ctx.sql(fieldPath.getJsonPath());
      ctx.sql("')");
    } else {
      var innerCtx = ctx.createNestedContext();
      innerCtx.visit(dateExpression);
      ctx.sql(innerCtx.toSql());
    }

    ctx.sql(", '");
    ctx.sql(ISO_FORMAT);
    ctx.sql("')");
  }

  @Override
  public String toString() {
    if (op == DatePartsOp.DATE_FROM_PARTS) {
      return "DatePartsExpression($dateFromParts, year="
          + year
          + ", month="
          + month
          + ", day="
          + day
          + ")";
    } else {
      return "DatePartsExpression($dateToParts, date=" + dateExpression + ")";
    }
  }
}
