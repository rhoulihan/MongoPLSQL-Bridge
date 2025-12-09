/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a date string expression for parsing strings to dates and formatting dates as strings.
 * Translates MongoDB $dateFromString and $dateToString operators to Oracle SQL equivalents.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$dateFromString: {dateString: "$dateStr", format: "%Y-%m-%d"}}} becomes {@code
 *       TO_TIMESTAMP(JSON_VALUE(data, '$.dateStr'), 'YYYY-MM-DD')}
 *   <li>{@code {$dateToString: {date: "$date", format: "%Y-%m-%d"}}} becomes {@code
 *       TO_CHAR(TO_TIMESTAMP(...), 'YYYY-MM-DD')}
 * </ul>
 *
 * <p>Note: MongoDB format specifiers are converted to Oracle format elements:
 *
 * <ul>
 *   <li>%Y -> YYYY (4-digit year)
 *   <li>%m -> MM (2-digit month)
 *   <li>%d -> DD (2-digit day)
 *   <li>%H -> HH24 (hour, 24-hour format)
 *   <li>%M -> MI (minutes)
 *   <li>%S -> SS (seconds)
 *   <li>%L -> FF3 (milliseconds)
 *   <li>%j -> DDD (day of year)
 * </ul>
 */
public final class DateStringExpression implements Expression {

  /**
   * ISO 8601 format pattern for Oracle TO_TIMESTAMP. Handles dates like: 2024-01-15T10:30:00.000Z
   */
  private static final String ISO_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"";

  private final DateStringOp op;
  private final Expression dateExpression;
  private final String format; // MongoDB format string (nullable)

  /**
   * Creates a date string expression.
   *
   * @param op the date string operator
   * @param dateExpression the date or string expression
   * @param format the MongoDB format string (may be null for default ISO format)
   */
  private DateStringExpression(DateStringOp op, Expression dateExpression, String format) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.dateExpression = Objects.requireNonNull(dateExpression, "dateExpression must not be null");
    this.format = format; // Can be null for default format
  }

  /** Creates a $dateFromString expression. */
  public static DateStringExpression dateFromString(Expression dateString, String format) {
    return new DateStringExpression(DateStringOp.DATE_FROM_STRING, dateString, format);
  }

  /** Creates a $dateToString expression. */
  public static DateStringExpression dateToString(Expression date, String format) {
    return new DateStringExpression(DateStringOp.DATE_TO_STRING, date, format);
  }

  /** Returns the date string operator. */
  public DateStringOp getOp() {
    return op;
  }

  /** Returns the date/string expression. */
  public Expression getDateExpression() {
    return dateExpression;
  }

  /** Returns the format string (may be null). */
  public String getFormat() {
    return format;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    if (op == DateStringOp.DATE_FROM_STRING) {
      renderDateFromString(ctx);
    } else {
      renderDateToString(ctx);
    }
  }

  private void renderDateFromString(SqlGenerationContext ctx) {
    // Build: TO_TIMESTAMP(string_expr, 'format')
    ctx.sql("TO_TIMESTAMP(");
    renderInputExpression(ctx);
    ctx.sql(", '");
    ctx.sql(getOracleFormat());
    ctx.sql("')");
  }

  private void renderDateToString(SqlGenerationContext ctx) {
    // Build: TO_CHAR(TO_TIMESTAMP(date_expr, 'iso_format'), 'output_format')
    ctx.sql("TO_CHAR(");

    // First convert the input to a timestamp (in case it's a string)
    ctx.sql("TO_TIMESTAMP(");
    renderInputExpression(ctx);
    ctx.sql(", '");
    ctx.sql(ISO_FORMAT);
    ctx.sql("')");

    ctx.sql(", '");
    ctx.sql(getOracleFormat());
    ctx.sql("')");
  }

  private void renderInputExpression(SqlGenerationContext ctx) {
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
  }

  /**
   * Converts MongoDB format string to Oracle format. If format is null, returns the default ISO
   * format.
   */
  private String getOracleFormat() {
    if (format == null) {
      return ISO_FORMAT;
    }
    return convertMongoFormatToOracle(format);
  }

  /**
   * Converts MongoDB format specifiers to Oracle format elements.
   *
   * <p>MongoDB format specifiers:
   *
   * <ul>
   *   <li>%Y - 4-digit year
   *   <li>%m - 2-digit month (01-12)
   *   <li>%d - 2-digit day (01-31)
   *   <li>%H - 2-digit hour (00-23)
   *   <li>%M - 2-digit minute (00-59)
   *   <li>%S - 2-digit second (00-59)
   *   <li>%L - 3-digit millisecond (000-999)
   *   <li>%j - 3-digit day of year (001-366)
   *   <li>%w - day of week (1-7, Sunday=1)
   *   <li>%U - week of year (00-53)
   *   <li>%Z - timezone offset
   * </ul>
   */
  private String convertMongoFormatToOracle(String mongoFormat) {
    String result = mongoFormat;

    // Replace MongoDB format specifiers with Oracle equivalents
    // Order matters - longer patterns first to avoid partial replacements
    result = result.replace("%Y", "YYYY"); // 4-digit year
    result = result.replace("%m", "MM"); // 2-digit month
    result = result.replace("%d", "DD"); // 2-digit day
    result = result.replace("%H", "HH24"); // 24-hour format
    result = result.replace("%M", "MI"); // Minutes
    result = result.replace("%S", "SS"); // Seconds
    result = result.replace("%L", "FF3"); // Milliseconds
    result = result.replace("%j", "DDD"); // Day of year
    result = result.replace("%w", "D"); // Day of week (Oracle: 1-7)
    result = result.replace("%U", "WW"); // Week of year
    result = result.replace("%Z", "TZH:TZM"); // Timezone

    return result;
  }

  @Override
  public String toString() {
    return "DateString("
        + op.getMongoOperator()
        + ", expr="
        + dateExpression
        + ", format="
        + format
        + ")";
  }
}
