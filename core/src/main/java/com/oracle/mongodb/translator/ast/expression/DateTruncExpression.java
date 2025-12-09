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
 * Represents a date truncation expression. Translates MongoDB $dateTrunc operator to Oracle SQL
 * TRUNC function.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$dateTrunc: {date: "$timestamp", unit: "day"}}} becomes {@code
 *       TRUNC(TO_TIMESTAMP(...))}
 *   <li>{@code {$dateTrunc: {date: "$createdAt", unit: "month"}}} becomes {@code
 *       TRUNC(TO_TIMESTAMP(...), 'MONTH')}
 *   <li>{@code {$dateTrunc: {date: "$date", unit: "year"}}} becomes {@code TRUNC(TO_TIMESTAMP(...),
 *       'YEAR')}
 * </ul>
 *
 * <p>Note: MongoDB dates stored as ISODate are represented as strings in JSON. Oracle needs
 * TO_TIMESTAMP to convert the ISO 8601 string to a timestamp before truncation.
 */
public final class DateTruncExpression implements Expression {

  /**
   * ISO 8601 format pattern for Oracle TO_TIMESTAMP. Handles dates like: 2024-01-15T10:30:00.000Z
   */
  private static final String ISO_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"";

  private final Expression dateExpression;
  private final String unit;

  /**
   * Creates a date truncation expression.
   *
   * @param dateExpression the date expression to truncate
   * @param unit the unit to truncate to (year, month, day, hour, minute, etc.)
   */
  private DateTruncExpression(Expression dateExpression, String unit) {
    this.dateExpression =
        Objects.requireNonNull(dateExpression, "dateExpression must not be null");
    this.unit = Objects.requireNonNull(unit, "unit must not be null").toLowerCase(Locale.ROOT);
  }

  /** Creates a $dateTrunc expression. */
  public static DateTruncExpression dateTrunc(Expression dateExpression, String unit) {
    return new DateTruncExpression(dateExpression, unit);
  }

  /** Returns the date expression to truncate. */
  public Expression getDateExpression() {
    return dateExpression;
  }

  /** Returns the truncation unit. */
  public String getUnit() {
    return unit;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Build: TRUNC(TO_TIMESTAMP(...), 'UNIT')
    ctx.sql("TRUNC(");
    renderTimestampExpression(ctx);

    // Add unit if not day (day is the default for TRUNC)
    String oracleUnit = mapUnitToOracle(unit);
    if (oracleUnit != null) {
      ctx.sql(", '");
      ctx.sql(oracleUnit);
      ctx.sql("'");
    }
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

  /**
   * Maps MongoDB unit to Oracle TRUNC format model.
   *
   * @param mongoUnit the MongoDB unit (year, month, day, hour, minute, etc.)
   * @return the Oracle format model, or null if day (default)
   */
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
        return "WW"; // Week of year
      case "day":
      case "days":
        return null; // Day is default for TRUNC
      case "hour":
      case "hours":
        return "HH";
      case "minute":
      case "minutes":
        return "MI";
      case "second":
      case "seconds":
        // Oracle TRUNC doesn't support seconds directly
        // We'll truncate to minutes as closest approximation
        return "MI";
      default:
        return null; // Default to day truncation
    }
  }

  @Override
  public String toString() {
    return "DateTrunc($dateTrunc, date=" + dateExpression + ", unit=" + unit + ")";
  }
}
