/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DateExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderYear() {
    var expr = DateExpression.year(FieldPathExpression.of("createdAt"));

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "EXTRACT(YEAR FROM TO_TIMESTAMP(JSON_VALUE(data, '$.createdAt'),"
                + " 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
  }

  @Test
  void shouldRenderMonth() {
    var expr = DateExpression.month(FieldPathExpression.of("createdAt"));

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(data, '$.createdAt'),"
                + " 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
  }

  @Test
  void shouldRenderDayOfMonth() {
    var expr = DateExpression.dayOfMonth(FieldPathExpression.of("createdAt"));

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "EXTRACT(DAY FROM TO_TIMESTAMP(JSON_VALUE(data, '$.createdAt'),"
                + " 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
  }

  @Test
  void shouldRenderHour() {
    var expr = DateExpression.hour(FieldPathExpression.of("timestamp"));

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "EXTRACT(HOUR FROM TO_TIMESTAMP(JSON_VALUE(data, '$.timestamp'),"
                + " 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
  }

  @Test
  void shouldRenderMinute() {
    var expr = DateExpression.minute(FieldPathExpression.of("timestamp"));

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "EXTRACT(MINUTE FROM TO_TIMESTAMP(JSON_VALUE(data, '$.timestamp'),"
                + " 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
  }

  @Test
  void shouldRenderSecond() {
    var expr = DateExpression.second(FieldPathExpression.of("timestamp"));

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "EXTRACT(SECOND FROM TO_TIMESTAMP(JSON_VALUE(data, '$.timestamp'),"
                + " 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
  }

  @Test
  void shouldRenderDayOfWeek() {
    // MongoDB: 1 (Sunday) - 7 (Saturday)
    // Oracle TO_CHAR with 'D': 1 (Sunday) - 7 (Saturday) - matches!
    var expr = DateExpression.dayOfWeek(FieldPathExpression.of("date"));

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "TO_NUMBER(TO_CHAR(TO_TIMESTAMP(JSON_VALUE(data, '$.date'),"
                + " 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), 'D'))");
  }

  @Test
  void shouldRenderDayOfYear() {
    // MongoDB: 1-366
    // Oracle TO_CHAR with 'DDD': 001-366
    var expr = DateExpression.dayOfYear(FieldPathExpression.of("date"));

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "TO_NUMBER(TO_CHAR(TO_TIMESTAMP(JSON_VALUE(data, '$.date'),"
                + " 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), 'DDD'))");
  }

  @Test
  void shouldRenderWeekWithMongoDbSundayStartConvention() {
    // MongoDB $week: returns 0-53 where week starts on Sunday
    // Week 0 = dates before the first Sunday of the year
    // Week 1 = first full week starting on first Sunday of the year
    // This differs from ISO week (IW) which starts on Monday
    var expr = DateExpression.week(FieldPathExpression.of("eventDate"));

    expr.render(context);

    String sql = context.toSql();
    // Should use NEXT_DAY to find first Sunday of year and calculate week number
    assertThat(sql)
        .as("Should calculate week based on first Sunday of year")
        .contains("NEXT_DAY");
    assertThat(sql)
        .as("Should reference SUNDAY for week boundary")
        .containsIgnoringCase("SUNDAY");
    // Should NOT use 'IW' (ISO week) which starts on Monday
    assertThat(sql)
        .as("Should not use ISO week format")
        .doesNotContain("'IW'");
  }

  @Test
  void shouldReturnOp() {
    var expr = DateExpression.year(FieldPathExpression.of("x"));
    assertThat(expr.getOp()).isEqualTo(DateOp.YEAR);
  }

  @Test
  void shouldReturnArgument() {
    var field = FieldPathExpression.of("x");
    var expr = DateExpression.year(field);
    assertThat(expr.getArgument()).isEqualTo(field);
  }

  @Test
  void shouldProvideReadableToString() {
    var expr = DateExpression.year(FieldPathExpression.of("createdAt"));
    assertThat(expr.toString()).contains("$year");
  }

  // =====================================================
  // Phase 1: Date Arithmetic Operations
  // =====================================================

  @Test
  void shouldRenderDateAdd() {
    // MongoDB: { $dateAdd: { startDate: "$orderDate", unit: "day", amount: 5 } }
    // Oracle: TO_TIMESTAMP(...) + INTERVAL '5' DAY
    var expr =
        DateArithmeticExpression.dateAdd(
            FieldPathExpression.of("orderDate"), "day", LiteralExpression.of(5));

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_TIMESTAMP(JSON_VALUE(data, '$.orderDate')")
        .contains("+ INTERVAL '5' DAY");
  }

  @Test
  void shouldRenderDateAddWithMonthUnit() {
    var expr =
        DateArithmeticExpression.dateAdd(
            FieldPathExpression.of("startDate"), "month", LiteralExpression.of(3));

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_TIMESTAMP(JSON_VALUE(data, '$.startDate')")
        .contains("+ INTERVAL '3' MONTH");
  }

  @Test
  void shouldRenderDateAddWithHourUnit() {
    var expr =
        DateArithmeticExpression.dateAdd(
            FieldPathExpression.of("timestamp"), "hour", LiteralExpression.of(24));

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_TIMESTAMP(JSON_VALUE(data, '$.timestamp')")
        .contains("+ INTERVAL '24' HOUR");
  }

  @Test
  void shouldRenderDateSubtract() {
    // MongoDB: { $dateSubtract: { startDate: "$endDate", unit: "day", amount: 7 } }
    // Oracle: TO_TIMESTAMP(...) - INTERVAL '7' DAY
    var expr =
        DateArithmeticExpression.dateSubtract(
            FieldPathExpression.of("endDate"), "day", LiteralExpression.of(7));

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_TIMESTAMP(JSON_VALUE(data, '$.endDate')")
        .contains("- INTERVAL '7' DAY");
  }

  @Test
  void shouldRenderDateDiff() {
    // MongoDB: { $dateDiff: { startDate: "$start", endDate: "$end", unit: "day" } }
    // Oracle: (end_timestamp - start_timestamp) for days
    var expr =
        DateArithmeticExpression.dateDiff(
            FieldPathExpression.of("startDate"), FieldPathExpression.of("endDate"), "day");

    expr.render(context);

    assertThat(context.toSql()).contains("'$.endDate'").contains("'$.startDate'");
  }

  @Test
  void shouldRenderDateDiffWithMonthUnit() {
    // MongoDB: { $dateDiff: { startDate: "$hire", endDate: "$term", unit: "month" } }
    // Oracle: MONTHS_BETWEEN(end, start)
    var expr =
        DateArithmeticExpression.dateDiff(
            FieldPathExpression.of("hireDate"), FieldPathExpression.of("termDate"), "month");

    expr.render(context);

    assertThat(context.toSql()).contains("MONTHS_BETWEEN");
  }

  // =====================================================
  // Phase 1: Date String Conversion Operations
  // =====================================================

  @Test
  void shouldRenderDateFromString() {
    // MongoDB: { $dateFromString: { dateString: "$dateStr" } }
    // Oracle: TO_TIMESTAMP(JSON_VALUE(data, '$.dateStr'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
    var expr = DateStringExpression.dateFromString(FieldPathExpression.of("dateStr"), null);

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_TIMESTAMP")
        .contains("JSON_VALUE(data, '$.dateStr')");
  }

  @Test
  void shouldRenderDateFromStringWithFormat() {
    // MongoDB: { $dateFromString: { dateString: "$dateStr", format: "%Y-%m-%d" } }
    // Oracle: TO_TIMESTAMP(JSON_VALUE(data, '$.dateStr'), 'YYYY-MM-DD')
    var expr =
        DateStringExpression.dateFromString(FieldPathExpression.of("dateStr"), "%Y-%m-%d");

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_TIMESTAMP")
        .contains("JSON_VALUE(data, '$.dateStr')")
        .contains("'YYYY-MM-DD'");
  }

  @Test
  void shouldRenderDateFromStringWithTimeFormat() {
    // MongoDB: { $dateFromString: { dateString: "$dateStr", format: "%Y-%m-%d %H:%M:%S" } }
    // Oracle: TO_TIMESTAMP(JSON_VALUE(data, '$.dateStr'), 'YYYY-MM-DD HH24:MI:SS')
    var expr =
        DateStringExpression.dateFromString(
            FieldPathExpression.of("dateStr"), "%Y-%m-%d %H:%M:%S");

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_TIMESTAMP")
        .contains("'YYYY-MM-DD HH24:MI:SS'");
  }

  @Test
  void shouldRenderDateToString() {
    // MongoDB: { $dateToString: { date: "$createdAt", format: "%Y-%m-%d" } }
    // Oracle: TO_CHAR(TO_TIMESTAMP(...), 'YYYY-MM-DD')
    var expr =
        DateStringExpression.dateToString(FieldPathExpression.of("createdAt"), "%Y-%m-%d");

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_CHAR")
        .contains("TO_TIMESTAMP")
        .contains("'YYYY-MM-DD'");
  }

  @Test
  void shouldRenderDateToStringWithTimeFormat() {
    // MongoDB: { $dateToString: { date: "$timestamp", format: "%Y-%m-%d %H:%M:%S" } }
    // Oracle: TO_CHAR(TO_TIMESTAMP(...), 'YYYY-MM-DD HH24:MI:SS')
    var expr =
        DateStringExpression.dateToString(
            FieldPathExpression.of("timestamp"), "%Y-%m-%d %H:%M:%S");

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_CHAR")
        .contains("'YYYY-MM-DD HH24:MI:SS'");
  }

  @Test
  void shouldRenderDateToStringWithDefaultIsoFormat() {
    // MongoDB: { $dateToString: { date: "$createdAt" } } (default format)
    // Oracle: TO_CHAR(TO_TIMESTAMP(...), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')
    var expr = DateStringExpression.dateToString(FieldPathExpression.of("createdAt"), null);

    expr.render(context);

    assertThat(context.toSql())
        .contains("TO_CHAR")
        .contains("TO_TIMESTAMP");
  }

  @Test
  void shouldMapMongoFormatSpecifiersToOracle() {
    // Test various format specifier mappings
    // %Y -> YYYY, %m -> MM, %d -> DD, %H -> HH24, %M -> MI, %S -> SS, %L -> FF3
    var expr =
        DateStringExpression.dateToString(
            FieldPathExpression.of("date"), "%Y/%m/%d %H:%M:%S.%L");

    expr.render(context);

    assertThat(context.toSql()).contains("'YYYY/MM/DD HH24:MI:SS.FF3'");
  }

  // =====================================================
  // Phase 1: Date Truncation Operations
  // =====================================================

  @Test
  void shouldRenderDateTruncToDay() {
    // MongoDB: { $dateTrunc: { date: "$timestamp", unit: "day" } }
    // Oracle: TRUNC(TO_TIMESTAMP(...))
    var expr = DateTruncExpression.dateTrunc(FieldPathExpression.of("timestamp"), "day");

    expr.render(context);

    assertThat(context.toSql()).contains("TRUNC");
    assertThat(context.toSql()).contains("TO_TIMESTAMP");
  }

  @Test
  void shouldRenderDateTruncToMonth() {
    // MongoDB: { $dateTrunc: { date: "$createdAt", unit: "month" } }
    // Oracle: TRUNC(TO_TIMESTAMP(...), 'MONTH')
    var expr = DateTruncExpression.dateTrunc(FieldPathExpression.of("createdAt"), "month");

    expr.render(context);

    assertThat(context.toSql()).contains("TRUNC");
    assertThat(context.toSql()).contains("'MONTH'");
  }

  @Test
  void shouldRenderDateTruncToYear() {
    // MongoDB: { $dateTrunc: { date: "$date", unit: "year" } }
    // Oracle: TRUNC(TO_TIMESTAMP(...), 'YEAR')
    var expr = DateTruncExpression.dateTrunc(FieldPathExpression.of("date"), "year");

    expr.render(context);

    assertThat(context.toSql()).contains("TRUNC");
    assertThat(context.toSql()).contains("'YEAR'");
  }

  @Test
  void shouldRenderDateTruncToHour() {
    // MongoDB: { $dateTrunc: { date: "$timestamp", unit: "hour" } }
    // Oracle: TRUNC(TO_TIMESTAMP(...), 'HH')
    var expr = DateTruncExpression.dateTrunc(FieldPathExpression.of("timestamp"), "hour");

    expr.render(context);

    assertThat(context.toSql()).contains("TRUNC");
    assertThat(context.toSql()).contains("'HH'");
  }

  @Test
  void shouldRenderDateTruncToMinute() {
    // MongoDB: { $dateTrunc: { date: "$timestamp", unit: "minute" } }
    // Oracle: TRUNC(TO_TIMESTAMP(...), 'MI')
    var expr = DateTruncExpression.dateTrunc(FieldPathExpression.of("timestamp"), "minute");

    expr.render(context);

    assertThat(context.toSql()).contains("TRUNC");
    assertThat(context.toSql()).contains("'MI'");
  }

  // =====================================================
  // Phase 1: Date Parts Operations
  // =====================================================

  @Test
  void shouldRenderDateFromParts() {
    // MongoDB: { $dateFromParts: { year: 2023, month: 6, day: 15 } }
    // Oracle: TO_TIMESTAMP('2023-06-15 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
    var expr =
        DatePartsExpression.dateFromParts(
            LiteralExpression.of(2023),
            LiteralExpression.of(6),
            LiteralExpression.of(15),
            null,
            null,
            null);

    expr.render(context);

    assertThat(context.toSql()).contains("TO_TIMESTAMP");
  }

  @Test
  void shouldRenderDateFromPartsWithTime() {
    // MongoDB: { $dateFromParts: { year: 2023, month: 6, day: 15, hour: 10, minute: 30, second: 0 }
    var expr =
        DatePartsExpression.dateFromParts(
            LiteralExpression.of(2023),
            LiteralExpression.of(6),
            LiteralExpression.of(15),
            LiteralExpression.of(10),
            LiteralExpression.of(30),
            LiteralExpression.of(0));

    expr.render(context);

    assertThat(context.toSql()).contains("TO_TIMESTAMP");
  }

  @Test
  void shouldRenderDateFromPartsWithFieldPaths() {
    // MongoDB: { $dateFromParts: { year: "$yr", month: "$mo", day: "$dy" } }
    var expr =
        DatePartsExpression.dateFromParts(
            FieldPathExpression.of("yr"),
            FieldPathExpression.of("mo"),
            FieldPathExpression.of("dy"),
            null,
            null,
            null);

    expr.render(context);

    assertThat(context.toSql()).contains("TO_TIMESTAMP");
    assertThat(context.toSql()).contains("$.yr");
    assertThat(context.toSql()).contains("$.mo");
    assertThat(context.toSql()).contains("$.dy");
  }

  @Test
  void shouldRenderDateToParts() {
    // MongoDB: { $dateToParts: { date: "$timestamp" } }
    // Oracle: JSON_OBJECT with multiple EXTRACT calls
    var expr = DatePartsExpression.dateToParts(FieldPathExpression.of("timestamp"));

    expr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_OBJECT");
    assertThat(sql).contains("EXTRACT");
    assertThat(sql).contains("YEAR");
    assertThat(sql).contains("MONTH");
    assertThat(sql).contains("DAY");
  }
}
