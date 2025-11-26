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
            .isEqualTo("EXTRACT(YEAR FROM TO_TIMESTAMP(JSON_VALUE(data, '$.createdAt'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
    }

    @Test
    void shouldRenderMonth() {
        var expr = DateExpression.month(FieldPathExpression.of("createdAt"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(data, '$.createdAt'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
    }

    @Test
    void shouldRenderDayOfMonth() {
        var expr = DateExpression.dayOfMonth(FieldPathExpression.of("createdAt"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("EXTRACT(DAY FROM TO_TIMESTAMP(JSON_VALUE(data, '$.createdAt'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
    }

    @Test
    void shouldRenderHour() {
        var expr = DateExpression.hour(FieldPathExpression.of("timestamp"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("EXTRACT(HOUR FROM TO_TIMESTAMP(JSON_VALUE(data, '$.timestamp'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
    }

    @Test
    void shouldRenderMinute() {
        var expr = DateExpression.minute(FieldPathExpression.of("timestamp"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("EXTRACT(MINUTE FROM TO_TIMESTAMP(JSON_VALUE(data, '$.timestamp'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
    }

    @Test
    void shouldRenderSecond() {
        var expr = DateExpression.second(FieldPathExpression.of("timestamp"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("EXTRACT(SECOND FROM TO_TIMESTAMP(JSON_VALUE(data, '$.timestamp'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'))");
    }

    @Test
    void shouldRenderDayOfWeek() {
        // MongoDB: 1 (Sunday) - 7 (Saturday)
        // Oracle TO_CHAR with 'D': 1 (Sunday) - 7 (Saturday) - matches!
        var expr = DateExpression.dayOfWeek(FieldPathExpression.of("date"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("TO_NUMBER(TO_CHAR(TO_TIMESTAMP(JSON_VALUE(data, '$.date'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), 'D'))");
    }

    @Test
    void shouldRenderDayOfYear() {
        // MongoDB: 1-366
        // Oracle TO_CHAR with 'DDD': 001-366
        var expr = DateExpression.dayOfYear(FieldPathExpression.of("date"));

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("TO_NUMBER(TO_CHAR(TO_TIMESTAMP(JSON_VALUE(data, '$.date'), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"'), 'DDD'))");
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
}
