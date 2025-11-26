/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class DateOpTest {

    @ParameterizedTest
    @CsvSource({
        "$year, EXTRACT(YEAR FROM %s)",
        "$month, EXTRACT(MONTH FROM %s)",
        "$dayOfMonth, EXTRACT(DAY FROM %s)",
        "$hour, EXTRACT(HOUR FROM %s)",
        "$minute, EXTRACT(MINUTE FROM %s)",
        "$second, EXTRACT(SECOND FROM %s)"
    })
    void shouldMapMongoToSqlExtract(String mongoOp, String sqlTemplate) {
        DateOp op = DateOp.fromMongo(mongoOp);
        assertThat(op.getSqlTemplate()).isEqualTo(sqlTemplate);
    }

    @Test
    void shouldMapDayOfWeekToSql() {
        DateOp op = DateOp.fromMongo("$dayOfWeek");
        assertThat(op.getSqlTemplate()).isEqualTo("TO_CHAR(%s, 'D')");
    }

    @Test
    void shouldMapDayOfYearToSql() {
        DateOp op = DateOp.fromMongo("$dayOfYear");
        assertThat(op.getSqlTemplate()).isEqualTo("TO_CHAR(%s, 'DDD')");
    }

    @Test
    void shouldReturnMongoOperator() {
        assertThat(DateOp.YEAR.getMongoOperator()).isEqualTo("$year");
        assertThat(DateOp.MONTH.getMongoOperator()).isEqualTo("$month");
        assertThat(DateOp.DAY_OF_MONTH.getMongoOperator()).isEqualTo("$dayOfMonth");
    }

    @Test
    void shouldThrowForUnknownOperator() {
        assertThatThrownBy(() -> DateOp.fromMongo("$unknown"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown date operator");
    }

    @Test
    void shouldDetectDateOperator() {
        assertThat(DateOp.isDateOp("$year")).isTrue();
        assertThat(DateOp.isDateOp("$month")).isTrue();
        assertThat(DateOp.isDateOp("$dayOfMonth")).isTrue();
        assertThat(DateOp.isDateOp("$hour")).isTrue();
        assertThat(DateOp.isDateOp("$minute")).isTrue();
        assertThat(DateOp.isDateOp("$second")).isTrue();
        assertThat(DateOp.isDateOp("$dayOfWeek")).isTrue();
        assertThat(DateOp.isDateOp("$dayOfYear")).isTrue();
    }

    @Test
    void shouldReturnFalseForNonDateOp() {
        assertThat(DateOp.isDateOp("$eq")).isFalse();
        assertThat(DateOp.isDateOp("$sum")).isFalse();
        assertThat(DateOp.isDateOp("$concat")).isFalse();
    }

    @Test
    void shouldIdentifyExtractBasedOperators() {
        assertThat(DateOp.YEAR.isExtractBased()).isTrue();
        assertThat(DateOp.MONTH.isExtractBased()).isTrue();
        assertThat(DateOp.DAY_OF_MONTH.isExtractBased()).isTrue();
        assertThat(DateOp.HOUR.isExtractBased()).isTrue();
        assertThat(DateOp.MINUTE.isExtractBased()).isTrue();
        assertThat(DateOp.SECOND.isExtractBased()).isTrue();
        assertThat(DateOp.DAY_OF_WEEK.isExtractBased()).isFalse();
        assertThat(DateOp.DAY_OF_YEAR.isExtractBased()).isFalse();
    }
}
