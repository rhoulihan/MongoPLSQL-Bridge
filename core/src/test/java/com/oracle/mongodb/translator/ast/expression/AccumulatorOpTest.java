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

class AccumulatorOpTest {

    @ParameterizedTest
    @CsvSource({
        "$sum, SUM",
        "$avg, AVG",
        "$count, COUNT",
        "$min, MIN",
        "$max, MAX",
        "$first, FIRST_VALUE",
        "$last, LAST_VALUE"
    })
    void shouldMapMongoToSql(String mongoOp, String sqlFunc) {
        AccumulatorOp op = AccumulatorOp.fromMongo(mongoOp);
        assertThat(op.getSqlFunction()).isEqualTo(sqlFunc);
    }

    @Test
    void shouldReturnMongoOperator() {
        assertThat(AccumulatorOp.SUM.getMongoOperator()).isEqualTo("$sum");
        assertThat(AccumulatorOp.AVG.getMongoOperator()).isEqualTo("$avg");
        assertThat(AccumulatorOp.COUNT.getMongoOperator()).isEqualTo("$count");
    }

    @Test
    void shouldThrowForUnknownOperator() {
        assertThatThrownBy(() -> AccumulatorOp.fromMongo("$unknown"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown accumulator operator");
    }

    @Test
    void shouldDetectAccumulatorOperator() {
        assertThat(AccumulatorOp.isAccumulator("$sum")).isTrue();
        assertThat(AccumulatorOp.isAccumulator("$avg")).isTrue();
        assertThat(AccumulatorOp.isAccumulator("$count")).isTrue();
        assertThat(AccumulatorOp.isAccumulator("$min")).isTrue();
        assertThat(AccumulatorOp.isAccumulator("$max")).isTrue();
    }

    @Test
    void shouldReturnFalseForNonAccumulator() {
        assertThat(AccumulatorOp.isAccumulator("$eq")).isFalse();
        assertThat(AccumulatorOp.isAccumulator("$match")).isFalse();
        assertThat(AccumulatorOp.isAccumulator("$add")).isFalse();
    }
}
