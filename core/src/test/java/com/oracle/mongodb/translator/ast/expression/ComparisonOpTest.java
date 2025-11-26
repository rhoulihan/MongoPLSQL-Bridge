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

class ComparisonOpTest {

    @ParameterizedTest
    @CsvSource({
        "$eq, EQ",
        "$ne, NE",
        "$gt, GT",
        "$gte, GTE",
        "$lt, LT",
        "$lte, LTE",
        "$in, IN",
        "$nin, NIN"
    })
    void shouldParseMongoOperator(String mongoOp, ComparisonOp expected) {
        assertThat(ComparisonOp.fromMongo(mongoOp)).isEqualTo(expected);
    }

    @Test
    void shouldThrowOnUnknownOperator() {
        assertThatThrownBy(() -> ComparisonOp.fromMongo("$unknown"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("$unknown");
    }

    @ParameterizedTest
    @CsvSource({
        "EQ, =",
        "NE, <>",
        "GT, >",
        "GTE, >=",
        "LT, <",
        "LTE, <=",
        "IN, IN",
        "NIN, NOT IN"
    })
    void shouldReturnSqlOperator(ComparisonOp op, String expectedSql) {
        assertThat(op.getSqlOperator()).isEqualTo(expectedSql);
    }

    @ParameterizedTest
    @CsvSource({
        "EQ, $eq",
        "NE, $ne",
        "GT, $gt",
        "GTE, $gte",
        "LT, $lt",
        "LTE, $lte",
        "IN, $in",
        "NIN, $nin"
    })
    void shouldReturnMongoOperator(ComparisonOp op, String expectedMongo) {
        assertThat(op.getMongoOperator()).isEqualTo(expectedMongo);
    }
}
