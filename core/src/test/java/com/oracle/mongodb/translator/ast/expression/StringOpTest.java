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

class StringOpTest {

    @ParameterizedTest
    @CsvSource({
        "$concat, CONCAT",
        "$toLower, LOWER",
        "$toUpper, UPPER",
        "$substr, SUBSTR",
        "$trim, TRIM",
        "$ltrim, LTRIM",
        "$rtrim, RTRIM",
        "$strLenCP, LENGTH"
    })
    void shouldMapMongoToSql(String mongoOp, String sqlFunc) {
        StringOp op = StringOp.fromMongo(mongoOp);
        assertThat(op.getSqlFunction()).isEqualTo(sqlFunc);
    }

    @Test
    void shouldReturnMongoOperator() {
        assertThat(StringOp.CONCAT.getMongoOperator()).isEqualTo("$concat");
        assertThat(StringOp.TO_LOWER.getMongoOperator()).isEqualTo("$toLower");
        assertThat(StringOp.TO_UPPER.getMongoOperator()).isEqualTo("$toUpper");
    }

    @Test
    void shouldThrowForUnknownOperator() {
        assertThatThrownBy(() -> StringOp.fromMongo("$unknown"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown string operator");
    }

    @Test
    void shouldDetectStringOperator() {
        assertThat(StringOp.isStringOp("$concat")).isTrue();
        assertThat(StringOp.isStringOp("$toLower")).isTrue();
        assertThat(StringOp.isStringOp("$toUpper")).isTrue();
        assertThat(StringOp.isStringOp("$substr")).isTrue();
        assertThat(StringOp.isStringOp("$trim")).isTrue();
    }

    @Test
    void shouldReturnFalseForNonStringOp() {
        assertThat(StringOp.isStringOp("$eq")).isFalse();
        assertThat(StringOp.isStringOp("$sum")).isFalse();
        assertThat(StringOp.isStringOp("$add")).isFalse();
    }
}
