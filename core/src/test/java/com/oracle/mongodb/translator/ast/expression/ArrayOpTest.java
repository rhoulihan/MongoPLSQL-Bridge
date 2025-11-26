/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class ArrayOpTest {

    @Test
    void shouldMapArrayElemAt() {
        ArrayOp op = ArrayOp.fromMongo("$arrayElemAt");
        assertThat(op).isEqualTo(ArrayOp.ARRAY_ELEM_AT);
    }

    @Test
    void shouldMapSize() {
        ArrayOp op = ArrayOp.fromMongo("$size");
        assertThat(op).isEqualTo(ArrayOp.SIZE);
    }

    @Test
    void shouldMapFirst() {
        ArrayOp op = ArrayOp.fromMongo("$first");
        assertThat(op).isEqualTo(ArrayOp.FIRST);
    }

    @Test
    void shouldMapLast() {
        ArrayOp op = ArrayOp.fromMongo("$last");
        assertThat(op).isEqualTo(ArrayOp.LAST);
    }

    @Test
    void shouldReturnMongoOperator() {
        assertThat(ArrayOp.ARRAY_ELEM_AT.getMongoOperator()).isEqualTo("$arrayElemAt");
        assertThat(ArrayOp.SIZE.getMongoOperator()).isEqualTo("$size");
        assertThat(ArrayOp.FIRST.getMongoOperator()).isEqualTo("$first");
        assertThat(ArrayOp.LAST.getMongoOperator()).isEqualTo("$last");
    }

    @Test
    void shouldThrowForUnknownOperator() {
        assertThatThrownBy(() -> ArrayOp.fromMongo("$unknown"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unknown array operator");
    }

    @Test
    void shouldDetectArrayOperator() {
        assertThat(ArrayOp.isArrayOp("$arrayElemAt")).isTrue();
        assertThat(ArrayOp.isArrayOp("$size")).isTrue();
        assertThat(ArrayOp.isArrayOp("$first")).isTrue();
        assertThat(ArrayOp.isArrayOp("$last")).isTrue();
    }

    @Test
    void shouldReturnFalseForNonArrayOp() {
        assertThat(ArrayOp.isArrayOp("$eq")).isFalse();
        assertThat(ArrayOp.isArrayOp("$sum")).isFalse();
        assertThat(ArrayOp.isArrayOp("$concat")).isFalse();
    }
}
