/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class UnsupportedOperatorExceptionTest {

    @Test
    void shouldContainOperatorName() {
        var exception = new UnsupportedOperatorException("$graphLookup");

        assertThat(exception.getOperatorName()).isEqualTo("$graphLookup");
        assertThat(exception.getMessage()).contains("$graphLookup");
    }

    @Test
    void shouldIndicateIfPartiallySupported() {
        var exception = new UnsupportedOperatorException("$lookup", true);

        assertThat(exception.isPartiallySupported()).isTrue();
    }

    @Test
    void shouldDefaultToNotPartiallySupported() {
        var exception = new UnsupportedOperatorException("$unknown");

        assertThat(exception.isPartiallySupported()).isFalse();
    }
}
