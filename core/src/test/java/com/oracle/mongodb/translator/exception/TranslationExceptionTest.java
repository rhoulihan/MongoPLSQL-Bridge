/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TranslationExceptionTest {

    @Test
    void shouldCreateWithMessage() {
        var exception = new TranslationException("Translation failed");

        assertThat(exception.getMessage()).isEqualTo("Translation failed");
    }

    @Test
    void shouldCreateWithCause() {
        var cause = new RuntimeException("root cause");
        var exception = new TranslationException("Translation failed", cause);

        assertThat(exception.getMessage()).isEqualTo("Translation failed");
        assertThat(exception.getCause()).isSameAs(cause);
    }
}
