/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TranslationWarningTest {

    @Test
    void shouldCreateWarningWithCodeAndMessage() {
        var warning = TranslationWarning.of("PERF001", "Query may be slow without index");

        assertThat(warning.code()).isEqualTo("PERF001");
        assertThat(warning.message()).isEqualTo("Query may be slow without index");
    }

    @Test
    void shouldProvideReadableString() {
        var warning = TranslationWarning.of("WARN002", "Deprecated operator");

        assertThat(warning.toString()).contains("WARN002");
        assertThat(warning.toString()).contains("Deprecated operator");
    }
}
