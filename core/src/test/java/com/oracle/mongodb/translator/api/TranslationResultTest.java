/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class TranslationResultTest {

  @Test
  void shouldCreateResultWithSqlAndBindVariables() {
    var result =
        TranslationResult.of(
            "SELECT * FROM orders WHERE JSON_VALUE(data, '$.status') = :1", List.of("active"));

    assertThat(result.sql()).contains("SELECT");
    assertThat(result.bindVariables()).containsExactly("active");
    assertThat(result.hasWarnings()).isFalse();
  }

  @Test
  void shouldCreateResultWithWarnings() {
    var result =
        TranslationResult.of(
            "SELECT * FROM orders",
            List.of(),
            List.of(TranslationWarning.of("PERF001", "Consider adding index")));

    assertThat(result.hasWarnings()).isTrue();
    assertThat(result.warnings()).hasSize(1);
    assertThat(result.warnings().get(0).code()).isEqualTo("PERF001");
  }

  @Test
  void shouldCreateResultWithEmptyWarnings() {
    var result = TranslationResult.of("SELECT 1 FROM dual", List.of());

    assertThat(result.warnings()).isEmpty();
    assertThat(result.hasWarnings()).isFalse();
  }

  @Test
  void shouldReturnCapability() {
    var result =
        TranslationResult.of(
            "SELECT * FROM orders", List.of(), List.of(), TranslationCapability.FULL_SUPPORT);

    assertThat(result.capability()).isEqualTo(TranslationCapability.FULL_SUPPORT);
  }

  @Test
  void shouldDefaultToFullSupport() {
    var result = TranslationResult.of("SELECT 1 FROM dual", List.of());

    assertThat(result.capability()).isEqualTo(TranslationCapability.FULL_SUPPORT);
  }
}
