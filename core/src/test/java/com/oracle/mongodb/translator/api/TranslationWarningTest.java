/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

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

  @Test
  void shouldBeEqualWhenCodeAndMessageMatch() {
    var warning1 = TranslationWarning.of("WARN001", "Test message");
    var warning2 = TranslationWarning.of("WARN001", "Test message");

    assertThat(warning1).isEqualTo(warning2);
    assertThat(warning1.hashCode()).isEqualTo(warning2.hashCode());
  }

  @Test
  void shouldNotBeEqualWhenCodeDiffers() {
    var warning1 = TranslationWarning.of("WARN001", "Test message");
    var warning2 = TranslationWarning.of("WARN002", "Test message");

    assertThat(warning1).isNotEqualTo(warning2);
  }

  @Test
  void shouldNotBeEqualWhenMessageDiffers() {
    var warning1 = TranslationWarning.of("WARN001", "Test message 1");
    var warning2 = TranslationWarning.of("WARN001", "Test message 2");

    assertThat(warning1).isNotEqualTo(warning2);
  }

  @Test
  void shouldNotBeEqualToNull() {
    var warning = TranslationWarning.of("WARN001", "Test message");

    assertThat(warning).isNotEqualTo(null);
  }

  @Test
  void shouldNotBeEqualToDifferentType() {
    var warning = TranslationWarning.of("WARN001", "Test message");

    assertThat(warning).isNotEqualTo("some string");
  }

  @Test
  void shouldBeEqualToSelf() {
    var warning = TranslationWarning.of("WARN001", "Test message");

    assertThat(warning).isEqualTo(warning);
  }

  @Test
  void shouldThrowOnNullCode() {
    assertThatNullPointerException()
        .isThrownBy(() -> TranslationWarning.of(null, "message"))
        .withMessageContaining("code");
  }

  @Test
  void shouldThrowOnNullMessage() {
    assertThatNullPointerException()
        .isThrownBy(() -> TranslationWarning.of("CODE", null))
        .withMessageContaining("message");
  }

  @Test
  void shouldFormatToStringCorrectly() {
    var warning = TranslationWarning.of("TEST001", "This is a test");

    assertThat(warning.toString()).isEqualTo("[TEST001] This is a test");
  }
}
