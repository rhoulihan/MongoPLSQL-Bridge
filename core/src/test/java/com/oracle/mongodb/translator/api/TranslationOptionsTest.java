/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TranslationOptionsTest {

  @Test
  void shouldCreateDefaultOptions() {
    var options = TranslationOptions.defaults();

    assertThat(options.inlineBindVariables()).isFalse();
    assertThat(options.prettyPrint()).isFalse();
    assertThat(options.includeHints()).isTrue();
    assertThat(options.strictMode()).isFalse();
  }

  @Test
  void shouldBuildCustomOptions() {
    var options =
        TranslationOptions.builder()
            .inlineBindVariables(true)
            .prettyPrint(true)
            .strictMode(true)
            .includeHints(false)
            .build();

    assertThat(options.inlineBindVariables()).isTrue();
    assertThat(options.prettyPrint()).isTrue();
    assertThat(options.strictMode()).isTrue();
    assertThat(options.includeHints()).isFalse();
  }

  @Test
  void shouldSupportDefaultDataColumnName() {
    var options = TranslationOptions.defaults();

    assertThat(options.dataColumnName()).isEqualTo("data");
  }

  @Test
  void shouldAllowCustomDataColumnName() {
    var options = TranslationOptions.builder().dataColumnName("json_doc").build();

    assertThat(options.dataColumnName()).isEqualTo("json_doc");
  }

  @Test
  void shouldDefaultProceduralToFalse() {
    var options = TranslationOptions.defaults();

    assertThat(options.procedural()).isFalse();
    assertThat(options.autoProcedural()).isFalse();
  }

  @Test
  void shouldAllowExplicitProceduralMode() {
    var options = TranslationOptions.builder().procedural(true).build();

    assertThat(options.procedural()).isTrue();
  }

  @Test
  void shouldAllowAutoProceduralMode() {
    var options = TranslationOptions.builder().autoProcedural(true).build();

    assertThat(options.autoProcedural()).isTrue();
    // procedural() returns false because it's not explicitly set
    assertThat(options.procedural()).isFalse();
  }
}
