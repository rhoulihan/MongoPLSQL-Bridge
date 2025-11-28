/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.SampleStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SampleStageParserTest {

  private SampleStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new SampleStageParser();
  }

  @Test
  void shouldParseSampleWithSize() {
    var doc = Document.parse("{ \"size\": 10 }");

    SampleStage stage = parser.parse(doc);

    assertThat(stage).isNotNull();
    assertThat(stage.getSize()).isEqualTo(10);
  }

  @Test
  void shouldParseSampleWithLargeSize() {
    var doc = Document.parse("{ \"size\": 1000 }");

    SampleStage stage = parser.parse(doc);

    assertThat(stage.getSize()).isEqualTo(1000);
  }

  @Test
  void shouldReturnCorrectOperatorName() {
    var doc = Document.parse("{ \"size\": 5 }");

    SampleStage stage = parser.parse(doc);

    assertThat(stage.getOperatorName()).isEqualTo("$sample");
  }

  @Test
  void shouldThrowOnMissingSize() {
    var doc = Document.parse("{ }");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("size");
  }

  @Test
  void shouldThrowOnInvalidSizeType() {
    var doc = Document.parse("{ \"size\": \"ten\" }");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("size");
  }

  @Test
  void shouldThrowOnNonDocumentValue() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse("invalid"))
        .withMessageContaining("$sample");
  }

  @Test
  void shouldThrowOnNullValue() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(null))
        .withMessageContaining("$sample");
  }

  @Test
  void shouldThrowOnZeroSize() {
    var doc = Document.parse("{ \"size\": 0 }");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("positive");
  }

  @Test
  void shouldThrowOnNegativeSize() {
    var doc = Document.parse("{ \"size\": -5 }");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("positive");
  }
}
