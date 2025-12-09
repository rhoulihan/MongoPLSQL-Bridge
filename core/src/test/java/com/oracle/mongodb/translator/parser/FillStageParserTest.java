/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.stage.FillStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for FillStageParser. */
class FillStageParserTest {

  private FillStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new FillStageParser();
  }

  @Test
  void shouldParseBasicFillWithMethod() {
    Document doc =
        Document.parse(
            """
        {
          "output": {
            "quantity": { "method": "locf" }
          }
        }
        """);

    FillStage stage = parser.parse(doc);

    assertThat(stage.getOutput()).hasSize(1);
    assertThat(stage.getOutput().get("quantity").method()).isEqualTo("locf");
    assertThat(stage.getOutput().get("quantity").value()).isNull();
  }

  @Test
  void shouldParseFillWithConstantValue() {
    Document doc =
        Document.parse(
            """
        {
          "output": {
            "price": { "value": 0 }
          }
        }
        """);

    FillStage stage = parser.parse(doc);

    assertThat(stage.getOutput()).hasSize(1);
    assertThat(stage.getOutput().get("price").value()).isEqualTo(0);
    assertThat(stage.getOutput().get("price").method()).isNull();
  }

  @Test
  void shouldParseFillWithPartitionBy() {
    Document doc =
        Document.parse(
            """
        {
          "partitionBy": "$region",
          "output": {
            "quantity": { "method": "locf" }
          }
        }
        """);

    FillStage stage = parser.parse(doc);

    assertThat(stage.getPartitionBy()).isEqualTo("$region");
  }

  @Test
  void shouldParseFillWithSortBy() {
    Document doc =
        Document.parse(
            """
        {
          "sortBy": { "date": 1, "time": -1 },
          "output": {
            "quantity": { "method": "locf" }
          }
        }
        """);

    FillStage stage = parser.parse(doc);

    assertThat(stage.getSortBy()).containsEntry("date", 1);
    assertThat(stage.getSortBy()).containsEntry("time", -1);
  }

  @Test
  void shouldParseFillWithMultipleOutputs() {
    Document doc =
        Document.parse(
            """
        {
          "output": {
            "quantity": { "method": "locf" },
            "price": { "value": 0 },
            "total": { "method": "linear" }
          }
        }
        """);

    FillStage stage = parser.parse(doc);

    assertThat(stage.getOutput()).hasSize(3);
  }

  @Test
  void shouldRejectNullInput() {
    assertThatThrownBy(() -> parser.parse(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$fill");
  }

  @Test
  void shouldRejectMissingOutput() {
    Document doc = Document.parse("{}");

    assertThatThrownBy(() -> parser.parse(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("output");
  }
}
