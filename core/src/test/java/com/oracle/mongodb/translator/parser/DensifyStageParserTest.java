/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.stage.DensifyStage;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for DensifyStageParser. */
class DensifyStageParserTest {

  private DensifyStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new DensifyStageParser();
  }

  @Test
  void shouldParseBasicDensify() {
    Document doc = Document.parse("""
        {
          "field": "timestamp",
          "range": {
            "step": 1,
            "bounds": "full"
          }
        }
        """);

    DensifyStage stage = parser.parse(doc);

    assertThat(stage.getField()).isEqualTo("timestamp");
    assertThat(stage.getPartitionByFields()).isEmpty();
    assertThat(stage.getRange().step()).isEqualTo(1);
    assertThat(stage.getRange().bounds()).isEqualTo("full");
  }

  @Test
  void shouldParseDensifyWithUnit() {
    Document doc = Document.parse("""
        {
          "field": "timestamp",
          "range": {
            "step": 1,
            "unit": "hour",
            "bounds": "full"
          }
        }
        """);

    DensifyStage stage = parser.parse(doc);

    assertThat(stage.getField()).isEqualTo("timestamp");
    assertThat(stage.getRange().step()).isEqualTo(1);
    assertThat(stage.getRange().unit()).isEqualTo("hour");
    assertThat(stage.getRange().bounds()).isEqualTo("full");
  }

  @Test
  void shouldParseDensifyWithPartition() {
    Document doc = Document.parse("""
        {
          "field": "timestamp",
          "partitionByFields": ["region", "category"],
          "range": {
            "step": 1,
            "unit": "day",
            "bounds": "partition"
          }
        }
        """);

    DensifyStage stage = parser.parse(doc);

    assertThat(stage.getField()).isEqualTo("timestamp");
    assertThat(stage.getPartitionByFields()).containsExactly("region", "category");
    assertThat(stage.getRange().bounds()).isEqualTo("partition");
  }

  @Test
  void shouldRejectNullInput() {
    assertThatThrownBy(() -> parser.parse(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$densify");
  }

  @Test
  void shouldRejectMissingField() {
    Document doc = Document.parse("""
        {
          "range": {
            "step": 1,
            "bounds": "full"
          }
        }
        """);

    assertThatThrownBy(() -> parser.parse(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("field");
  }

  @Test
  void shouldRejectMissingRange() {
    Document doc = Document.parse("""
        {
          "field": "timestamp"
        }
        """);

    assertThatThrownBy(() -> parser.parse(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("range");
  }

  @Test
  void shouldRejectMissingStep() {
    Document doc = Document.parse("""
        {
          "field": "timestamp",
          "range": {
            "bounds": "full"
          }
        }
        """);

    assertThatThrownBy(() -> parser.parse(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("step");
  }
}
