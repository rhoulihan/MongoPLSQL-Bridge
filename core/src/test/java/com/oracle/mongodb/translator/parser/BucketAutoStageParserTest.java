/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.BucketAutoStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BucketAutoStageParserTest {

  private BucketAutoStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new BucketAutoStageParser();
  }

  @Test
  void shouldParseBasicBucketAuto() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$price",
                "buckets": 5
            }
            """);

    BucketAutoStage stage = parser.parse(doc);

    assertThat(stage.getBuckets()).isEqualTo(5);
    assertThat(stage.getOutput()).isEmpty();
    assertThat(stage.hasGranularity()).isFalse();
  }

  @Test
  void shouldParseWithOutput() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$amount",
                "buckets": 10,
                "output": {
                    "count": { "$sum": 1 },
                    "average": { "$avg": "$value" }
                }
            }
            """);

    BucketAutoStage stage = parser.parse(doc);

    assertThat(stage.getOutput()).hasSize(2);
    assertThat(stage.getOutput()).containsKey("count");
    assertThat(stage.getOutput()).containsKey("average");
  }

  @Test
  void shouldParseWithGranularity() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$price",
                "buckets": 5,
                "granularity": "R5"
            }
            """);

    BucketAutoStage stage = parser.parse(doc);

    assertThat(stage.hasGranularity()).isTrue();
    assertThat(stage.getGranularity()).isEqualTo("R5");
  }

  @Test
  void shouldParseComplexGroupBy() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": { "$add": ["$price", 10] },
                "buckets": 3
            }
            """);

    BucketAutoStage stage = parser.parse(doc);

    assertThat(stage.getGroupBy()).isNotNull();
  }

  @Test
  void shouldThrowOnMissingGroupBy() {
    var doc =
        Document.parse(
            """
            {
                "buckets": 5
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("groupBy");
  }

  @Test
  void shouldThrowOnMissingBuckets() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$price"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("buckets");
  }

  @Test
  void shouldThrowOnNonNumericBuckets() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$price",
                "buckets": "five"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("buckets")
        .withMessageContaining("number");
  }

  @Test
  void shouldThrowOnZeroBuckets() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$price",
                "buckets": 0
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("positive");
  }

  @Test
  void shouldThrowOnNegativeBuckets() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$price",
                "buckets": -1
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("positive");
  }

  @Test
  void shouldThrowOnNonDocumentOutput() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$price",
                "buckets": 5,
                "output": "invalid"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("output")
        .withMessageContaining("document");
  }

  @Test
  void shouldThrowOnNonStringGranularity() {
    var doc =
        Document.parse(
            """
            {
                "groupBy": "$price",
                "buckets": 5,
                "granularity": 123
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("granularity")
        .withMessageContaining("string");
  }
}
