/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.UnionWithStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnionWithStageParserTest {

  private UnionWithStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new UnionWithStageParser();
  }

  @Test
  void shouldParseSimpleStringForm() {
    UnionWithStage stage = parser.parse("inventory");

    assertThat(stage.getCollection()).isEqualTo("inventory");
    assertThat(stage.hasPipeline()).isFalse();
  }

  @Test
  void shouldParseDocumentFormWithCollectionOnly() {
    var doc =
        Document.parse(
            """
            {
                "coll": "inventory"
            }
            """);

    UnionWithStage stage = parser.parse(doc);

    assertThat(stage.getCollection()).isEqualTo("inventory");
    assertThat(stage.hasPipeline()).isFalse();
  }

  @Test
  void shouldParseDocumentFormWithEmptyPipeline() {
    var doc =
        Document.parse(
            """
            {
                "coll": "inventory",
                "pipeline": []
            }
            """);

    UnionWithStage stage = parser.parse(doc);

    assertThat(stage.getCollection()).isEqualTo("inventory");
    assertThat(stage.hasPipeline()).isFalse();
    assertThat(stage.getPipeline()).isEmpty();
  }

  @Test
  void shouldParseDocumentFormWithPipeline() {
    var doc =
        Document.parse(
            """
            {
                "coll": "products",
                "pipeline": [
                    { "$match": { "active": true } },
                    { "$limit": 10 }
                ]
            }
            """);

    UnionWithStage stage = parser.parse(doc);

    assertThat(stage.getCollection()).isEqualTo("products");
    assertThat(stage.hasPipeline()).isTrue();
    assertThat(stage.getPipeline()).hasSize(2);
    assertThat(stage.getPipeline().get(0)).isInstanceOf(MatchStage.class);
    assertThat(stage.getPipeline().get(1)).isInstanceOf(LimitStage.class);
  }

  @Test
  void shouldThrowOnNullValue() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(null))
        .withMessageContaining("string or document");
  }

  @Test
  void shouldThrowOnInvalidType() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(123))
        .withMessageContaining("string or document");
  }

  @Test
  void shouldThrowOnMissingCollInDocumentForm() {
    var doc =
        Document.parse(
            """
            {
                "pipeline": []
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("coll");
  }

  @Test
  void shouldThrowOnNonStringColl() {
    var doc =
        Document.parse("""
            {
                "coll": 123
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("coll")
        .withMessageContaining("string");
  }

  @Test
  void shouldThrowOnNonArrayPipeline() {
    var doc =
        Document.parse(
            """
            {
                "coll": "inventory",
                "pipeline": "invalid"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("pipeline")
        .withMessageContaining("array");
  }

  @Test
  void shouldWorkWithPipelineParserInjected() {
    PipelineParser pipelineParser = new PipelineParser();
    UnionWithStageParser parserWithInjection = new UnionWithStageParser(pipelineParser);

    var doc =
        Document.parse(
            """
            {
                "coll": "products",
                "pipeline": [
                    { "$limit": 5 }
                ]
            }
            """);

    UnionWithStage stage = parserWithInjection.parse(doc);

    assertThat(stage.getCollection()).isEqualTo("products");
    assertThat(stage.getPipeline()).hasSize(1);
  }
}
