/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.FacetStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FacetStageParserTest {

  private FacetStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new FacetStageParser(new PipelineParser());
  }

  @Test
  void shouldParseSingleFacet() {
    var doc =
        Document.parse(
            """
            {
                "prices": [
                    { "$limit": 10 }
                ]
            }
            """);

    FacetStage stage = parser.parse(doc);

    assertThat(stage.getFacetNames()).containsExactly("prices");
    assertThat(stage.getFacetPipeline("prices")).hasSize(1);
    assertThat(stage.getFacetPipeline("prices").get(0)).isInstanceOf(LimitStage.class);
  }

  @Test
  void shouldParseMultipleFacets() {
    var doc =
        Document.parse(
            """
            {
                "categorizedByStatus": [
                    { "$match": { "status": "A" } },
                    { "$limit": 10 }
                ],
                "totalCount": [
                    { "$limit": 1 }
                ]
            }
            """);

    FacetStage stage = parser.parse(doc);

    assertThat(stage.getFacetNames())
        .containsExactlyInAnyOrder("categorizedByStatus", "totalCount");
    assertThat(stage.getFacetPipeline("categorizedByStatus")).hasSize(2);
    assertThat(stage.getFacetPipeline("categorizedByStatus").get(0)).isInstanceOf(MatchStage.class);
    assertThat(stage.getFacetPipeline("totalCount")).hasSize(1);
  }

  @Test
  void shouldParseEmptyFacetPipeline() {
    var doc =
        Document.parse(
            """
            {
                "emptyPipeline": []
            }
            """);

    FacetStage stage = parser.parse(doc);

    assertThat(stage.getFacetPipeline("emptyPipeline")).isEmpty();
  }

  @Test
  void shouldThrowOnEmptyDocument() {
    var doc = Document.parse("{}");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("at least one");
  }

  @Test
  void shouldThrowOnNonArrayFacet() {
    var doc =
        Document.parse(
            """
            {
                "badFacet": "not an array"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("array");
  }
}
