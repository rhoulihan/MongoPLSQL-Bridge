/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProjectStageParserTest {

  private ProjectStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new ProjectStageParser();
  }

  @Test
  void shouldParseSimpleInclusion() {
    // { $project: { name: 1 } }
    var doc = new Document("name", 1);

    var stage = parser.parse(doc);

    assertThat(stage.getProjections()).containsKey("name");
    assertThat(stage.getProjections().get("name").isExcluded()).isFalse();
  }

  @Test
  void shouldParseMultipleInclusions() {
    // { $project: { name: 1, status: 1, amount: 1 } }
    var doc = new Document().append("name", 1).append("status", 1).append("amount", 1);

    var stage = parser.parse(doc);

    assertThat(stage.getProjections()).hasSize(3);
  }

  @Test
  void shouldParseBooleanInclusion() {
    // { $project: { name: true } }
    var doc = new Document("name", true);

    var stage = parser.parse(doc);

    assertThat(stage.getProjections().get("name").isExcluded()).isFalse();
  }

  @Test
  void shouldParseExclusion() {
    // { $project: { _id: 0 } }
    var doc = new Document("_id", 0);

    var stage = parser.parse(doc);

    assertThat(stage.getProjections().get("_id").isExcluded()).isTrue();
  }

  @Test
  void shouldParseBooleanExclusion() {
    // { $project: { _id: false } }
    var doc = new Document("_id", false);

    var stage = parser.parse(doc);

    assertThat(stage.getProjections().get("_id").isExcluded()).isTrue();
  }

  @Test
  void shouldParseFieldReference() {
    // { $project: { userName: "$name" } }
    var doc = new Document("userName", "$name");

    var stage = parser.parse(doc);

    assertThat(stage.getProjections().get("userName").isExcluded()).isFalse();
    assertThat(stage.getProjections().get("userName").getExpression()).isNotNull();
  }

  @Test
  void shouldParseLiteralString() {
    // { $project: { type: "user" } }
    var doc = new Document("type", "user");

    var stage = parser.parse(doc);

    assertThat(stage.getProjections().get("type").isExcluded()).isFalse();
  }

  @Test
  void shouldParseMixedInclusionAndExclusion() {
    // { $project: { _id: 0, name: 1, status: 1 } }
    var doc = new Document().append("_id", 0).append("name", 1).append("status", 1);

    var stage = parser.parse(doc);

    assertThat(stage.getProjections().get("_id").isExcluded()).isTrue();
    assertThat(stage.getProjections().get("name").isExcluded()).isFalse();
    assertThat(stage.isExclusionMode()).isFalse();
  }

  @Test
  void shouldDetectExclusionOnlyMode() {
    // { $project: { password: 0, secret: 0 } }
    var doc = new Document().append("password", 0).append("secret", 0);

    var stage = parser.parse(doc);

    assertThat(stage.isExclusionMode()).isTrue();
  }

  @Test
  void shouldRejectUnsupportedExpressionOperator() {
    // { $project: { computed: { $zip: {...} } } } - $zip is not supported
    var doc =
        new Document(
            "computed",
            new Document(
                "$zip",
                new Document(
                    "inputs",
                    java.util.List.of(java.util.List.of(1, 2), java.util.List.of("a", "b")))));

    assertThatThrownBy(() -> parser.parse(doc))
        .isInstanceOf(com.oracle.mongodb.translator.exception.UnsupportedOperatorException.class);
  }
}
