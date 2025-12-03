/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.stage.ReplaceRootStage;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for ReplaceRootStageParser. */
class ReplaceRootStageParserTest {

  private ReplaceRootStageParser parser;
  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    parser = new ReplaceRootStageParser();
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldParseReplaceRootWithFieldPath() {
    var doc = Document.parse("{\"newRoot\": \"$subdocument\"}");

    ReplaceRootStage stage = parser.parse(doc);
    stage.render(context);

    assertThat(context.toSql()).contains("subdocument");
  }

  @Test
  void shouldParseReplaceRootWithNestedField() {
    var doc = Document.parse("{\"newRoot\": \"$nested.document\"}");

    ReplaceRootStage stage = parser.parse(doc);
    stage.render(context);

    assertThat(context.toSql()).contains("nested.document");
  }

  @Test
  void shouldParseReplaceRootWithMergeObjects() {
    var doc = Document.parse("{\"newRoot\": {\"$mergeObjects\": [\"$defaults\", \"$doc\"]}}");

    ReplaceRootStage stage = parser.parse(doc);
    stage.render(context);

    assertThat(context.toSql()).containsAnyOf("defaults", "JSON_MERGEPATCH");
  }

  @Test
  void shouldThrowOnMissingNewRoot() {
    var doc = Document.parse("{}");

    assertThatThrownBy(() -> parser.parse(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("newRoot");
  }

  @Test
  void shouldThrowOnNonDocument() {
    assertThatThrownBy(() -> parser.parse("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("document");
  }
}
