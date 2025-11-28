/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.MergeStage;
import com.oracle.mongodb.translator.ast.stage.MergeStage.WhenMatched;
import com.oracle.mongodb.translator.ast.stage.MergeStage.WhenNotMatched;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MergeStageParserTest {

  private MergeStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new MergeStageParser();
  }

  @Test
  void shouldParseSimpleStringForm() {
    MergeStage stage = parser.parse("outputCollection");

    assertThat(stage.getTargetCollection()).isEqualTo("outputCollection");
    assertThat(stage.getOnFields()).containsExactly("_id");
  }

  @Test
  void shouldParseDocumentFormWithIntoString() {
    var doc =
        Document.parse(
            """
            {
                "into": "targetCollection"
            }
            """);

    MergeStage stage = parser.parse(doc);

    assertThat(stage.getTargetCollection()).isEqualTo("targetCollection");
  }

  @Test
  void shouldParseDocumentFormWithIntoDocument() {
    var doc =
        Document.parse(
            """
            {
                "into": { "db": "mydb", "coll": "mycollection" }
            }
            """);

    MergeStage stage = parser.parse(doc);

    assertThat(stage.getTargetCollection()).isEqualTo("mycollection");
  }

  @Test
  void shouldParseOnFieldAsString() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "on": "customId"
            }
            """);

    MergeStage stage = parser.parse(doc);

    assertThat(stage.getOnFields()).containsExactly("customId");
  }

  @Test
  void shouldParseOnFieldAsArray() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "on": ["orderId", "customerId"]
            }
            """);

    MergeStage stage = parser.parse(doc);

    assertThat(stage.getOnFields()).containsExactly("orderId", "customerId");
  }

  @Test
  void shouldParseWhenMatchedOptions() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenMatched": "replace"
            }
            """);

    MergeStage stage = parser.parse(doc);
    assertThat(stage.getWhenMatched()).isEqualTo(WhenMatched.REPLACE);

    doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenMatched": "keepExisting"
            }
            """);
    stage = parser.parse(doc);
    assertThat(stage.getWhenMatched()).isEqualTo(WhenMatched.KEEP_EXISTING);
  }

  @Test
  void shouldParseWhenNotMatchedOptions() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenNotMatched": "discard"
            }
            """);

    MergeStage stage = parser.parse(doc);
    assertThat(stage.getWhenNotMatched()).isEqualTo(WhenNotMatched.DISCARD);
  }

  @Test
  void shouldThrowOnMissingInto() {
    var doc =
        Document.parse("""
            {
                "on": "_id"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("into");
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
  void shouldParseWhenMatchedMerge() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenMatched": "merge"
            }
            """);

    MergeStage stage = parser.parse(doc);
    assertThat(stage.getWhenMatched()).isEqualTo(WhenMatched.MERGE);
  }

  @Test
  void shouldParseWhenMatchedFail() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenMatched": "fail"
            }
            """);

    MergeStage stage = parser.parse(doc);
    assertThat(stage.getWhenMatched()).isEqualTo(WhenMatched.FAIL);
  }

  @Test
  void shouldParseWhenNotMatchedInsert() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenNotMatched": "insert"
            }
            """);

    MergeStage stage = parser.parse(doc);
    assertThat(stage.getWhenNotMatched()).isEqualTo(WhenNotMatched.INSERT);
  }

  @Test
  void shouldParseWhenNotMatchedFail() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenNotMatched": "fail"
            }
            """);

    MergeStage stage = parser.parse(doc);
    assertThat(stage.getWhenNotMatched()).isEqualTo(WhenNotMatched.FAIL);
  }

  @Test
  void shouldThrowOnUnknownWhenMatched() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenMatched": "unknown"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("Unknown whenMatched");
  }

  @Test
  void shouldThrowOnUnknownWhenNotMatched() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "whenNotMatched": "unknown"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("Unknown whenNotMatched");
  }

  @Test
  void shouldThrowOnInvalidIntoType() {
    var doc =
        Document.parse("""
            {
                "into": 123
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("into")
        .withMessageContaining("string or document");
  }

  @Test
  void shouldThrowOnInvalidIntoCollType() {
    var doc =
        Document.parse(
            """
            {
                "into": { "db": "mydb", "coll": 123 }
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("into.coll");
  }

  @Test
  void shouldThrowOnInvalidOnType() {
    var doc =
        Document.parse(
            """
            {
                "into": "target",
                "on": 123
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("on")
        .withMessageContaining("string or array");
  }

  @Test
  void shouldThrowOnInvalidOnArrayItem() {
    var doc = new Document().append("into", "target").append("on", java.util.List.of("valid", 123));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("on")
        .withMessageContaining("strings");
  }

  @Test
  void shouldThrowOnWhenMatchedPipeline() {
    var doc =
        new Document()
            .append("into", "target")
            .append("whenMatched", java.util.List.of(new Document("$set", new Document("x", 1))));

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("pipeline not yet supported");
  }

  @Test
  void shouldThrowOnInvalidWhenNotMatchedType() {
    var doc = new Document().append("into", "target").append("whenNotMatched", 123);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("whenNotMatched")
        .withMessageContaining("string");
  }
}
