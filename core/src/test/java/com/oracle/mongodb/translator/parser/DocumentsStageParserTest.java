/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.DocumentsStage;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for DocumentsStageParser.
 *
 * <p>The $documents stage accepts an array of documents:
 * <pre>
 * { $documents: [ { x: "Andrew" }, { x: "Dan" }, { x: ["Dev", "Eliot"] } ] }
 * </pre>
 */
class DocumentsStageParserTest {

  private DocumentsStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new DocumentsStageParser();
  }

  @Test
  void shouldParseSingleDocument() {
    List<Document> docs = List.of(new Document("x", "Andrew"));

    DocumentsStage stage = parser.parse(docs);

    assertThat(stage).isNotNull();
    assertThat(stage.getDocuments()).hasSize(1);
    assertThat(stage.getDocuments().get(0).get("x")).isEqualTo("Andrew");
  }

  @Test
  void shouldParseMultipleDocuments() {
    List<Document> docs = List.of(
        new Document("x", "Andrew"),
        new Document("x", "Dan"),
        new Document("x", List.of("Dev", "Eliot"))
    );

    DocumentsStage stage = parser.parse(docs);

    assertThat(stage.getDocuments()).hasSize(3);
    assertThat(stage.getDocuments().get(0).get("x")).isEqualTo("Andrew");
    assertThat(stage.getDocuments().get(1).get("x")).isEqualTo("Dan");
    assertThat(stage.getDocuments().get(2).get("x")).isEqualTo(List.of("Dev", "Eliot"));
  }

  @Test
  void shouldParseDocumentWithNestedStructure() {
    Document nested = new Document("city", "New York").append("zip", "10001");
    List<Document> docs = List.of(new Document("address", nested));

    DocumentsStage stage = parser.parse(docs);

    assertThat(stage.getDocuments()).hasSize(1);
    Document address = (Document) stage.getDocuments().get(0).get("address");
    assertThat(address.get("city")).isEqualTo("New York");
    assertThat(address.get("zip")).isEqualTo("10001");
  }

  @Test
  void shouldParseDocumentWithMixedTypes() {
    Document doc = new Document("name", "Test")
        .append("count", 42)
        .append("active", true)
        .append("score", 3.14);
    List<Document> docs = List.of(doc);

    DocumentsStage stage = parser.parse(docs);

    Document parsed = stage.getDocuments().get(0);
    assertThat(parsed.get("name")).isEqualTo("Test");
    assertThat(parsed.get("count")).isEqualTo(42);
    assertThat(parsed.get("active")).isEqualTo(true);
    assertThat(parsed.get("score")).isEqualTo(3.14);
  }

  @Test
  void shouldReturnCorrectOperatorName() {
    List<Document> docs = List.of(new Document("x", 1));

    DocumentsStage stage = parser.parse(docs);

    assertThat(stage.getOperatorName()).isEqualTo("$documents");
  }

  @Test
  void shouldThrowOnNullValue() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(null))
        .withMessageContaining("$documents");
  }

  @Test
  void shouldThrowOnEmptyList() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(List.of()))
        .withMessageContaining("$documents")
        .withMessageContaining("at least one");
  }

  @Test
  void shouldThrowOnNonListValue() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse("not a list"))
        .withMessageContaining("$documents")
        .withMessageContaining("array");
  }

  @Test
  void shouldThrowOnListWithNonDocumentElements() {
    List<Object> mixedList = List.of(new Document("x", 1), "not a document");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(mixedList))
        .withMessageContaining("$documents")
        .withMessageContaining("document");
  }
}
