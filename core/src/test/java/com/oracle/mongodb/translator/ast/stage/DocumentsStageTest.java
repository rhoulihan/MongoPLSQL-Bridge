/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Tests for DocumentsStage AST node.
 *
 * <p>The $documents stage generates literal documents as input to a pipeline,
 * allowing pipelines to start with synthetic data rather than a collection.
 *
 * <p>MongoDB syntax:
 * <pre>
 * { $documents: [ { doc1 }, { doc2 }, ... ] }
 * </pre>
 *
 * <p>Oracle translation uses UNION ALL of SELECT FROM DUAL:
 * <pre>
 * SELECT JSON('{"x":"value"}') AS "DATA" FROM DUAL
 * UNION ALL
 * SELECT JSON('{"y":123}') AS "DATA" FROM DUAL
 * </pre>
 */
class DocumentsStageTest {

  @Test
  void shouldRenderSingleDocument() {
    var doc = new Document("x", "Andrew");
    var stage = new DocumentsStage(List.of(doc));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).contains("FROM DUAL");
    assertThat(sql).contains("Andrew");
  }

  @Test
  void shouldRenderMultipleDocumentsWithUnionAll() {
    var doc1 = new Document("x", "Andrew");
    var doc2 = new Document("x", "Dan");
    var stage = new DocumentsStage(List.of(doc1, doc2));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("Andrew");
    assertThat(sql).contains("Dan");
  }

  @Test
  void shouldRenderDocumentWithArrayValue() {
    var doc = new Document("x", List.of("Dev", "Eliot"));
    var stage = new DocumentsStage(List.of(doc));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("Dev");
    assertThat(sql).contains("Eliot");
  }

  @Test
  void shouldRenderDocumentWithNumericValue() {
    var doc = new Document("count", 42);
    var stage = new DocumentsStage(List.of(doc));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("42");
  }

  @Test
  void shouldRenderDocumentWithNestedDocument() {
    var nested = new Document("city", "New York");
    var doc = new Document("address", nested);
    var stage = new DocumentsStage(List.of(doc));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("address");
    assertThat(sql).contains("city");
    assertThat(sql).contains("New York");
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new DocumentsStage(List.of(new Document("x", 1)));

    assertThat(stage.getOperatorName()).isEqualTo("$documents");
  }

  @Test
  void shouldReturnDocuments() {
    var doc1 = new Document("a", 1);
    var doc2 = new Document("b", 2);
    var stage = new DocumentsStage(List.of(doc1, doc2));

    assertThat(stage.getDocuments()).hasSize(2);
    assertThat(stage.getDocuments().get(0).get("a")).isEqualTo(1);
    assertThat(stage.getDocuments().get(1).get("b")).isEqualTo(2);
  }

  @Test
  void shouldReturnDefensiveCopyOfDocuments() {
    var doc = new Document("x", 1);
    var stage = new DocumentsStage(List.of(doc));

    // Modifying the returned list should not affect the stage
    List<Document> docs = stage.getDocuments();
    assertThatThrownBy(() -> docs.add(new Document("y", 2)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void shouldRejectNullDocumentsList() {
    assertThatThrownBy(() -> new DocumentsStage(null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void shouldRejectEmptyDocumentsList() {
    assertThatThrownBy(() -> new DocumentsStage(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one document");
  }

  @Test
  void shouldProvideToString() {
    var doc = new Document("x", "test");
    var stage = new DocumentsStage(List.of(doc));

    assertThat(stage.toString()).contains("DocumentsStage");
    assertThat(stage.toString()).contains("1 document");
  }

  @Test
  void shouldProvidePluralToStringForMultipleDocuments() {
    var doc1 = new Document("x", 1);
    var doc2 = new Document("y", 2);
    var doc3 = new Document("z", 3);
    var stage = new DocumentsStage(List.of(doc1, doc2, doc3));

    assertThat(stage.toString()).contains("3 documents");
  }
}
