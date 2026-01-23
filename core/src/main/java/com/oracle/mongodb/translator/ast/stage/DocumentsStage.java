/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.bson.Document;

/**
 * Represents a $documents stage that generates literal documents as pipeline input.
 *
 * <p>The $documents stage allows pipelines to start with synthetic data rather than
 * reading from a collection. This is useful for testing, generating seed data, or
 * providing explicit input documents to subsequent stages like $graphLookup.
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
public final class DocumentsStage implements Stage {

  private final List<Document> documents;

  /**
   * Creates a documents stage with the specified literal documents.
   *
   * @param documents the list of documents to generate (must not be null or empty)
   * @throws NullPointerException if documents is null
   * @throws IllegalArgumentException if documents is empty
   */
  public DocumentsStage(List<Document> documents) {
    Objects.requireNonNull(documents, "documents must not be null");
    if (documents.isEmpty()) {
      throw new IllegalArgumentException("$documents requires at least one document");
    }
    // Make defensive copy
    this.documents = List.copyOf(documents);
  }

  /**
   * Returns an unmodifiable view of the documents.
   *
   * @return the list of documents
   */
  public List<Document> getDocuments() {
    return documents;
  }

  @Override
  public String getOperatorName() {
    return "$documents";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    boolean first = true;
    for (Document doc : documents) {
      if (!first) {
        ctx.sql(" UNION ALL ");
      }
      ctx.sql("SELECT JSON('");
      // Escape single quotes in JSON by doubling them
      String json = doc.toJson().replace("'", "''");
      ctx.sql(json);
      ctx.sql("') AS \"DATA\" FROM DUAL");
      first = false;
    }
  }

  @Override
  public String toString() {
    int count = documents.size();
    String plural = count == 1 ? "document" : "documents";
    return "DocumentsStage(" + count + " " + plural + ")";
  }
}
