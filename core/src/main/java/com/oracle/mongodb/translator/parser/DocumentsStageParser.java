/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.DocumentsStage;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/**
 * Parser for the $documents aggregation stage.
 *
 * <p>Parses MongoDB $documents stage which accepts an array of documents:
 * <pre>
 * { $documents: [ { x: "Andrew" }, { x: "Dan" }, { x: ["Dev", "Eliot"] } ] }
 * </pre>
 */
public class DocumentsStageParser implements StageParser<DocumentsStage> {

  @Override
  public DocumentsStage parse(Object value) {
    if (value == null) {
      throw new IllegalArgumentException("$documents value must not be null");
    }
    if (!(value instanceof List<?> list)) {
      throw new IllegalArgumentException(
          "$documents requires an array of documents, got: " + value.getClass().getSimpleName());
    }
    if (list.isEmpty()) {
      throw new IllegalArgumentException("$documents requires at least one document");
    }

    List<Document> documents = new ArrayList<>(list.size());
    for (int i = 0; i < list.size(); i++) {
      Object item = list.get(i);
      if (!(item instanceof Document doc)) {
        throw new IllegalArgumentException(
            "$documents array element at index " + i + " must be a document, got: "
                + (item == null ? "null" : item.getClass().getSimpleName()));
      }
      documents.add(doc);
    }

    return new DocumentsStage(documents);
  }
}
