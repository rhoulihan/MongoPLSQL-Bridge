/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.OutStage;
import org.bson.Document;

/**
 * Parser for the MongoDB $out stage.
 *
 * <p>Expected format:
 *
 * <pre>
 * // Simple form
 * { $out: "outputCollection" }
 *
 * // Document form
 * {
 *   $out: {
 *     db: "database",
 *     coll: "collection"
 *   }
 * }
 * </pre>
 */
public final class OutStageParser {

  /**
   * Parses a $out stage from the given value.
   *
   * @param value the stage value (String or Document)
   * @return the parsed OutStage
   * @throws IllegalArgumentException if the value is invalid
   */
  public OutStage parse(Object value) {
    if (value instanceof String targetCollection) {
      return new OutStage(targetCollection);
    }

    if (value instanceof Document doc) {
      return parseDocumentForm(doc);
    }

    throw new IllegalArgumentException(
        "$out requires a string or document, got: "
            + (value == null ? "null" : value.getClass().getSimpleName()));
  }

  private OutStage parseDocumentForm(Document doc) {
    // Parse 'coll' field - required
    Object collValue = doc.get("coll");
    if (collValue == null) {
      throw new IllegalArgumentException("$out document form requires 'coll' field");
    }
    if (!(collValue instanceof String)) {
      throw new IllegalArgumentException(
          "$out 'coll' must be a string, got: " + collValue.getClass().getSimpleName());
    }
    String targetCollection = (String) collValue;

    // Parse 'db' field - optional
    String targetDatabase = null;
    Object dbValue = doc.get("db");
    if (dbValue != null) {
      if (!(dbValue instanceof String)) {
        throw new IllegalArgumentException(
            "$out 'db' must be a string, got: " + dbValue.getClass().getSimpleName());
      }
      targetDatabase = (String) dbValue;
    }

    return new OutStage(targetCollection, targetDatabase);
  }
}
