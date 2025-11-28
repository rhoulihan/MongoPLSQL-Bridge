/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import org.bson.Document;

/**
 * Parser for the MongoDB $unwind stage.
 *
 * <p>Supports both simple and expanded formats:
 *
 * <pre>
 * // Simple format
 * { $unwind: "$items" }
 *
 * // Expanded format
 * { $unwind: {
 *     path: "$items",
 *     includeArrayIndex: "index",
 *     preserveNullAndEmptyArrays: true
 * }}
 * </pre>
 */
public final class UnwindStageParser {

  /**
   * Parses an $unwind stage value (either a string path or a document with options).
   *
   * @param value the BSON value (String or Document)
   * @return the parsed UnwindStage
   * @throws IllegalArgumentException if the value is invalid
   */
  public UnwindStage parse(Object value) {
    if (value instanceof String stringPath) {
      return parseStringPath(stringPath);
    } else if (value instanceof Document doc) {
      return parseDocument(doc);
    } else {
      throw new IllegalArgumentException(
          "$unwind requires a string path or document, got: "
              + (value == null ? "null" : value.getClass().getSimpleName()));
    }
  }

  private UnwindStage parseStringPath(String path) {
    String normalizedPath = normalizePath(path);
    return new UnwindStage(normalizedPath);
  }

  private UnwindStage parseDocument(Document doc) {
    // Get required path
    Object pathObj = doc.get("path");
    if (pathObj == null) {
      throw new IllegalArgumentException(
          "$unwind requires 'path' field when using document format");
    }
    if (!(pathObj instanceof String)) {
      throw new IllegalArgumentException(
          "$unwind 'path' must be a string, got: " + pathObj.getClass().getSimpleName());
    }
    String path = normalizePath((String) pathObj);

    // Get optional includeArrayIndex
    String includeArrayIndex = null;
    Object indexObj = doc.get("includeArrayIndex");
    if (indexObj != null) {
      if (!(indexObj instanceof String)) {
        throw new IllegalArgumentException(
            "$unwind 'includeArrayIndex' must be a string, got: "
                + indexObj.getClass().getSimpleName());
      }
      includeArrayIndex = (String) indexObj;
    }

    // Get optional preserveNullAndEmptyArrays
    boolean preserveNullAndEmptyArrays = false;
    Object preserveObj = doc.get("preserveNullAndEmptyArrays");
    if (preserveObj != null) {
      if (!(preserveObj instanceof Boolean)) {
        throw new IllegalArgumentException(
            "$unwind 'preserveNullAndEmptyArrays' must be a boolean, got: "
                + preserveObj.getClass().getSimpleName());
      }
      preserveNullAndEmptyArrays = (Boolean) preserveObj;
    }

    return new UnwindStage(path, includeArrayIndex, preserveNullAndEmptyArrays);
  }

  /** Normalizes a path by removing the leading $ if present. */
  private String normalizePath(String path) {
    if (path == null || path.isEmpty()) {
      throw new IllegalArgumentException("$unwind path must not be empty");
    }
    if (path.equals("$")) {
      throw new IllegalArgumentException("$unwind path must specify a field after $");
    }
    if (path.startsWith("$")) {
      return path.substring(1);
    }
    return path;
  }
}
