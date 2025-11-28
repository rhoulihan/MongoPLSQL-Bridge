/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.GraphLookupStage;
import org.bson.Document;

/**
 * Parser for the MongoDB $graphLookup stage.
 *
 * <p>Expected format:
 *
 * <pre>
 * {
 *   "from": "employees",
 *   "startWith": "$reportsTo",
 *   "connectFromField": "reportsTo",
 *   "connectToField": "name",
 *   "as": "reportingHierarchy",
 *   "maxDepth": 5,
 *   "depthField": "level"
 * }
 * </pre>
 */
public final class GraphLookupStageParser {

  /**
   * Parses a $graphLookup stage document.
   *
   * @param doc the BSON document containing graph lookup parameters
   * @return the parsed GraphLookupStage
   * @throws IllegalArgumentException if required fields are missing or invalid
   */
  public GraphLookupStage parse(Document doc) {
    final String from = getRequiredString(doc, "from");
    final String startWith = parseStartWith(doc.get("startWith"));
    final String connectFromField = getRequiredString(doc, "connectFromField");
    final String connectToField = getRequiredString(doc, "connectToField");
    final String as = getRequiredString(doc, "as");

    Integer maxDepth = null;
    Object maxDepthValue = doc.get("maxDepth");
    if (maxDepthValue != null) {
      if (maxDepthValue instanceof Number) {
        maxDepth = ((Number) maxDepthValue).intValue();
      } else {
        throw new IllegalArgumentException(
            "$graphLookup 'maxDepth' must be a number, got: "
                + maxDepthValue.getClass().getSimpleName());
      }
    }

    String depthField = null;
    Object depthFieldValue = doc.get("depthField");
    if (depthFieldValue != null) {
      if (depthFieldValue instanceof String) {
        depthField = (String) depthFieldValue;
      } else {
        throw new IllegalArgumentException(
            "$graphLookup 'depthField' must be a string, got: "
                + depthFieldValue.getClass().getSimpleName());
      }
    }

    Document restrictSearchWithMatch = null;
    Object restrictValue = doc.get("restrictSearchWithMatch");
    if (restrictValue != null) {
      if (restrictValue instanceof Document) {
        restrictSearchWithMatch = (Document) restrictValue;
      } else {
        throw new IllegalArgumentException(
            "$graphLookup 'restrictSearchWithMatch' must be a document, got: "
                + restrictValue.getClass().getSimpleName());
      }
    }

    return new GraphLookupStage(
        from,
        startWith,
        connectFromField,
        connectToField,
        as,
        maxDepth,
        depthField,
        restrictSearchWithMatch);
  }

  private String parseStartWith(Object value) {
    if (value == null) {
      throw new IllegalArgumentException("$graphLookup requires 'startWith' field");
    }
    if (value instanceof String) {
      return (String) value;
    }
    // For complex expressions, just convert to string representation for now
    return value.toString();
  }

  private String getRequiredString(Document doc, String fieldName) {
    Object value = doc.get(fieldName);
    if (value == null) {
      throw new IllegalArgumentException("$graphLookup requires '" + fieldName + "' field");
    }
    if (!(value instanceof String)) {
      throw new IllegalArgumentException(
          "$graphLookup '"
              + fieldName
              + "' must be a string, got: "
              + value.getClass().getSimpleName());
    }
    return (String) value;
  }
}
