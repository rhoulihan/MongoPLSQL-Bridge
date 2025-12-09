/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.FillStage;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;

/**
 * Parser for the $fill pipeline stage.
 *
 * <p>MongoDB $fill fills null and missing values in documents.
 *
 * <p>Example:
 *
 * <pre>
 * {
 *   $fill: {
 *     partitionBy: "$state",
 *     sortBy: { "date": 1 },
 *     output: {
 *       "quantity": { method: "locf" },
 *       "price": { value: 0 }
 *     }
 *   }
 * }
 * </pre>
 */
public final class FillStageParser implements StageParser<FillStage> {

  @Override
  @SuppressWarnings("unchecked")
  public FillStage parse(Object stageValue) {
    if (stageValue == null) {
      throw new IllegalArgumentException(
          "$fill requires a document with 'output' field, got: null");
    }
    if (!(stageValue instanceof Document doc)) {
      throw new IllegalArgumentException(
          "$fill requires a document, got: " + stageValue.getClass().getSimpleName());
    }

    // Parse optional partitionBy
    String partitionBy = null;
    Object partitionByObj = doc.get("partitionBy");
    if (partitionByObj instanceof String s) {
      partitionBy = s;
    }

    // Parse optional sortBy
    Map<String, Integer> sortBy = null;
    Object sortByObj = doc.get("sortBy");
    if (sortByObj instanceof Document sortDoc) {
      sortBy = new LinkedHashMap<>();
      for (Map.Entry<String, Object> entry : sortDoc.entrySet()) {
        if (entry.getValue() instanceof Number num) {
          sortBy.put(entry.getKey(), num.intValue());
        }
      }
    }

    // Parse required output
    Object outputObj = doc.get("output");
    if (outputObj == null) {
      throw new IllegalArgumentException("$fill requires an 'output' field");
    }
    if (!(outputObj instanceof Document outputDoc)) {
      throw new IllegalArgumentException(
          "$fill 'output' must be a document, got: " + outputObj.getClass().getSimpleName());
    }

    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : outputDoc.entrySet()) {
      String fieldName = entry.getKey();
      Object fieldSpec = entry.getValue();

      if (!(fieldSpec instanceof Document specDoc)) {
        throw new IllegalArgumentException(
            "$fill output field '" + fieldName + "' must be a document");
      }

      String method = specDoc.getString("method");
      Object value = specDoc.get("value");

      output.put(fieldName, new FillStage.FillSpec(method, value));
    }

    return new FillStage(partitionBy, sortBy, output);
  }
}
