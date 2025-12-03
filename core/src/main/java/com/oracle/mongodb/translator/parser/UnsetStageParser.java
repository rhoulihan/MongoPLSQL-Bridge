/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.UnsetStage;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for $unset stage.
 *
 * <p>Parses:
 *
 * <ul>
 *   <li>{@code {$unset: "fieldToRemove"}} - single field
 *   <li>{@code {$unset: ["field1", "field2"]}} - multiple fields
 * </ul>
 */
public final class UnsetStageParser {

  /**
   * Parses an $unset stage.
   *
   * @param value the stage value (string or array of strings)
   * @return the parsed UnsetStage
   */
  public UnsetStage parse(Object value) {
    List<String> fields = new ArrayList<>();

    if (value instanceof String field) {
      // Single field: {$unset: "fieldName"}
      fields.add(field);
    } else if (value instanceof List<?> list) {
      // Multiple fields: {$unset: ["field1", "field2"]}
      for (Object item : list) {
        if (!(item instanceof String)) {
          throw new IllegalArgumentException(
              "$unset array must contain only strings, got: " + item.getClass().getSimpleName());
        }
        fields.add((String) item);
      }
    } else {
      throw new IllegalArgumentException(
          "$unset requires a string or array of strings, got: "
              + (value == null ? "null" : value.getClass().getSimpleName()));
    }

    if (fields.isEmpty()) {
      throw new IllegalArgumentException("$unset requires at least one field");
    }

    return new UnsetStage(fields);
  }
}
