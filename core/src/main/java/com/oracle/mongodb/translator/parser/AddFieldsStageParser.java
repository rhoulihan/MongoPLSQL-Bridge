/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.stage.AddFieldsStage;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;

/**
 * Parser for the MongoDB $addFields and $set stages.
 *
 * <p>Parses field specifications like:
 *
 * <pre>
 * { $addFields: { totalWithTax: { $add: ["$total", "$tax"] }, status: "processed" } }
 * { $set: { newField: "$existingField" } }
 * </pre>
 */
public final class AddFieldsStageParser {

  private final ExpressionParser expressionParser;

  public AddFieldsStageParser() {
    this.expressionParser = new ExpressionParser();
  }

  /**
   * Parses an $addFields or $set stage document.
   *
   * @param doc the BSON document containing field specifications
   * @return the parsed AddFieldsStage
   * @throws IllegalArgumentException if the document is invalid
   */
  public AddFieldsStage parse(Document doc) {
    if (doc.isEmpty()) {
      throw new IllegalArgumentException("$addFields requires at least one field specification");
    }

    Map<String, Expression> fields = new LinkedHashMap<>();

    for (Map.Entry<String, Object> entry : doc.entrySet()) {
      String fieldName = entry.getKey();
      Object value = entry.getValue();

      Expression expr = expressionParser.parseValue(value);
      fields.put(fieldName, expr);
    }

    return new AddFieldsStage(fields);
  }
}
