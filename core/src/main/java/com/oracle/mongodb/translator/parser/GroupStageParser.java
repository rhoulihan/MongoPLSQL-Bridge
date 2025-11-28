/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.CompoundIdExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;

/** Parser for $group stage documents. */
public final class GroupStageParser {

  /**
   * Parses a $group stage document.
   *
   * @param groupDoc the $group document
   * @return GroupStage AST node
   */
  public GroupStage parse(Document groupDoc) {
    Expression idExpression = parseIdExpression(groupDoc.get("_id"));

    Map<String, AccumulatorExpression> accumulators = new LinkedHashMap<>();

    for (Map.Entry<String, Object> entry : groupDoc.entrySet()) {
      String fieldName = entry.getKey();
      if ("_id".equals(fieldName)) {
        continue; // Already processed
      }

      Object value = entry.getValue();
      if (!(value instanceof Document)) {
        throw new IllegalArgumentException("Accumulator must be a document, got: " + value);
      }
      accumulators.put(
          fieldName, AccumulatorParserUtil.parseAccumulator((Document) value, expressionParser));
    }

    return new GroupStage(idExpression, accumulators);
  }

  private final ExpressionParser expressionParser = new ExpressionParser();

  private Expression parseIdExpression(Object idValue) {
    if (idValue == null) {
      return null; // Group all documents
    }

    if (idValue instanceof String strVal) {
      if (strVal.startsWith("$")) {
        // Field reference: "$fieldName"
        return FieldPathExpression.of(strVal.substring(1));
      }
      // Literal string value
      return LiteralExpression.of(strVal);
    }

    if (idValue instanceof Document idDoc) {
      // Complex _id expression - could be multiple fields or expressions
      Map<String, Expression> fields = new LinkedHashMap<>();

      for (Map.Entry<String, Object> entry : idDoc.entrySet()) {
        String fieldName = entry.getKey();
        Object fieldValue = entry.getValue();

        // Parse each field value as an expression
        Expression expr = expressionParser.parseValue(fieldValue);
        fields.put(fieldName, expr);
      }

      // If single field, we could return a simple FieldPath, but for consistency
      // and proper aliasing, use CompoundIdExpression even for single fields
      return new CompoundIdExpression(fields);
    }

    // Literal value for _id
    return LiteralExpression.of(idValue);
  }
}
