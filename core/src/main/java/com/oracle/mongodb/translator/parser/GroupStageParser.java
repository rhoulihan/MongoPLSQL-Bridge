/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.AccumulatorOp;
import com.oracle.mongodb.translator.ast.expression.CompoundIdExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
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
      accumulators.put(fieldName, parseAccumulator(value));
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

  private AccumulatorExpression parseAccumulator(Object value) {
    if (!(value instanceof Document)) {
      throw new IllegalArgumentException("Accumulator must be a document, got: " + value);
    }

    Document accDoc = (Document) value;
    if (accDoc.size() != 1) {
      throw new IllegalArgumentException(
          "Accumulator document must have exactly one operator, got: " + accDoc.keySet());
    }

    Map.Entry<String, Object> entry = accDoc.entrySet().iterator().next();
    String operator = entry.getKey();
    Object argument = entry.getValue();

    if (!AccumulatorOp.isAccumulator(operator)) {
      throw new UnsupportedOperatorException(operator);
    }

    AccumulatorOp accOp = AccumulatorOp.fromMongo(operator);
    Expression argumentExpr = parseAccumulatorArgument(argument);

    return new AccumulatorExpression(accOp, argumentExpr);
  }

  private Expression parseAccumulatorArgument(Object argument) {
    if (argument == null) {
      return null;
    }

    // Handle $count: {} or $count: 1
    if (argument instanceof Document doc && doc.isEmpty()) {
      return null;
    }

    if (argument instanceof String strVal) {
      if (strVal.startsWith("$")) {
        // Field reference
        return FieldPathExpression.of(strVal.substring(1));
      }
      return LiteralExpression.of(strVal);
    }

    if (argument instanceof Number) {
      // Literal number (e.g., { $sum: 1 } for counting)
      return LiteralExpression.of(argument);
    }

    if (argument instanceof Document) {
      // Complex expression (e.g., { $cond: [...] }, { $multiply: [...] })
      return expressionParser.parseValue(argument);
    }

    throw new IllegalArgumentException("Unsupported accumulator argument: " + argument);
  }
}
