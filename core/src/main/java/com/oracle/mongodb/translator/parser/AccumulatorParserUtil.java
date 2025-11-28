/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.AccumulatorOp;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;

/**
 * Utility class for parsing MongoDB accumulator expressions used by $group, $bucket, and
 * $bucketAuto stages.
 */
public final class AccumulatorParserUtil {

  private AccumulatorParserUtil() {
    // Utility class - prevent instantiation
  }

  /**
   * Parses an output document containing accumulator expressions.
   *
   * @param outputDoc the output document from $bucket or $bucketAuto
   * @param expressionParser parser for complex expressions
   * @param stageName the stage name for error messages (e.g., "$bucket")
   * @return map of field names to accumulator expressions
   * @throws IllegalArgumentException if the document is invalid
   */
  public static Map<String, AccumulatorExpression> parseOutput(
      Document outputDoc, ExpressionParser expressionParser, String stageName) {
    Map<String, AccumulatorExpression> result = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : outputDoc.entrySet()) {
      String fieldName = entry.getKey();
      Object fieldValue = entry.getValue();
      if (!(fieldValue instanceof Document)) {
        throw new IllegalArgumentException(
            stageName + " output field '" + fieldName + "' must be an accumulator document");
      }
      result.put(fieldName, parseAccumulator((Document) fieldValue, expressionParser));
    }
    return result;
  }

  /**
   * Parses a single accumulator expression document.
   *
   * @param accDoc the accumulator document (e.g., {"$sum": 1})
   * @param expressionParser parser for complex expressions
   * @return the parsed AccumulatorExpression
   * @throws IllegalArgumentException if the document is invalid
   * @throws UnsupportedOperatorException if the accumulator operator is not supported
   */
  public static AccumulatorExpression parseAccumulator(
      Document accDoc, ExpressionParser expressionParser) {
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
    Expression argumentExpr = parseAccumulatorArgument(argument, expressionParser);

    return new AccumulatorExpression(accOp, argumentExpr);
  }

  /**
   * Parses an accumulator argument value.
   *
   * @param argument the argument value (field path, literal, or complex expression)
   * @param expressionParser parser for complex expressions
   * @return the parsed Expression
   * @throws IllegalArgumentException if the argument type is not supported
   */
  public static Expression parseAccumulatorArgument(
      Object argument, ExpressionParser expressionParser) {
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
