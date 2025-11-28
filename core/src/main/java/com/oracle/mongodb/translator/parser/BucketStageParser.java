/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.stage.BucketStage;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;

/**
 * Parser for the MongoDB $bucket stage.
 *
 * <p>Expected format:
 *
 * <pre>
 * {
 *   "groupBy": "$field" or expression,
 *   "boundaries": [value1, value2, ...],
 *   "default": "Other",  // optional
 *   "output": {          // optional
 *     "field": { "$sum": 1 }
 *   }
 * }
 * </pre>
 */
public final class BucketStageParser {

  private final ExpressionParser expressionParser;

  public BucketStageParser() {
    this.expressionParser = new ExpressionParser();
  }

  public BucketStageParser(ExpressionParser expressionParser) {
    this.expressionParser = expressionParser;
  }

  /**
   * Parses a $bucket stage document.
   *
   * @param doc the BSON document containing bucket parameters
   * @return the parsed BucketStage
   * @throws IllegalArgumentException if required fields are missing or invalid
   */
  public BucketStage parse(Document doc) {
    // Parse required groupBy field
    Object groupByValue = doc.get("groupBy");
    if (groupByValue == null) {
      throw new IllegalArgumentException("$bucket requires 'groupBy' field");
    }
    final Expression groupBy = expressionParser.parseValue(groupByValue);

    // Parse required boundaries field
    Object boundariesValue = doc.get("boundaries");
    if (boundariesValue == null) {
      throw new IllegalArgumentException("$bucket requires 'boundaries' field");
    }
    if (!(boundariesValue instanceof List)) {
      throw new IllegalArgumentException(
          "$bucket 'boundaries' must be an array, got: "
              + boundariesValue.getClass().getSimpleName());
    }
    @SuppressWarnings("unchecked")
    List<Object> boundaries = new ArrayList<>((List<Object>) boundariesValue);
    if (boundaries.size() < 2) {
      throw new IllegalArgumentException("$bucket 'boundaries' must have at least 2 values");
    }

    // Parse optional default field
    Object defaultBucket = doc.get("default");

    // Parse optional output field
    Map<String, AccumulatorExpression> output = new LinkedHashMap<>();
    Object outputValue = doc.get("output");
    if (outputValue != null) {
      if (!(outputValue instanceof Document)) {
        throw new IllegalArgumentException(
            "$bucket 'output' must be a document, got: " + outputValue.getClass().getSimpleName());
      }
      output = parseOutput((Document) outputValue);
    }

    return new BucketStage(groupBy, boundaries, defaultBucket, output);
  }

  private Map<String, AccumulatorExpression> parseOutput(Document outputDoc) {
    return AccumulatorParserUtil.parseOutput(outputDoc, expressionParser, "$bucket");
  }
}
