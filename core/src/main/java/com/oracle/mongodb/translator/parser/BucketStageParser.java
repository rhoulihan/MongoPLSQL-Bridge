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
import com.oracle.mongodb.translator.ast.stage.BucketStage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for the MongoDB $bucket stage.
 *
 * <p>Expected format:
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
        Expression groupBy = expressionParser.parseValue(groupByValue);

        // Parse required boundaries field
        Object boundariesValue = doc.get("boundaries");
        if (boundariesValue == null) {
            throw new IllegalArgumentException("$bucket requires 'boundaries' field");
        }
        if (!(boundariesValue instanceof List)) {
            throw new IllegalArgumentException(
                "$bucket 'boundaries' must be an array, got: " + boundariesValue.getClass().getSimpleName());
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
        Map<String, AccumulatorExpression> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : outputDoc.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            if (!(fieldValue instanceof Document)) {
                throw new IllegalArgumentException(
                    "$bucket output field '" + fieldName + "' must be an accumulator document");
            }
            result.put(fieldName, parseAccumulator((Document) fieldValue));
        }
        return result;
    }

    private AccumulatorExpression parseAccumulator(Document accDoc) {
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

        if (argument instanceof Document doc && doc.isEmpty()) {
            return null;
        }

        if (argument instanceof String strVal) {
            if (strVal.startsWith("$")) {
                return FieldPathExpression.of(strVal.substring(1));
            }
            return LiteralExpression.of(strVal);
        }

        if (argument instanceof Number) {
            return LiteralExpression.of(argument);
        }

        if (argument instanceof Document) {
            // Complex expression (e.g., { $cond: [...] }, { $multiply: [...] })
            return expressionParser.parseValue(argument);
        }

        throw new IllegalArgumentException("Unsupported accumulator argument: " + argument);
    }
}
