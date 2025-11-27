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
import com.oracle.mongodb.translator.ast.stage.BucketAutoStage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parser for the MongoDB $bucketAuto stage.
 *
 * <p>Expected format:
 * <pre>
 * {
 *   "groupBy": "$field" or expression,
 *   "buckets": 5,
 *   "output": {          // optional
 *     "field": { "$sum": 1 }
 *   },
 *   "granularity": "R5"  // optional
 * }
 * </pre>
 */
public final class BucketAutoStageParser {

    private final ExpressionParser expressionParser;

    public BucketAutoStageParser() {
        this.expressionParser = new ExpressionParser();
    }

    public BucketAutoStageParser(ExpressionParser expressionParser) {
        this.expressionParser = expressionParser;
    }

    /**
     * Parses a $bucketAuto stage document.
     *
     * @param doc the BSON document containing bucketAuto parameters
     * @return the parsed BucketAutoStage
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    public BucketAutoStage parse(Document doc) {
        // Parse required groupBy field
        Object groupByValue = doc.get("groupBy");
        if (groupByValue == null) {
            throw new IllegalArgumentException("$bucketAuto requires 'groupBy' field");
        }
        Expression groupBy = expressionParser.parseValue(groupByValue);

        // Parse required buckets field
        Object bucketsValue = doc.get("buckets");
        if (bucketsValue == null) {
            throw new IllegalArgumentException("$bucketAuto requires 'buckets' field");
        }
        int buckets;
        if (bucketsValue instanceof Number num) {
            buckets = num.intValue();
        } else {
            throw new IllegalArgumentException(
                "$bucketAuto 'buckets' must be a number, got: " + bucketsValue.getClass().getSimpleName());
        }
        if (buckets <= 0) {
            throw new IllegalArgumentException("$bucketAuto 'buckets' must be positive, got: " + buckets);
        }

        // Parse optional output field
        Map<String, AccumulatorExpression> output = new LinkedHashMap<>();
        Object outputValue = doc.get("output");
        if (outputValue != null) {
            if (!(outputValue instanceof Document)) {
                throw new IllegalArgumentException(
                    "$bucketAuto 'output' must be a document, got: " + outputValue.getClass().getSimpleName());
            }
            output = parseOutput((Document) outputValue);
        }

        // Parse optional granularity field
        String granularity = null;
        Object granularityValue = doc.get("granularity");
        if (granularityValue != null) {
            if (!(granularityValue instanceof String)) {
                throw new IllegalArgumentException(
                    "$bucketAuto 'granularity' must be a string, got: " + granularityValue.getClass().getSimpleName());
            }
            granularity = (String) granularityValue;
        }

        return new BucketAutoStage(groupBy, buckets, output, granularity);
    }

    private Map<String, AccumulatorExpression> parseOutput(Document outputDoc) {
        Map<String, AccumulatorExpression> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : outputDoc.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            if (!(fieldValue instanceof Document)) {
                throw new IllegalArgumentException(
                    "$bucketAuto output field '" + fieldName + "' must be an accumulator document");
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

        throw new IllegalArgumentException("Unsupported accumulator argument: " + argument);
    }
}
