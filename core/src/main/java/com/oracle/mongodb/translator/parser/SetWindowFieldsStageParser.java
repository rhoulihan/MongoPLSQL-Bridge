/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage.WindowField;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage.WindowSpec;
import org.bson.Document;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for the MongoDB $setWindowFields stage.
 *
 * <p>Expected format:
 * <pre>
 * {
 *   "partitionBy": "$state",
 *   "sortBy": { "orderDate": 1 },
 *   "output": {
 *     "cumulativeSum": {
 *       "$sum": "$quantity",
 *       "window": { "documents": ["unbounded", "current"] }
 *     }
 *   }
 * }
 * </pre>
 */
public final class SetWindowFieldsStageParser {

    /**
     * Parses a $setWindowFields stage document.
     *
     * @param doc the BSON document containing window fields parameters
     * @return the parsed SetWindowFieldsStage
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    public SetWindowFieldsStage parse(Document doc) {
        // Parse partitionBy - optional
        String partitionBy = null;
        Object partitionByValue = doc.get("partitionBy");
        if (partitionByValue instanceof String) {
            partitionBy = (String) partitionByValue;
        }

        // Parse sortBy - optional
        Map<String, Integer> sortBy = new LinkedHashMap<>();
        Object sortByValue = doc.get("sortBy");
        if (sortByValue instanceof Document sortDoc) {
            for (Map.Entry<String, Object> entry : sortDoc.entrySet()) {
                if (entry.getValue() instanceof Number) {
                    sortBy.put(entry.getKey(), ((Number) entry.getValue()).intValue());
                }
            }
        }

        // Parse output - required
        Object outputValue = doc.get("output");
        if (outputValue == null) {
            throw new IllegalArgumentException("$setWindowFields requires 'output' field");
        }
        if (!(outputValue instanceof Document)) {
            throw new IllegalArgumentException(
                "$setWindowFields 'output' must be a document, got: " + outputValue.getClass().getSimpleName());
        }
        Map<String, WindowField> output = parseOutput((Document) outputValue);

        return new SetWindowFieldsStage(partitionBy, sortBy, output);
    }

    private Map<String, WindowField> parseOutput(Document outputDoc) {
        Map<String, WindowField> result = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : outputDoc.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();

            if (!(fieldValue instanceof Document)) {
                throw new IllegalArgumentException(
                    "$setWindowFields output field '" + fieldName + "' must be a document");
            }

            result.put(fieldName, parseWindowField((Document) fieldValue));
        }

        return result;
    }

    private WindowField parseWindowField(Document fieldDoc) {
        String operator = null;
        String argument = null;
        WindowSpec window = null;

        for (Map.Entry<String, Object> entry : fieldDoc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.startsWith("$")) {
                // Window function operator
                operator = key;
                if (value instanceof String) {
                    argument = (String) value;
                } else if (value instanceof Document argDoc && argDoc.isEmpty()) {
                    argument = null;
                } else {
                    argument = value != null ? value.toString() : null;
                }
            } else if ("window".equals(key) && value instanceof Document windowDoc) {
                window = parseWindowSpec(windowDoc);
            }
        }

        return new WindowField(operator, argument, window);
    }

    private WindowSpec parseWindowSpec(Document windowDoc) {
        String type = null;
        List<String> bounds = new ArrayList<>();

        if (windowDoc.containsKey("documents")) {
            type = "documents";
            Object docsValue = windowDoc.get("documents");
            if (docsValue instanceof List<?> list) {
                for (Object item : list) {
                    bounds.add(item != null ? item.toString() : "current");
                }
            }
        } else if (windowDoc.containsKey("range")) {
            type = "range";
            Object rangeValue = windowDoc.get("range");
            if (rangeValue instanceof List<?> list) {
                for (Object item : list) {
                    bounds.add(item != null ? item.toString() : "current");
                }
            }
        }

        return new WindowSpec(type, bounds);
    }
}
