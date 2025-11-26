/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.LookupStage;
import org.bson.Document;

/**
 * Parser for the MongoDB $lookup stage.
 *
 * <p>Expected format:
 * <pre>
 * {
 *   "from": "foreignCollection",
 *   "localField": "fieldInInput",
 *   "foreignField": "fieldInForeign",
 *   "as": "outputArrayField"
 * }
 * </pre>
 */
public final class LookupStageParser {

    /**
     * Parses a $lookup stage document.
     *
     * @param doc the BSON document containing lookup parameters
     * @return the parsed LookupStage
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    public LookupStage parse(Document doc) {
        String from = getRequiredString(doc, "from");
        String localField = getRequiredString(doc, "localField");
        String foreignField = getRequiredString(doc, "foreignField");
        String as = getRequiredString(doc, "as");

        return new LookupStage(from, localField, foreignField, as);
    }

    private String getRequiredString(Document doc, String fieldName) {
        Object value = doc.get(fieldName);
        if (value == null) {
            throw new IllegalArgumentException(
                "$lookup requires '" + fieldName + "' field");
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(
                "$lookup '" + fieldName + "' must be a string, got: " + value.getClass().getSimpleName());
        }
        return (String) value;
    }
}
