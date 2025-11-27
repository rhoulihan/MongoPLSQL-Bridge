/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.Stage;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for the MongoDB $lookup stage.
 *
 * <p>Supports two forms:
 *
 * <p>1. Simple equality match:
 * <pre>
 * {
 *   "from": "foreignCollection",
 *   "localField": "fieldInInput",
 *   "foreignField": "fieldInForeign",
 *   "as": "outputArrayField"
 * }
 * </pre>
 *
 * <p>2. Pipeline with variables (correlated subquery):
 * <pre>
 * {
 *   "from": "foreignCollection",
 *   "let": { "var1": "$localField1", "var2": "$localField2" },
 *   "pipeline": [ ... ],
 *   "as": "outputArrayField"
 * }
 * </pre>
 */
public final class LookupStageParser {

    // Use lazy initialization to avoid circular dependency with PipelineParser
    private PipelineParser pipelineParser;

    public LookupStageParser() {
        // Don't initialize pipelineParser here to avoid circular dependency
    }

    private PipelineParser getPipelineParser() {
        if (pipelineParser == null) {
            pipelineParser = new PipelineParser();
        }
        return pipelineParser;
    }

    /**
     * Parses a $lookup stage document.
     *
     * @param doc the BSON document containing lookup parameters
     * @return the parsed LookupStage
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    public LookupStage parse(Document doc) {
        String from = getRequiredString(doc, "from");
        String as = getRequiredString(doc, "as");

        // Check if this is the pipeline form
        if (doc.containsKey("pipeline") || doc.containsKey("let")) {
            return parsePipelineForm(doc, from, as);
        }

        // Simple equality form
        String localField = getRequiredString(doc, "localField");
        String foreignField = getRequiredString(doc, "foreignField");

        return LookupStage.equality(from, localField, foreignField, as);
    }

    private LookupStage parsePipelineForm(Document doc, String from, String as) {
        // Parse let variables
        Map<String, String> letVariables = new LinkedHashMap<>();
        Object letObj = doc.get("let");
        if (letObj instanceof Document letDoc) {
            for (Map.Entry<String, Object> entry : letDoc.entrySet()) {
                String varName = entry.getKey();
                Object varValue = entry.getValue();
                if (varValue instanceof String strVal && strVal.startsWith("$")) {
                    letVariables.put(varName, strVal.substring(1));
                } else {
                    throw new IllegalArgumentException(
                        "$lookup let variable '" + varName + "' must be a field reference starting with $");
                }
            }
        }

        // Parse pipeline
        List<Stage> stages = new ArrayList<>();
        Object pipelineObj = doc.get("pipeline");
        if (pipelineObj instanceof List<?> pipelineList) {
            List<Document> pipelineDocs = new ArrayList<>();
            for (Object stageObj : pipelineList) {
                if (stageObj instanceof Document stageDoc) {
                    pipelineDocs.add(stageDoc);
                }
            }
            if (!pipelineDocs.isEmpty()) {
                Pipeline parsed = getPipelineParser().parse(from, pipelineDocs);
                stages.addAll(parsed.getStages());
            }
        }

        return LookupStage.withPipeline(from, letVariables, stages, as);
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
