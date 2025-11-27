/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.FacetStage;
import com.oracle.mongodb.translator.ast.stage.Stage;
import org.bson.Document;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Parser for the MongoDB $facet stage.
 *
 * <p>Expected format:
 * <pre>
 * {
 *   "facetName1": [
 *     { "$match": { "status": "A" } },
 *     { "$group": { "_id": "$cust_id", "total": { "$sum": "$amount" } } }
 *   ],
 *   "facetName2": [
 *     { "$group": { "_id": "$item", "count": { "$sum": 1 } } }
 *   ]
 * }
 * </pre>
 */
public final class FacetStageParser {

    private final PipelineParser pipelineParser;

    public FacetStageParser(PipelineParser pipelineParser) {
        this.pipelineParser = pipelineParser;
    }

    /**
     * Parses a $facet stage document.
     *
     * @param doc the BSON document containing facet definitions
     * @return the parsed FacetStage
     * @throws IllegalArgumentException if the document is invalid
     */
    public FacetStage parse(Document doc) {
        if (doc.isEmpty()) {
            throw new IllegalArgumentException("$facet must have at least one facet");
        }

        Map<String, List<Stage>> facets = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String facetName = entry.getKey();
            Object facetValue = entry.getValue();

            if (!(facetValue instanceof List)) {
                throw new IllegalArgumentException(
                    "$facet '" + facetName + "' must be an array, got: "
                        + (facetValue == null ? "null" : facetValue.getClass().getSimpleName()));
            }

            @SuppressWarnings("unchecked")
            List<Document> pipelineDocList = (List<Document>) facetValue;

            // Parse the facet pipeline
            var parsedPipeline = pipelineParser.parse(facetName, pipelineDocList);
            facets.put(facetName, parsedPipeline.getStages());
        }

        return new FacetStage(facets);
    }
}
