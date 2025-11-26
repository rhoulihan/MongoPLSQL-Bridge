/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Parses MongoDB aggregation pipeline documents into AST.
 */
public class PipelineParser {

    private final StageParserRegistry stageRegistry;

    public PipelineParser() {
        this.stageRegistry = new StageParserRegistry();
    }

    public PipelineParser(StageParserRegistry stageRegistry) {
        this.stageRegistry = Objects.requireNonNull(stageRegistry, "stageRegistry must not be null");
    }

    /**
     * Parses a pipeline of BSON documents into an AST Pipeline.
     *
     * @param collectionName the name of the collection being queried
     * @param pipeline the list of pipeline stage documents
     * @return the parsed Pipeline AST
     */
    public Pipeline parse(String collectionName, List<Document> pipeline) {
        Objects.requireNonNull(collectionName, "collectionName must not be null");
        Objects.requireNonNull(pipeline, "pipeline must not be null");

        List<Stage> stages = new ArrayList<>();
        for (Document stageDoc : pipeline) {
            stages.add(parseStage(stageDoc));
        }

        return Pipeline.of(collectionName, stages.toArray(new Stage[0]));
    }

    private Stage parseStage(Document stageDoc) {
        if (stageDoc.size() != 1) {
            throw new IllegalArgumentException(
                "Stage document must have exactly one key, got: " + stageDoc.keySet());
        }

        String operatorName = stageDoc.keySet().iterator().next();
        Object operatorValue = stageDoc.get(operatorName);

        StageParser<?> parser = stageRegistry.getParser(operatorName);
        if (parser == null) {
            throw new UnsupportedOperatorException(operatorName);
        }

        return parser.parse(operatorValue);
    }

    /**
     * Returns the stage parser registry.
     */
    public StageParserRegistry getStageRegistry() {
        return stageRegistry;
    }
}
