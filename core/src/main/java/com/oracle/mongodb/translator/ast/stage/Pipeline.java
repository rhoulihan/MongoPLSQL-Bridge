/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a MongoDB aggregation pipeline consisting of multiple stages.
 * The pipeline is immutable - adding stages returns a new Pipeline instance.
 */
public final class Pipeline implements AstNode {

    private final String collectionName;
    private final List<Stage> stages;

    private Pipeline(String collectionName, List<Stage> stages) {
        this.collectionName = collectionName;
        this.stages = Collections.unmodifiableList(new ArrayList<>(stages));
    }

    /**
     * Creates an empty pipeline.
     */
    public static Pipeline of() {
        return new Pipeline(null, Collections.emptyList());
    }

    /**
     * Creates a pipeline with the given stages.
     */
    public static Pipeline of(Stage... stages) {
        return new Pipeline(null, Arrays.asList(stages));
    }

    /**
     * Creates an empty pipeline for a specific collection.
     */
    public static Pipeline of(String collectionName) {
        return new Pipeline(collectionName, Collections.emptyList());
    }

    /**
     * Creates a pipeline for a specific collection with the given stages.
     */
    public static Pipeline of(String collectionName, Stage... stages) {
        return new Pipeline(collectionName, Arrays.asList(stages));
    }

    /**
     * Returns a new pipeline with the given stage added.
     */
    public Pipeline addStage(Stage stage) {
        Objects.requireNonNull(stage, "stage must not be null");
        List<Stage> newStages = new ArrayList<>(stages);
        newStages.add(stage);
        return new Pipeline(collectionName, newStages);
    }

    /**
     * Returns the collection name, or null if not set.
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Returns an unmodifiable list of stages.
     */
    public List<Stage> getStages() {
        return stages;
    }

    /**
     * Returns the number of stages in the pipeline.
     */
    public int size() {
        return stages.size();
    }

    /**
     * Returns true if the pipeline has no stages.
     */
    public boolean isEmpty() {
        return stages.isEmpty();
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        boolean first = true;
        for (Stage stage : stages) {
            if (!first) {
                ctx.sql(" ");
            }
            stage.render(ctx);
            first = false;
        }
    }

    @Override
    public String toString() {
        return "Pipeline{collection=" + collectionName + ", stages=" + stages + "}";
    }
}
