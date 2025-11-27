/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.List;
import java.util.Objects;

/**
 * Represents a $unionWith stage that combines documents from another collection.
 * Translates to Oracle's UNION ALL.
 *
 * <p>MongoDB syntax (simple form):
 * <pre>
 * { $unionWith: "otherCollection" }
 * </pre>
 *
 * <p>MongoDB syntax (with pipeline):
 * <pre>
 * {
 *   $unionWith: {
 *     coll: "otherCollection",
 *     pipeline: [ ... ]
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation:
 * <pre>
 * ... UNION ALL SELECT data FROM otherCollection
 * </pre>
 *
 * <p>Note: The pipeline parameter is not yet supported and will be ignored.
 */
public final class UnionWithStage implements Stage {

    private final String collection;
    private final List<Stage> pipeline;

    /**
     * Creates a union with stage for a simple collection union.
     *
     * @param collection the collection to union with
     */
    public UnionWithStage(String collection) {
        this(collection, List.of());
    }

    /**
     * Creates a union with stage with an optional pipeline.
     *
     * @param collection the collection to union with
     * @param pipeline   optional pipeline to apply to the union collection (may be empty)
     */
    public UnionWithStage(String collection, List<Stage> pipeline) {
        this.collection = Objects.requireNonNull(collection, "collection must not be null");
        this.pipeline = pipeline != null ? List.copyOf(pipeline) : List.of();
    }

    /**
     * Returns the collection to union with.
     */
    public String getCollection() {
        return collection;
    }

    /**
     * Returns the optional pipeline to apply to the union collection.
     */
    public List<Stage> getPipeline() {
        return pipeline;
    }

    /**
     * Returns true if this union has a pipeline.
     */
    public boolean hasPipeline() {
        return !pipeline.isEmpty();
    }

    @Override
    public String getOperatorName() {
        return "$unionWith";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // $unionWith requires special handling in PipelineRenderer
        // because it needs to create a UNION ALL with a complete subquery.
        // This render method provides basic output for debugging.
        ctx.sql("UNION ALL SELECT data FROM ");
        ctx.sql(collection);
    }

    @Override
    public String toString() {
        if (pipeline.isEmpty()) {
            return "UnionWithStage(collection=" + collection + ")";
        }
        return "UnionWithStage(collection=" + collection + ", pipeline=" + pipeline + ")";
    }
}
