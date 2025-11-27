/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a $facet stage that processes multiple aggregation pipelines
 * within a single stage on the same set of input documents.
 *
 * <p>MongoDB syntax:
 * <pre>
 * {
 *   $facet: {
 *     &lt;outputField1&gt;: [ &lt;stage1&gt;, &lt;stage2&gt;, ... ],
 *     &lt;outputField2&gt;: [ &lt;stage1&gt;, &lt;stage2&gt;, ... ],
 *     ...
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation uses multiple subqueries combined with JSON_OBJECT:
 * <pre>
 * SELECT JSON_OBJECT(
 *   'outputField1' VALUE (SELECT JSON_ARRAYAGG(...) FROM (subquery1)),
 *   'outputField2' VALUE (SELECT JSON_ARRAYAGG(...) FROM (subquery2))
 * ) AS data
 * FROM DUAL
 * </pre>
 *
 * <p>Note: Full $facet support requires complex subquery generation.
 * This implementation provides the AST structure; PipelineRenderer handles the actual SQL.
 */
public final class FacetStage implements Stage {

    private final Map<String, List<Stage>> facets;

    /**
     * Creates a facet stage with the given facet pipelines.
     *
     * @param facets map of facet names to their pipeline stages
     */
    public FacetStage(Map<String, List<Stage>> facets) {
        Objects.requireNonNull(facets, "facets must not be null");
        if (facets.isEmpty()) {
            throw new IllegalArgumentException("$facet must have at least one facet");
        }
        this.facets = new LinkedHashMap<>();
        for (Map.Entry<String, List<Stage>> entry : facets.entrySet()) {
            Objects.requireNonNull(entry.getKey(), "facet name must not be null");
            Objects.requireNonNull(entry.getValue(), "facet pipeline must not be null");
            this.facets.put(entry.getKey(), List.copyOf(entry.getValue()));
        }
    }

    /**
     * Returns the facet pipelines.
     */
    public Map<String, List<Stage>> getFacets() {
        return facets;
    }

    /**
     * Returns the facet names.
     */
    public java.util.Set<String> getFacetNames() {
        return facets.keySet();
    }

    /**
     * Returns the pipeline for a specific facet.
     */
    public List<Stage> getFacetPipeline(String facetName) {
        return facets.get(facetName);
    }

    @Override
    public String getOperatorName() {
        return "$facet";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // $facet requires special handling in PipelineRenderer
        // because it needs to create multiple subqueries.
        // This render method provides basic output for debugging.
        ctx.sql("JSON_OBJECT(");
        boolean first = true;
        for (Map.Entry<String, List<Stage>> entry : facets.entrySet()) {
            if (!first) {
                ctx.sql(", ");
            }
            ctx.sql("'");
            ctx.sql(entry.getKey());
            ctx.sql("' VALUE (/* ");
            ctx.sql(String.valueOf(entry.getValue().size()));
            ctx.sql(" stages */)");
            first = false;
        }
        ctx.sql(")");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("FacetStage(");
        boolean first = true;
        for (Map.Entry<String, List<Stage>> entry : facets.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append("=[").append(entry.getValue().size()).append(" stages]");
            first = false;
        }
        sb.append(")");
        return sb.toString();
    }
}
