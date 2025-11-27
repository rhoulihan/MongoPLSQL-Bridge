/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $graphLookup stage that performs a recursive search on a collection.
 *
 * <p>MongoDB syntax:
 * <pre>
 * {
 *   $graphLookup: {
 *     from: &lt;collection&gt;,
 *     startWith: &lt;expression&gt;,
 *     connectFromField: &lt;string&gt;,
 *     connectToField: &lt;string&gt;,
 *     as: &lt;string&gt;,
 *     maxDepth: &lt;number&gt;,
 *     depthField: &lt;string&gt;,
 *     restrictSearchWithMatch: &lt;document&gt;
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation would require recursive CTE:
 * <pre>
 * WITH RECURSIVE graph_results AS (
 *   -- Base case: initial documents
 *   SELECT ... FROM collection WHERE startWith condition
 *   UNION ALL
 *   -- Recursive case
 *   SELECT ... FROM collection c
 *   JOIN graph_results g ON c.connectToField = g.connectFromField
 *   WHERE depth &lt; maxDepth
 * )
 * SELECT ... FROM main_collection
 * LEFT JOIN graph_results ...
 * </pre>
 *
 * <p>Note: This is a stub implementation. Full $graphLookup support
 * requires complex recursive CTE generation.
 */
public final class GraphLookupStage implements Stage {

    private final String from;
    private final String startWith;
    private final String connectFromField;
    private final String connectToField;
    private final String as;
    private final Integer maxDepth;
    private final String depthField;

    /**
     * Creates a graph lookup stage.
     *
     * @param from             the collection to search
     * @param startWith        the starting field/expression
     * @param connectFromField the field in the recursive documents to match
     * @param connectToField   the field in the from collection to match against
     * @param as               the output array field name
     */
    public GraphLookupStage(String from, String startWith, String connectFromField,
                            String connectToField, String as) {
        this(from, startWith, connectFromField, connectToField, as, null, null);
    }

    /**
     * Creates a graph lookup stage with all options.
     */
    public GraphLookupStage(String from, String startWith, String connectFromField,
                            String connectToField, String as, Integer maxDepth, String depthField) {
        this.from = Objects.requireNonNull(from, "from must not be null");
        this.startWith = Objects.requireNonNull(startWith, "startWith must not be null");
        this.connectFromField = Objects.requireNonNull(connectFromField, "connectFromField must not be null");
        this.connectToField = Objects.requireNonNull(connectToField, "connectToField must not be null");
        this.as = Objects.requireNonNull(as, "as must not be null");
        this.maxDepth = maxDepth;
        this.depthField = depthField;
    }

    public String getFrom() {
        return from;
    }

    public String getStartWith() {
        return startWith;
    }

    public String getConnectFromField() {
        return connectFromField;
    }

    public String getConnectToField() {
        return connectToField;
    }

    public String getAs() {
        return as;
    }

    public Integer getMaxDepth() {
        return maxDepth;
    }

    public String getDepthField() {
        return depthField;
    }

    @Override
    public String getOperatorName() {
        return "$graphLookup";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // $graphLookup is not fully supported - render as comment
        ctx.sql("/* $graphLookup from ");
        ctx.sql(from);
        ctx.sql(" startWith ");
        ctx.sql(startWith);
        ctx.sql(" connectFromField ");
        ctx.sql(connectFromField);
        ctx.sql(" connectToField ");
        ctx.sql(connectToField);
        ctx.sql(" as ");
        ctx.sql(as);
        if (maxDepth != null) {
            ctx.sql(" maxDepth ");
            ctx.sql(String.valueOf(maxDepth));
        }
        ctx.sql(" - NOT YET IMPLEMENTED */");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("GraphLookupStage(from=");
        sb.append(from);
        sb.append(", startWith=").append(startWith);
        sb.append(", connectFromField=").append(connectFromField);
        sb.append(", connectToField=").append(connectToField);
        sb.append(", as=").append(as);
        if (maxDepth != null) {
            sb.append(", maxDepth=").append(maxDepth);
        }
        if (depthField != null) {
            sb.append(", depthField=").append(depthField);
        }
        sb.append(")");
        return sb.toString();
    }
}
