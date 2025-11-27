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
 * <p>Oracle translation uses a recursive CTE (Common Table Expression):
 * <pre>
 * WITH graph_cte (id, data, depth) AS (
 *   -- Base case: initial documents matching startWith
 *   SELECT id, data, 0 AS depth
 *   FROM collection
 *   WHERE JSON_VALUE(data, '$.connectToField') = :startWith
 *   UNION ALL
 *   -- Recursive case: traverse connections
 *   SELECT c.id, c.data, g.depth + 1
 *   FROM collection c
 *   JOIN graph_cte g ON JSON_VALUE(c.data, '$.connectToField') = JSON_VALUE(g.data, '$.connectFromField')
 *   WHERE g.depth &lt; :maxDepth
 * )
 * SELECT ... FROM main_collection m
 * LEFT JOIN (
 *   SELECT JSON_ARRAYAGG(data) AS results FROM graph_cte
 * ) ON 1=1
 * </pre>
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
        // Render as recursive CTE
        String cteName = "graph_" + as;
        String startField = startWith.startsWith("$") ? startWith.substring(1) : startWith;

        // Generate the recursive CTE
        ctx.sql("WITH ");
        ctx.sql(cteName);
        ctx.sql(" (id, data, graph_depth) AS (");

        // Base case: find initial matching documents
        ctx.sql("SELECT id, data, 0 AS graph_depth FROM ");
        ctx.sql(from);
        ctx.sql(" WHERE JSON_VALUE(data, '$.");
        ctx.sql(connectToField);
        ctx.sql("') = JSON_VALUE(");
        ctx.sql(ctx.getBaseTableAlias());
        ctx.sql(".data, '$.");
        ctx.sql(startField);
        ctx.sql("')");

        ctx.sql(" UNION ALL ");

        // Recursive case: traverse the graph
        ctx.sql("SELECT c.id, c.data, g.graph_depth + 1 FROM ");
        ctx.sql(from);
        ctx.sql(" c JOIN ");
        ctx.sql(cteName);
        ctx.sql(" g ON JSON_VALUE(c.data, '$.");
        ctx.sql(connectToField);
        ctx.sql("') = JSON_VALUE(g.data, '$.");
        ctx.sql(connectFromField);
        ctx.sql("')");

        // Add depth limit if specified
        if (maxDepth != null) {
            ctx.sql(" WHERE g.graph_depth < ");
            ctx.sql(String.valueOf(maxDepth));
        }

        ctx.sql(") ");

        // Add the CTE results as a lateral join
        ctx.sql("SELECT ");
        if (depthField != null) {
            ctx.sql("g.graph_depth AS ");
            ctx.sql(depthField);
            ctx.sql(", ");
        }
        ctx.sql("JSON_ARRAYAGG(g.data) AS ");
        ctx.sql(as);
        ctx.sql(" FROM ");
        ctx.sql(cteName);
        ctx.sql(" g");
    }

    /**
     * Renders just the CTE definition without the SELECT, for use in PipelineRenderer.
     */
    public void renderCteDefinition(SqlGenerationContext ctx, String sourceAlias) {
        String cteName = "graph_" + as;
        String startField = startWith.startsWith("$") ? startWith.substring(1) : startWith;

        ctx.sql(cteName);
        ctx.sql(" (id, data, graph_depth) AS (");

        // Base case
        ctx.sql("SELECT id, data, 0 AS graph_depth FROM ");
        ctx.sql(from);
        ctx.sql(" WHERE JSON_VALUE(data, '$.");
        ctx.sql(connectToField);
        ctx.sql("') = JSON_VALUE(");
        ctx.sql(sourceAlias);
        ctx.sql(".data, '$.");
        ctx.sql(startField);
        ctx.sql("')");

        ctx.sql(" UNION ALL ");

        // Recursive case
        ctx.sql("SELECT c.id, c.data, g.graph_depth + 1 FROM ");
        ctx.sql(from);
        ctx.sql(" c JOIN ");
        ctx.sql(cteName);
        ctx.sql(" g ON JSON_VALUE(c.data, '$.");
        ctx.sql(connectToField);
        ctx.sql("') = JSON_VALUE(g.data, '$.");
        ctx.sql(connectFromField);
        ctx.sql("')");

        if (maxDepth != null) {
            ctx.sql(" WHERE g.graph_depth < ");
            ctx.sql(String.valueOf(maxDepth));
        }

        ctx.sql(")");
    }

    /**
     * Gets the CTE name for this graph lookup.
     */
    public String getCteName() {
        return "graph_" + as;
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
