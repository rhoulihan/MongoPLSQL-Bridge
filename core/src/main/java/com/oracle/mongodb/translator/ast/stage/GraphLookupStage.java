/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import org.bson.Document;
import java.util.Map;
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
    private final Document restrictSearchWithMatch;

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
        this(from, startWith, connectFromField, connectToField, as, null, null, null);
    }

    /**
     * Creates a graph lookup stage with maxDepth and depthField options.
     */
    public GraphLookupStage(String from, String startWith, String connectFromField,
                            String connectToField, String as, Integer maxDepth, String depthField) {
        this(from, startWith, connectFromField, connectToField, as, maxDepth, depthField, null);
    }

    /**
     * Creates a graph lookup stage with all options including restrictSearchWithMatch.
     *
     * @param from                    the collection to search
     * @param startWith               the starting field/expression
     * @param connectFromField        the field in the recursive documents to match
     * @param connectToField          the field in the from collection to match against
     * @param as                      the output array field name
     * @param maxDepth                maximum recursion depth (optional)
     * @param depthField              field name to store depth (optional)
     * @param restrictSearchWithMatch document to filter recursive searches (optional)
     */
    public GraphLookupStage(String from, String startWith, String connectFromField,
                            String connectToField, String as, Integer maxDepth, String depthField,
                            Document restrictSearchWithMatch) {
        this.from = Objects.requireNonNull(from, "from must not be null");
        this.startWith = Objects.requireNonNull(startWith, "startWith must not be null");
        this.connectFromField = Objects.requireNonNull(connectFromField, "connectFromField must not be null");
        this.connectToField = Objects.requireNonNull(connectToField, "connectToField must not be null");
        this.as = Objects.requireNonNull(as, "as must not be null");
        this.maxDepth = maxDepth;
        this.depthField = depthField;
        this.restrictSearchWithMatch = restrictSearchWithMatch;
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

    /**
     * Returns the restrictSearchWithMatch document, or null if not specified.
     */
    public Document getRestrictSearchWithMatch() {
        return restrictSearchWithMatch;
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

        // Add restrictSearchWithMatch filter to base case
        if (restrictSearchWithMatch != null) {
            renderRestrictSearchWithMatchConditions(ctx, "", "");
        }

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

        // Add depth limit and restrictSearchWithMatch filter if specified
        boolean hasWhere = false;
        if (maxDepth != null) {
            ctx.sql(" WHERE g.graph_depth < ");
            ctx.sql(String.valueOf(maxDepth));
            hasWhere = true;
        }

        if (restrictSearchWithMatch != null) {
            renderRestrictSearchWithMatchConditions(ctx, hasWhere ? "" : " WHERE 1=1", "c");
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
     * Renders the restrictSearchWithMatch conditions as AND clauses.
     *
     * @param ctx         the SQL generation context
     * @param prefix      prefix to add before conditions (e.g., " WHERE 1=1")
     * @param tableAlias  the table alias to use (e.g., "c" for recursive case, empty for base case)
     */
    private void renderRestrictSearchWithMatchConditions(SqlGenerationContext ctx, String prefix, String tableAlias) {
        if (restrictSearchWithMatch == null || restrictSearchWithMatch.isEmpty()) {
            return;
        }

        ctx.sql(prefix);
        String dataRef = tableAlias.isEmpty() ? "data" : tableAlias + ".data";

        for (Map.Entry<String, Object> entry : restrictSearchWithMatch.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();

            ctx.sql(" AND JSON_VALUE(");
            ctx.sql(dataRef);
            ctx.sql(", '$.");
            ctx.sql(field);
            ctx.sql("')");

            if (value instanceof Document doc) {
                // Handle operators like $in, $gt, etc.
                renderOperatorCondition(ctx, doc);
            } else {
                // Simple equality
                ctx.sql(" = ");
                renderLiteralValue(ctx, value);
            }
        }
    }

    private void renderOperatorCondition(SqlGenerationContext ctx, Document operatorDoc) {
        // For simplicity, handle common operators
        for (Map.Entry<String, Object> entry : operatorDoc.entrySet()) {
            String op = entry.getKey();
            Object operand = entry.getValue();

            switch (op) {
                case "$in" -> {
                    ctx.sql(" IN (");
                    if (operand instanceof java.util.List<?> list) {
                        boolean first = true;
                        for (Object item : list) {
                            if (!first) ctx.sql(", ");
                            renderLiteralValue(ctx, item);
                            first = false;
                        }
                    }
                    ctx.sql(")");
                }
                case "$eq" -> {
                    ctx.sql(" = ");
                    renderLiteralValue(ctx, operand);
                }
                case "$ne" -> {
                    ctx.sql(" != ");
                    renderLiteralValue(ctx, operand);
                }
                case "$gt" -> {
                    ctx.sql(" > ");
                    renderLiteralValue(ctx, operand);
                }
                case "$gte" -> {
                    ctx.sql(" >= ");
                    renderLiteralValue(ctx, operand);
                }
                case "$lt" -> {
                    ctx.sql(" < ");
                    renderLiteralValue(ctx, operand);
                }
                case "$lte" -> {
                    ctx.sql(" <= ");
                    renderLiteralValue(ctx, operand);
                }
                default -> {
                    // Unsupported operator - fall back to equality for now
                    ctx.sql(" = ");
                    renderLiteralValue(ctx, operand);
                }
            }
        }
    }

    private void renderLiteralValue(SqlGenerationContext ctx, Object value) {
        if (value == null) {
            ctx.sql("NULL");
        } else if (value instanceof String str) {
            ctx.sql("'");
            ctx.sql(str.replace("'", "''"));
            ctx.sql("'");
        } else if (value instanceof Boolean bool) {
            ctx.sql(bool ? "'true'" : "'false'");
        } else {
            ctx.sql(String.valueOf(value));
        }
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

        // Add restrictSearchWithMatch filter to base case
        if (restrictSearchWithMatch != null) {
            renderRestrictSearchWithMatchConditions(ctx, "", "");
        }

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

        // Add depth limit and restrictSearchWithMatch filter if specified
        boolean hasWhere = false;
        if (maxDepth != null) {
            ctx.sql(" WHERE g.graph_depth < ");
            ctx.sql(String.valueOf(maxDepth));
            hasWhere = true;
        }

        if (restrictSearchWithMatch != null) {
            renderRestrictSearchWithMatchConditions(ctx, hasWhere ? "" : " WHERE 1=1", "c");
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
        if (restrictSearchWithMatch != null) {
            sb.append(", restrictSearchWithMatch=").append(restrictSearchWithMatch.toJson());
        }
        sb.append(")");
        return sb.toString();
    }
}
