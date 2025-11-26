/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $lookup stage that performs a left outer join to another collection.
 * Translates to Oracle's LEFT OUTER JOIN.
 *
 * <p>MongoDB syntax:
 * <pre>
 * {
 *   $lookup: {
 *     from: "inventory",
 *     localField: "item",
 *     foreignField: "sku",
 *     as: "inventory_docs"
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation:
 * <pre>
 * LEFT OUTER JOIN inventory lookup_1 ON
 *   JSON_VALUE(base.data, '$.item') = JSON_VALUE(lookup_1.data, '$.sku')
 * </pre>
 */
public final class LookupStage implements Stage {

    private final String from;
    private final String localField;
    private final String foreignField;
    private final String as;

    /**
     * Creates a lookup stage.
     *
     * @param from         the foreign collection to join
     * @param localField   the field from the input documents
     * @param foreignField the field from the foreign collection documents
     * @param as           the name of the new array field to add to the input documents
     */
    public LookupStage(String from, String localField, String foreignField, String as) {
        this.from = Objects.requireNonNull(from, "from must not be null");
        this.localField = Objects.requireNonNull(localField, "localField must not be null");
        this.foreignField = Objects.requireNonNull(foreignField, "foreignField must not be null");
        this.as = Objects.requireNonNull(as, "as must not be null");
    }

    /**
     * Returns the foreign collection name.
     */
    public String getFrom() {
        return from;
    }

    /**
     * Returns the local field path.
     */
    public String getLocalField() {
        return localField;
    }

    /**
     * Returns the foreign field path.
     */
    public String getForeignField() {
        return foreignField;
    }

    /**
     * Returns the output array field name.
     */
    public String getAs() {
        return as;
    }

    @Override
    public String getOperatorName() {
        return "$lookup";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Generate a unique alias for the joined table
        String alias = ctx.generateTableAlias(from);

        ctx.sql("LEFT OUTER JOIN ");
        ctx.sql(from);
        ctx.sql(" ");
        ctx.sql(alias);
        ctx.sql(" ON JSON_VALUE(");
        ctx.sql(ctx.getBaseTableAlias());
        ctx.sql(".data, '$.");
        ctx.sql(localField);
        ctx.sql("') = JSON_VALUE(");
        ctx.sql(alias);
        ctx.sql(".data, '$.");
        ctx.sql(foreignField);
        ctx.sql("')");
    }

    @Override
    public String toString() {
        return "LookupStage(from=" + from
            + ", localField=" + localField
            + ", foreignField=" + foreignField
            + ", as=" + as + ")";
    }
}
