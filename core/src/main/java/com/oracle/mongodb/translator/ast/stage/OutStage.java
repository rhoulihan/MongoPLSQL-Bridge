/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $out stage that writes the results of the aggregation pipeline
 * to a specified collection, replacing the collection if it exists.
 *
 * <p>MongoDB syntax:
 * <pre>
 * // Simple form
 * { $out: "outputCollection" }
 *
 * // Document form
 * {
 *   $out: {
 *     db: "database",
 *     coll: "collection"
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation uses INSERT statement:
 * <pre>
 * INSERT INTO outputCollection (data)
 * SELECT data FROM (aggregation query)
 * </pre>
 *
 * <p>Note: This is a stub implementation. Full $out support requires
 * CREATE TABLE AS SELECT or DROP/INSERT pattern.
 */
public final class OutStage implements Stage {

    private final String targetCollection;
    private final String targetDatabase;

    /**
     * Creates an out stage with a target collection.
     *
     * @param targetCollection the target collection name
     */
    public OutStage(String targetCollection) {
        this(targetCollection, null);
    }

    /**
     * Creates an out stage with target database and collection.
     *
     * @param targetCollection the target collection name
     * @param targetDatabase   the target database name (may be null)
     */
    public OutStage(String targetCollection, String targetDatabase) {
        this.targetCollection = Objects.requireNonNull(targetCollection, "targetCollection must not be null");
        this.targetDatabase = targetDatabase;
    }

    public String getTargetCollection() {
        return targetCollection;
    }

    public String getTargetDatabase() {
        return targetDatabase;
    }

    public boolean hasTargetDatabase() {
        return targetDatabase != null && !targetDatabase.isEmpty();
    }

    @Override
    public String getOperatorName() {
        return "$out";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // $out requires special handling - generates INSERT statement
        // This render provides debugging output
        ctx.sql("/* INSERT INTO ");
        if (hasTargetDatabase()) {
            ctx.sql(targetDatabase);
            ctx.sql(".");
        }
        ctx.sql(targetCollection);
        ctx.sql(" */");
    }

    @Override
    public String toString() {
        if (hasTargetDatabase()) {
            return "OutStage(database=" + targetDatabase + ", collection=" + targetCollection + ")";
        }
        return "OutStage(collection=" + targetCollection + ")";
    }
}
