/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Represents a $limit stage that restricts the number of documents.
 * Translates to Oracle's FETCH FIRST n ROWS ONLY clause.
 */
public final class LimitStage implements Stage {

    private final int limit;

    public LimitStage(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    @Override
    public String getOperatorName() {
        return "$limit";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("FETCH FIRST ");
        ctx.sql(String.valueOf(limit));
        ctx.sql(" ROWS ONLY");
    }

    @Override
    public String toString() {
        return "LimitStage(" + limit + ")";
    }
}
