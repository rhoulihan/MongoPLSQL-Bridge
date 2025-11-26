/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $match stage that filters documents.
 * Translates to Oracle's WHERE clause.
 */
public final class MatchStage implements Stage {

    private final Expression filter;

    public MatchStage(Expression filter) {
        this.filter = Objects.requireNonNull(filter, "filter must not be null");
    }

    /**
     * Returns the filter expression.
     */
    public Expression getFilter() {
        return filter;
    }

    @Override
    public String getOperatorName() {
        return "$match";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("WHERE ");
        ctx.visit(filter);
    }

    @Override
    public String toString() {
        return "MatchStage(" + filter + ")";
    }
}
