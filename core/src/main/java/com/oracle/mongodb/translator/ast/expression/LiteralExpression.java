/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a literal constant value.
 */
public final class LiteralExpression implements Expression {

    private final Object value;
    private final boolean isNull;

    private LiteralExpression(Object value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    /**
     * Creates a literal expression with the given value.
     */
    public static LiteralExpression of(Object value) {
        return new LiteralExpression(value, false);
    }

    /**
     * Creates a NULL literal expression.
     */
    public static LiteralExpression ofNull() {
        return new LiteralExpression(null, true);
    }

    public Object getValue() {
        return value;
    }

    public boolean isNull() {
        return isNull;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        if (isNull) {
            ctx.sql("NULL");
        } else {
            ctx.bind(value);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LiteralExpression that = (LiteralExpression) obj;
        return isNull == that.isNull && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, isNull);
    }

    @Override
    public String toString() {
        return isNull ? "Literal(NULL)" : "Literal(" + value + ")";
    }
}
