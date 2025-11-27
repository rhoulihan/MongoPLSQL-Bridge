/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a compound _id expression for $group stage.
 *
 * <p>MongoDB syntax:
 * <pre>
 * { _id: { category: "$category", brand: "$brand" } }
 * </pre>
 *
 * <p>Translates to Oracle SQL with multiple GROUP BY columns:
 * <pre>
 * SELECT JSON_VALUE(data, '$.category') AS category,
 *        JSON_VALUE(data, '$.brand') AS brand, ...
 * GROUP BY JSON_VALUE(data, '$.category'), JSON_VALUE(data, '$.brand')
 * </pre>
 */
public final class CompoundIdExpression implements Expression {

    private final Map<String, Expression> fields;

    /**
     * Creates a compound _id expression.
     *
     * @param fields map of field names to expressions
     */
    public CompoundIdExpression(Map<String, Expression> fields) {
        Objects.requireNonNull(fields, "fields must not be null");
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("Compound _id must have at least one field");
        }
        this.fields = new LinkedHashMap<>(fields);
    }

    /**
     * Returns the fields as an unmodifiable map.
     */
    public Map<String, Expression> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Render as comma-separated expressions
        // The GroupStage will use this for both SELECT and GROUP BY
        boolean first = true;
        for (Map.Entry<String, Expression> entry : fields.entrySet()) {
            if (!first) {
                ctx.sql(", ");
            }
            ctx.visit(entry.getValue());
            first = false;
        }
    }

    /**
     * Renders the expression with aliases for the SELECT clause.
     */
    public void renderWithAliases(SqlGenerationContext ctx) {
        boolean first = true;
        for (Map.Entry<String, Expression> entry : fields.entrySet()) {
            if (!first) {
                ctx.sql(", ");
            }
            ctx.visit(entry.getValue());
            ctx.sql(" AS ");
            ctx.identifier(entry.getKey());
            first = false;
        }
    }

    @Override
    public String toString() {
        return "CompoundId(" + fields.keySet() + ")";
    }
}
