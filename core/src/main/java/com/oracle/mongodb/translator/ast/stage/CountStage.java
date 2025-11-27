/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Represents a $count stage that counts all input documents.
 *
 * <p>The $count stage takes a string specifying the name of the output field
 * that will contain the count. It returns a single document with the count.
 *
 * <p>Example:
 * <pre>
 * {$count: "totalDocuments"}
 * </pre>
 * translates to:
 * <pre>
 * SELECT JSON_OBJECT('totalDocuments' VALUE COUNT(*)) AS data FROM ...
 * </pre>
 */
public final class CountStage implements Stage {

    private final String fieldName;

    /**
     * Creates a count stage.
     *
     * @param fieldName the name of the field that will contain the count (must not be null, empty, or start with $)
     */
    public CountStage(String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            throw new IllegalArgumentException("$count field name must not be null or empty");
        }
        if (fieldName.startsWith("$")) {
            throw new IllegalArgumentException("$count field name must not start with $, got: " + fieldName);
        }
        this.fieldName = fieldName;
    }

    /**
     * Returns the output field name.
     */
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public String getOperatorName() {
        return "$count";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // $count replaces the entire input with a single document containing the count
        // This renders as SELECT JSON_OBJECT('fieldName' VALUE COUNT(*))
        ctx.sql("SELECT JSON_OBJECT('");
        ctx.sql(fieldName);
        ctx.sql("' VALUE COUNT(*)) AS data FROM");
    }

    @Override
    public String toString() {
        return "CountStage(" + fieldName + ")";
    }
}
