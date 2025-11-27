/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Represents a $sample stage that randomly selects documents.
 *
 * <p>The $sample stage takes a document with a "size" field specifying the
 * number of random documents to return. In Oracle SQL, this is translated
 * to ORDER BY DBMS_RANDOM.VALUE with FETCH FIRST n ROWS ONLY.
 *
 * <p>Example:
 * <pre>
 * {$sample: {size: 10}}
 * </pre>
 * translates to:
 * <pre>
 * ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 10 ROWS ONLY
 * </pre>
 */
public final class SampleStage implements Stage {

    private final int size;

    /**
     * Creates a sample stage.
     *
     * @param size the number of random documents to return (must be positive)
     */
    public SampleStage(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Sample size must be positive, got: " + size);
        }
        this.size = size;
    }

    /**
     * Returns the sample size.
     */
    public int getSize() {
        return size;
    }

    @Override
    public String getOperatorName() {
        return "$sample";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("ORDER BY DBMS_RANDOM.VALUE FETCH FIRST ");
        ctx.sql(String.valueOf(size));
        ctx.sql(" ROWS ONLY");
    }

    @Override
    public String toString() {
        return "SampleStage(" + size + ")";
    }
}
