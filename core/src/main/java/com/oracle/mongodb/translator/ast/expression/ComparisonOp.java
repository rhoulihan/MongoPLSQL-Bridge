/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

/**
 * Comparison operators mapping MongoDB to SQL.
 */
public enum ComparisonOp {
    EQ("=", "$eq"),
    NE("<>", "$ne"),
    GT(">", "$gt"),
    GTE(">=", "$gte"),
    LT("<", "$lt"),
    LTE("<=", "$lte"),
    IN("IN", "$in"),
    NIN("NOT IN", "$nin");

    private final String sqlOperator;
    private final String mongoOperator;

    ComparisonOp(String sqlOperator, String mongoOperator) {
        this.sqlOperator = sqlOperator;
        this.mongoOperator = mongoOperator;
    }

    /**
     * Returns the SQL operator string.
     */
    public String getSqlOperator() {
        return sqlOperator;
    }

    /**
     * Returns the MongoDB operator string.
     */
    public String getMongoOperator() {
        return mongoOperator;
    }

    /**
     * Returns the ComparisonOp for a MongoDB operator string.
     */
    public static ComparisonOp fromMongo(String mongoOp) {
        for (ComparisonOp op : values()) {
            if (op.mongoOperator.equals(mongoOp)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown comparison operator: " + mongoOp);
    }
}
