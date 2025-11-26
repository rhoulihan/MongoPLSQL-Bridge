/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * Date operators for expressions.
 * Maps MongoDB date operators to their Oracle SQL equivalents.
 *
 * <p>MongoDB stores dates as ISODate which is parsed from JSON as a string.
 * Oracle needs TO_TIMESTAMP to convert, then EXTRACT or TO_CHAR for components.
 */
public enum DateOp {

    YEAR("EXTRACT(YEAR FROM %s)", "$year", true),
    MONTH("EXTRACT(MONTH FROM %s)", "$month", true),
    DAY_OF_MONTH("EXTRACT(DAY FROM %s)", "$dayOfMonth", true),
    HOUR("EXTRACT(HOUR FROM %s)", "$hour", true),
    MINUTE("EXTRACT(MINUTE FROM %s)", "$minute", true),
    SECOND("EXTRACT(SECOND FROM %s)", "$second", true),
    DAY_OF_WEEK("TO_CHAR(%s, 'D')", "$dayOfWeek", false),
    DAY_OF_YEAR("TO_CHAR(%s, 'DDD')", "$dayOfYear", false);

    private static final Map<String, DateOp> MONGO_LOOKUP = Map.of(
        "$year", YEAR,
        "$month", MONTH,
        "$dayOfMonth", DAY_OF_MONTH,
        "$hour", HOUR,
        "$minute", MINUTE,
        "$second", SECOND,
        "$dayOfWeek", DAY_OF_WEEK,
        "$dayOfYear", DAY_OF_YEAR
    );

    private final String sqlTemplate;
    private final String mongoOperator;
    private final boolean extractBased;

    DateOp(String sqlTemplate, String mongoOperator, boolean extractBased) {
        this.sqlTemplate = sqlTemplate;
        this.mongoOperator = mongoOperator;
        this.extractBased = extractBased;
    }

    /**
     * Returns the Oracle SQL template with %s placeholder for the date expression.
     */
    public String getSqlTemplate() {
        return sqlTemplate;
    }

    /**
     * Returns the MongoDB operator name.
     */
    public String getMongoOperator() {
        return mongoOperator;
    }

    /**
     * Returns true if this operator uses EXTRACT (returns a number directly).
     * False means it uses TO_CHAR and needs TO_NUMBER wrapping.
     */
    public boolean isExtractBased() {
        return extractBased;
    }

    /**
     * Returns the DateOp for the given MongoDB operator.
     *
     * @param mongoOp MongoDB operator (e.g., "$year")
     * @return corresponding DateOp
     * @throws IllegalArgumentException if operator is not recognized
     */
    public static DateOp fromMongo(String mongoOp) {
        DateOp op = MONGO_LOOKUP.get(mongoOp);
        if (op == null) {
            throw new IllegalArgumentException("Unknown date operator: " + mongoOp);
        }
        return op;
    }

    /**
     * Returns true if the given operator is a known date operator.
     */
    public static boolean isDateOp(String mongoOp) {
        return MONGO_LOOKUP.containsKey(mongoOp);
    }
}
