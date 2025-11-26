/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * String operators for expressions.
 * Maps MongoDB string operators to their Oracle SQL equivalents.
 */
public enum StringOp {

    CONCAT("CONCAT", "$concat"),
    TO_LOWER("LOWER", "$toLower"),
    TO_UPPER("UPPER", "$toUpper"),
    SUBSTR("SUBSTR", "$substr"),
    TRIM("TRIM", "$trim"),
    LTRIM("LTRIM", "$ltrim"),
    RTRIM("RTRIM", "$rtrim"),
    STRLEN("LENGTH", "$strLenCP");

    private static final Map<String, StringOp> MONGO_LOOKUP = Map.of(
        "$concat", CONCAT,
        "$toLower", TO_LOWER,
        "$toUpper", TO_UPPER,
        "$substr", SUBSTR,
        "$trim", TRIM,
        "$ltrim", LTRIM,
        "$rtrim", RTRIM,
        "$strLenCP", STRLEN
    );

    private final String sqlFunction;
    private final String mongoOperator;

    StringOp(String sqlFunction, String mongoOperator) {
        this.sqlFunction = sqlFunction;
        this.mongoOperator = mongoOperator;
    }

    /**
     * Returns the Oracle SQL function name.
     */
    public String getSqlFunction() {
        return sqlFunction;
    }

    /**
     * Returns the MongoDB operator name.
     */
    public String getMongoOperator() {
        return mongoOperator;
    }

    /**
     * Returns the StringOp for the given MongoDB operator.
     *
     * @param mongoOp MongoDB operator (e.g., "$concat")
     * @return corresponding StringOp
     * @throws IllegalArgumentException if operator is not recognized
     */
    public static StringOp fromMongo(String mongoOp) {
        StringOp op = MONGO_LOOKUP.get(mongoOp);
        if (op == null) {
            throw new IllegalArgumentException("Unknown string operator: " + mongoOp);
        }
        return op;
    }

    /**
     * Returns true if the given operator is a known string operator.
     */
    public static boolean isStringOp(String mongoOp) {
        return MONGO_LOOKUP.containsKey(mongoOp);
    }
}
