/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

/**
 * Enumeration of MongoDB type conversion operators.
 */
public enum TypeConversionOp {

    /**
     * $type - Returns the BSON type of the field.
     * Oracle: Uses JSON_VALUE with returning clause checks
     */
    TYPE("$type"),

    /**
     * $toInt - Converts a value to an integer.
     * Oracle: TO_NUMBER or CAST AS NUMBER
     */
    TO_INT("$toInt"),

    /**
     * $toLong - Converts a value to a long.
     * Oracle: TO_NUMBER or CAST AS NUMBER
     */
    TO_LONG("$toLong"),

    /**
     * $toDouble - Converts a value to a double.
     * Oracle: TO_NUMBER or CAST AS BINARY_DOUBLE
     */
    TO_DOUBLE("$toDouble"),

    /**
     * $toDecimal - Converts a value to a decimal.
     * Oracle: TO_NUMBER
     */
    TO_DECIMAL("$toDecimal"),

    /**
     * $toString - Converts a value to a string.
     * Oracle: TO_CHAR
     */
    TO_STRING("$toString"),

    /**
     * $toBool - Converts a value to a boolean.
     * Oracle: CASE expression (0/null = false, else true)
     */
    TO_BOOL("$toBool"),

    /**
     * $toDate - Converts a value to a date.
     * Oracle: TO_TIMESTAMP_TZ
     */
    TO_DATE("$toDate"),

    /**
     * $toObjectId - Converts a value to ObjectId.
     * Oracle: Returns value as-is (ObjectId is stored as string in Oracle JSON)
     */
    TO_OBJECT_ID("$toObjectId"),

    /**
     * $convert - General type conversion with onError/onNull handling.
     * Oracle: CASE with NVL and exception handling
     */
    CONVERT("$convert"),

    /**
     * $isNumber - Checks if expression is a number.
     * Oracle: JSON_VALUE with NUMBER return type check
     */
    IS_NUMBER("$isNumber");

    private final String mongoOperator;

    TypeConversionOp(String mongoOperator) {
        this.mongoOperator = mongoOperator;
    }

    /**
     * Returns the MongoDB operator name (e.g., "$toInt").
     */
    public String getMongoOperator() {
        return mongoOperator;
    }

    /**
     * Finds the TypeConversionOp for a given MongoDB operator string.
     *
     * @param operator the MongoDB operator (e.g., "$toInt")
     * @return the corresponding TypeConversionOp
     * @throws IllegalArgumentException if the operator is not recognized
     */
    public static TypeConversionOp fromMongoOperator(String operator) {
        for (TypeConversionOp op : values()) {
            if (op.mongoOperator.equals(operator)) {
                return op;
            }
        }
        throw new IllegalArgumentException("Unknown type conversion operator: " + operator);
    }
}
