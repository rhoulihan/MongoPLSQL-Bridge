/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * Arithmetic operators for computed expressions.
 * Maps MongoDB arithmetic operators to their Oracle SQL equivalents.
 */
public enum ArithmeticOp {

    ADD("+", "$add"),
    SUBTRACT("-", "$subtract"),
    MULTIPLY("*", "$multiply"),
    DIVIDE("/", "$divide"),
    MOD("MOD", "$mod");

    private static final Map<String, ArithmeticOp> MONGO_LOOKUP;

    static {
        MONGO_LOOKUP = Map.of(
            "$add", ADD,
            "$subtract", SUBTRACT,
            "$multiply", MULTIPLY,
            "$divide", DIVIDE,
            "$mod", MOD
        );
    }

    private final String sqlOperator;
    private final String mongoOperator;

    ArithmeticOp(String sqlOperator, String mongoOperator) {
        this.sqlOperator = sqlOperator;
        this.mongoOperator = mongoOperator;
    }

    /**
     * Returns the Oracle SQL operator or function name.
     */
    public String getSqlOperator() {
        return sqlOperator;
    }

    /**
     * Returns the MongoDB operator name.
     */
    public String getMongoOperator() {
        return mongoOperator;
    }

    /**
     * Returns true if this operator requires a function call (like MOD).
     */
    public boolean requiresFunctionCall() {
        return this == MOD;
    }

    /**
     * Returns the ArithmeticOp for the given MongoDB operator.
     *
     * @param mongoOp MongoDB operator (e.g., "$add")
     * @return corresponding ArithmeticOp
     * @throws IllegalArgumentException if operator is not recognized
     */
    public static ArithmeticOp fromMongo(String mongoOp) {
        ArithmeticOp op = MONGO_LOOKUP.get(mongoOp);
        if (op == null) {
            throw new IllegalArgumentException("Unknown arithmetic operator: " + mongoOp);
        }
        return op;
    }

    /**
     * Returns true if the given operator is a known arithmetic operator.
     */
    public static boolean isArithmetic(String mongoOp) {
        return MONGO_LOOKUP.containsKey(mongoOp);
    }
}
