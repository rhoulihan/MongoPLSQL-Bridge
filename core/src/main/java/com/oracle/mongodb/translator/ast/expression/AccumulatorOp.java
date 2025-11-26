/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * Accumulator operators for $group stage aggregations.
 * Maps MongoDB accumulator operators to their Oracle SQL equivalents.
 */
public enum AccumulatorOp {

    SUM("SUM", "$sum"),
    AVG("AVG", "$avg"),
    COUNT("COUNT", "$count"),
    MIN("MIN", "$min"),
    MAX("MAX", "$max"),
    FIRST("FIRST_VALUE", "$first"),
    LAST("LAST_VALUE", "$last");

    private static final Map<String, AccumulatorOp> MONGO_LOOKUP;

    static {
        MONGO_LOOKUP = Map.of(
            "$sum", SUM,
            "$avg", AVG,
            "$count", COUNT,
            "$min", MIN,
            "$max", MAX,
            "$first", FIRST,
            "$last", LAST
        );
    }

    private final String sqlFunction;
    private final String mongoOperator;

    AccumulatorOp(String sqlFunction, String mongoOperator) {
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
     * Returns the AccumulatorOp for the given MongoDB operator.
     *
     * @param mongoOp MongoDB operator (e.g., "$sum")
     * @return corresponding AccumulatorOp
     * @throws IllegalArgumentException if operator is not recognized
     */
    public static AccumulatorOp fromMongo(String mongoOp) {
        AccumulatorOp op = MONGO_LOOKUP.get(mongoOp);
        if (op == null) {
            throw new IllegalArgumentException("Unknown accumulator operator: " + mongoOp);
        }
        return op;
    }

    /**
     * Returns true if the given operator is a known accumulator operator.
     */
    public static boolean isAccumulator(String mongoOp) {
        return MONGO_LOOKUP.containsKey(mongoOp);
    }
}
