/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * Array operators for expressions.
 * Maps MongoDB array operators to their Oracle SQL equivalents.
 */
public enum ArrayOp {

    ARRAY_ELEM_AT("$arrayElemAt"),
    SIZE("$size"),
    FIRST("$first"),
    LAST("$last"),
    FILTER("$filter"),
    MAP("$map"),
    REDUCE("$reduce"),
    CONCAT_ARRAYS("$concatArrays"),
    SLICE("$slice");

    private static final Map<String, ArrayOp> MONGO_LOOKUP;

    static {
        MONGO_LOOKUP = Map.of(
            "$arrayElemAt", ARRAY_ELEM_AT,
            "$size", SIZE,
            "$first", FIRST,
            "$last", LAST,
            "$filter", FILTER,
            "$map", MAP,
            "$reduce", REDUCE,
            "$concatArrays", CONCAT_ARRAYS,
            "$slice", SLICE
        );
    }

    private final String mongoOperator;

    ArrayOp(String mongoOperator) {
        this.mongoOperator = mongoOperator;
    }

    /**
     * Returns the MongoDB operator name.
     */
    public String getMongoOperator() {
        return mongoOperator;
    }

    /**
     * Returns the ArrayOp for the given MongoDB operator.
     *
     * @param mongoOp MongoDB operator (e.g., "$arrayElemAt")
     * @return corresponding ArrayOp
     * @throws IllegalArgumentException if operator is not recognized
     */
    public static ArrayOp fromMongo(String mongoOp) {
        ArrayOp op = MONGO_LOOKUP.get(mongoOp);
        if (op == null) {
            throw new IllegalArgumentException("Unknown array operator: " + mongoOp);
        }
        return op;
    }

    /**
     * Returns true if the given operator is a known array operator.
     */
    public static boolean isArrayOp(String mongoOp) {
        return MONGO_LOOKUP.containsKey(mongoOp);
    }
}
