/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * Array operators for expressions. Maps MongoDB array operators to their Oracle SQL equivalents.
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
  SLICE("$slice"),
  REVERSE_ARRAY("$reverseArray"),
  SORT_ARRAY("$sortArray"),
  IN("$in"),
  IS_ARRAY("$isArray"),
  INDEX_OF_ARRAY("$indexOfArray"),
  ARRAY_TO_OBJECT("$arrayToObject"),
  OBJECT_TO_ARRAY("$objectToArray"),
  SET_UNION("$setUnion"),
  SET_INTERSECTION("$setIntersection"),
  SET_DIFFERENCE("$setDifference"),
  SET_EQUALS("$setEquals"),
  SET_IS_SUBSET("$setIsSubset"),
  ANY_ELEMENT_TRUE("$anyElementTrue"),
  ALL_ELEMENTS_TRUE("$allElementsTrue");

  private static final Map<String, ArrayOp> MONGO_LOOKUP;

  static {
    MONGO_LOOKUP =
        Map.ofEntries(
            Map.entry("$arrayElemAt", ARRAY_ELEM_AT),
            Map.entry("$size", SIZE),
            Map.entry("$first", FIRST),
            Map.entry("$last", LAST),
            Map.entry("$filter", FILTER),
            Map.entry("$map", MAP),
            Map.entry("$reduce", REDUCE),
            Map.entry("$concatArrays", CONCAT_ARRAYS),
            Map.entry("$slice", SLICE),
            Map.entry("$reverseArray", REVERSE_ARRAY),
            Map.entry("$sortArray", SORT_ARRAY),
            Map.entry("$in", IN),
            Map.entry("$isArray", IS_ARRAY),
            Map.entry("$indexOfArray", INDEX_OF_ARRAY),
            Map.entry("$arrayToObject", ARRAY_TO_OBJECT),
            Map.entry("$objectToArray", OBJECT_TO_ARRAY),
            Map.entry("$setUnion", SET_UNION),
            Map.entry("$setIntersection", SET_INTERSECTION),
            Map.entry("$setDifference", SET_DIFFERENCE),
            Map.entry("$setEquals", SET_EQUALS),
            Map.entry("$setIsSubset", SET_IS_SUBSET),
            Map.entry("$anyElementTrue", ANY_ELEMENT_TRUE),
            Map.entry("$allElementsTrue", ALL_ELEMENTS_TRUE));
  }

  private final String mongoOperator;

  ArrayOp(String mongoOperator) {
    this.mongoOperator = mongoOperator;
  }

  /** Returns the MongoDB operator name. */
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

  /** Returns true if the given operator is a known array operator. */
  public static boolean isArrayOp(String mongoOp) {
    return MONGO_LOOKUP.containsKey(mongoOp);
  }
}
