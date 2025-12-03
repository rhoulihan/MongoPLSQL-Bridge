/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * Object operators for expressions. Maps MongoDB object operators to their Oracle SQL equivalents.
 */
public enum ObjectOp {
  MERGE_OBJECTS("$mergeObjects"),
  OBJECT_TO_ARRAY("$objectToArray"),
  ARRAY_TO_OBJECT("$arrayToObject");

  private static final Map<String, ObjectOp> MONGO_LOOKUP;

  static {
    MONGO_LOOKUP =
        Map.ofEntries(
            Map.entry("$mergeObjects", MERGE_OBJECTS),
            Map.entry("$objectToArray", OBJECT_TO_ARRAY),
            Map.entry("$arrayToObject", ARRAY_TO_OBJECT));
  }

  private final String mongoOperator;

  ObjectOp(String mongoOperator) {
    this.mongoOperator = mongoOperator;
  }

  /** Returns the MongoDB operator name. */
  public String getMongoOperator() {
    return mongoOperator;
  }

  /**
   * Returns the ObjectOp for the given MongoDB operator.
   *
   * @param mongoOp MongoDB operator (e.g., "$mergeObjects")
   * @return corresponding ObjectOp
   * @throws IllegalArgumentException if operator is not recognized
   */
  public static ObjectOp fromMongo(String mongoOp) {
    ObjectOp op = MONGO_LOOKUP.get(mongoOp);
    if (op == null) {
      throw new IllegalArgumentException("Unknown object operator: " + mongoOp);
    }
    return op;
  }

  /** Returns true if the given operator is a known object operator. */
  public static boolean isObjectOp(String mongoOp) {
    return MONGO_LOOKUP.containsKey(mongoOp);
  }
}
