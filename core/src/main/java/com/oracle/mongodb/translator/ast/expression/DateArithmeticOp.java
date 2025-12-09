/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * Date arithmetic operators for date computations. Maps MongoDB date arithmetic operators to their
 * Oracle SQL equivalents.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$dateAdd: {startDate: "$date", unit: "day", amount: 5}}} becomes {@code timestamp +
 *       INTERVAL '5' DAY}
 *   <li>{@code {$dateSubtract: {startDate: "$date", unit: "month", amount: 1}}} becomes {@code
 *       timestamp - INTERVAL '1' MONTH}
 *   <li>{@code {$dateDiff: {startDate: "$start", endDate: "$end", unit: "day"}}} becomes date
 *       subtraction
 * </ul>
 */
public enum DateArithmeticOp {
  DATE_ADD("$dateAdd", "+"),
  DATE_SUBTRACT("$dateSubtract", "-"),
  DATE_DIFF("$dateDiff", null);

  private static final Map<String, DateArithmeticOp> MONGO_LOOKUP;

  static {
    MONGO_LOOKUP = new java.util.HashMap<>();
    MONGO_LOOKUP.put("$dateAdd", DATE_ADD);
    MONGO_LOOKUP.put("$dateSubtract", DATE_SUBTRACT);
    MONGO_LOOKUP.put("$dateDiff", DATE_DIFF);
  }

  private final String mongoOperator;
  private final String sqlOperator;

  DateArithmeticOp(String mongoOperator, String sqlOperator) {
    this.mongoOperator = mongoOperator;
    this.sqlOperator = sqlOperator;
  }

  /** Returns the MongoDB operator name. */
  public String getMongoOperator() {
    return mongoOperator;
  }

  /** Returns the Oracle SQL operator (+ or -) for add/subtract, null for diff. */
  public String getSqlOperator() {
    return sqlOperator;
  }

  /**
   * Returns the DateArithmeticOp for the given MongoDB operator.
   *
   * @param mongoOp MongoDB operator (e.g., "$dateAdd")
   * @return corresponding DateArithmeticOp
   * @throws IllegalArgumentException if operator is not recognized
   */
  public static DateArithmeticOp fromMongo(String mongoOp) {
    DateArithmeticOp op = MONGO_LOOKUP.get(mongoOp);
    if (op == null) {
      throw new IllegalArgumentException("Unknown date arithmetic operator: " + mongoOp);
    }
    return op;
  }

  /** Returns true if the given operator is a known date arithmetic operator. */
  public static boolean isDateArithmeticOp(String mongoOp) {
    return MONGO_LOOKUP.containsKey(mongoOp);
  }
}
