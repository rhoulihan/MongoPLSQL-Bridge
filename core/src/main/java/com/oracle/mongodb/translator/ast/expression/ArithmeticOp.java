/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * Arithmetic operators for computed expressions. Maps MongoDB arithmetic operators to their Oracle
 * SQL equivalents.
 */
public enum ArithmeticOp {
  ADD("+", "$add", false),
  SUBTRACT("-", "$subtract", false),
  MULTIPLY("*", "$multiply", false),
  DIVIDE("/", "$divide", false),
  MOD("MOD", "$mod", true),
  ROUND("ROUND", "$round", true),
  ABS("ABS", "$abs", true),
  CEIL("CEIL", "$ceil", true),
  FLOOR("FLOOR", "$floor", true),
  TRUNC("TRUNC", "$trunc", true),
  SQRT("SQRT", "$sqrt", true),
  POW("POWER", "$pow", true),
  EXP("EXP", "$exp", true),
  LN("LN", "$ln", true),
  LOG10("LOG", "$log10", true),
  // $max and $min as expression operators (not accumulators)
  MAX("GREATEST", "$max", true),
  MIN("LEAST", "$min", true);

  private static final Map<String, ArithmeticOp> MONGO_LOOKUP;

  static {
    MONGO_LOOKUP = new java.util.HashMap<>();
    MONGO_LOOKUP.put("$add", ADD);
    MONGO_LOOKUP.put("$subtract", SUBTRACT);
    MONGO_LOOKUP.put("$multiply", MULTIPLY);
    MONGO_LOOKUP.put("$divide", DIVIDE);
    MONGO_LOOKUP.put("$mod", MOD);
    MONGO_LOOKUP.put("$round", ROUND);
    MONGO_LOOKUP.put("$abs", ABS);
    MONGO_LOOKUP.put("$ceil", CEIL);
    MONGO_LOOKUP.put("$floor", FLOOR);
    MONGO_LOOKUP.put("$trunc", TRUNC);
    MONGO_LOOKUP.put("$sqrt", SQRT);
    MONGO_LOOKUP.put("$pow", POW);
    MONGO_LOOKUP.put("$exp", EXP);
    MONGO_LOOKUP.put("$ln", LN);
    MONGO_LOOKUP.put("$log10", LOG10);
    MONGO_LOOKUP.put("$max", MAX);
    MONGO_LOOKUP.put("$min", MIN);
  }

  private final String sqlOperator;
  private final String mongoOperator;
  private final boolean isFunction;

  ArithmeticOp(String sqlOperator, String mongoOperator, boolean isFunction) {
    this.sqlOperator = sqlOperator;
    this.mongoOperator = mongoOperator;
    this.isFunction = isFunction;
  }

  /** Returns the Oracle SQL operator or function name. */
  public String getSqlOperator() {
    return sqlOperator;
  }

  /** Returns the MongoDB operator name. */
  public String getMongoOperator() {
    return mongoOperator;
  }

  /** Returns true if this operator requires a function call (like MOD, ROUND, etc.). */
  public boolean requiresFunctionCall() {
    return isFunction;
  }

  /** Returns true if this operator can accept a single argument. */
  public boolean allowsSingleOperand() {
    return this == ABS
        || this == CEIL
        || this == FLOOR
        || this == SQRT
        || this == EXP
        || this == LN
        || this == LOG10
        || this == TRUNC
        || this == ROUND;
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

  /** Returns true if the given operator is a known arithmetic operator. */
  public static boolean isArithmetic(String mongoOp) {
    return MONGO_LOOKUP.containsKey(mongoOp);
  }
}
