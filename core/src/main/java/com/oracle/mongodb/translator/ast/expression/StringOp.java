/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import java.util.Map;

/**
 * String operators for expressions. Maps MongoDB string operators to their Oracle SQL equivalents.
 */
public enum StringOp {
  CONCAT("CONCAT", "$concat"),
  TO_LOWER("LOWER", "$toLower"),
  TO_UPPER("UPPER", "$toUpper"),
  SUBSTR("SUBSTR", "$substr"),
  TRIM("TRIM", "$trim"),
  LTRIM("LTRIM", "$ltrim"),
  RTRIM("RTRIM", "$rtrim"),
  STRLEN("LENGTH", "$strLenCP"),
  SPLIT("SPLIT", "$split"),
  INDEX_OF_CP("INSTR", "$indexOfCP"),
  REGEX_MATCH("REGEXP_LIKE", "$regexMatch"),
  REGEX_FIND("REGEXP_INSTR", "$regexFind"),
  REPLACE_ONE("REGEXP_REPLACE", "$replaceOne"),
  REPLACE_ALL("REGEXP_REPLACE", "$replaceAll");

  private static final Map<String, StringOp> MONGO_LOOKUP;

  static {
    MONGO_LOOKUP =
        Map.ofEntries(
            Map.entry("$concat", CONCAT),
            Map.entry("$toLower", TO_LOWER),
            Map.entry("$toUpper", TO_UPPER),
            Map.entry("$substr", SUBSTR),
            Map.entry("$trim", TRIM),
            Map.entry("$ltrim", LTRIM),
            Map.entry("$rtrim", RTRIM),
            Map.entry("$strLenCP", STRLEN),
            Map.entry("$split", SPLIT),
            Map.entry("$indexOfCP", INDEX_OF_CP),
            Map.entry("$regexMatch", REGEX_MATCH),
            Map.entry("$regexFind", REGEX_FIND),
            Map.entry("$replaceOne", REPLACE_ONE),
            Map.entry("$replaceAll", REPLACE_ALL));
  }

  private final String sqlFunction;
  private final String mongoOperator;

  StringOp(String sqlFunction, String mongoOperator) {
    this.sqlFunction = sqlFunction;
    this.mongoOperator = mongoOperator;
  }

  /** Returns the Oracle SQL function name. */
  public String getSqlFunction() {
    return sqlFunction;
  }

  /** Returns the MongoDB operator name. */
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

  /** Returns true if the given operator is a known string operator. */
  public static boolean isStringOp(String mongoOp) {
    return MONGO_LOOKUP.containsKey(mongoOp);
  }
}
