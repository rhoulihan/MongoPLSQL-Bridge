/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

/**
 * Enumeration of date string conversion operations. Maps MongoDB date string operators to their
 * Oracle equivalents.
 */
public enum DateStringOp {
  /** Parse string to date - MongoDB: $dateFromString, Oracle: TO_TIMESTAMP. */
  DATE_FROM_STRING("$dateFromString"),

  /** Format date as string - MongoDB: $dateToString, Oracle: TO_CHAR. */
  DATE_TO_STRING("$dateToString");

  private final String mongoOperator;

  DateStringOp(String mongoOperator) {
    this.mongoOperator = mongoOperator;
  }

  /** Returns the MongoDB operator name. */
  public String getMongoOperator() {
    return mongoOperator;
  }

  /** Returns true if the given operator is a date string operation. */
  public static boolean isDateStringOp(String op) {
    return "$dateFromString".equals(op) || "$dateToString".equals(op);
  }

  /** Returns the DateStringOp for the given MongoDB operator. */
  public static DateStringOp fromMongo(String op) {
    for (DateStringOp dateOp : values()) {
      if (dateOp.mongoOperator.equals(op)) {
        return dateOp;
      }
    }
    throw new IllegalArgumentException("Unknown date string operator: " + op);
  }
}
