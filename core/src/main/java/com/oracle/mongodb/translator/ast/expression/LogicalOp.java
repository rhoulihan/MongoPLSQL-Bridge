/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

/** Logical operators mapping MongoDB to SQL. */
public enum LogicalOp {
  AND("AND", "$and"),
  OR("OR", "$or"),
  NOT("NOT", "$not"),
  NOR("NOR", "$nor");

  private final String sqlOperator;
  private final String mongoOperator;

  LogicalOp(String sqlOperator, String mongoOperator) {
    this.sqlOperator = sqlOperator;
    this.mongoOperator = mongoOperator;
  }

  /** Returns the SQL operator string. */
  public String getSqlOperator() {
    return sqlOperator;
  }

  /** Returns the MongoDB operator string. */
  public String getMongoOperator() {
    return mongoOperator;
  }

  /** Returns the LogicalOp for a MongoDB operator string. */
  public static LogicalOp fromMongo(String mongoOp) {
    for (LogicalOp op : values()) {
      if (op.mongoOperator.equals(mongoOp)) {
        return op;
      }
    }
    throw new IllegalArgumentException("Unknown logical operator: " + mongoOp);
  }
}
