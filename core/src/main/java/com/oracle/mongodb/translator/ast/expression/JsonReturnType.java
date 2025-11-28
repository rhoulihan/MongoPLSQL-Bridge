/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

/** JSON_VALUE RETURNING clause types. */
public enum JsonReturnType {
  VARCHAR("VARCHAR2(4000)"),
  NUMBER("NUMBER"),
  DATE("DATE"),
  TIMESTAMP("TIMESTAMP"),
  BOOLEAN("VARCHAR2(5)"),
  JSON("JSON");

  private final String oracleSyntax;

  JsonReturnType(String oracleSyntax) {
    this.oracleSyntax = oracleSyntax;
  }

  public String getOracleSyntax() {
    return oracleSyntax;
  }
}
