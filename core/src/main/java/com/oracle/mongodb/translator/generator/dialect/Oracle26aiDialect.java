/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator.dialect;

/** Oracle 26ai dialect with full JSON support. */
public final class Oracle26aiDialect implements OracleDialect {

  public static final Oracle26aiDialect INSTANCE = new Oracle26aiDialect();

  private Oracle26aiDialect() {}

  @Override
  public String name() {
    return "Oracle 26ai";
  }

  @Override
  public boolean supportsJsonValueReturning() {
    return true;
  }

  @Override
  public boolean supportsNestedPath() {
    return true;
  }

  @Override
  public boolean supportsJsonCollectionTables() {
    return true;
  }
}
