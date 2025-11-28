/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator.dialect;

/** Represents an Oracle database dialect version. */
public interface OracleDialect {

  /** Returns the dialect name. */
  String name();

  /** Returns true if JSON_VALUE supports RETURNING clause. */
  boolean supportsJsonValueReturning();

  /** Returns true if JSON_TABLE supports NESTED PATH. */
  boolean supportsNestedPath();

  /** Returns true if JSON COLLECTION TABLEs are supported. */
  boolean supportsJsonCollectionTables();
}
