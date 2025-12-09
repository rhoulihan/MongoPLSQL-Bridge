/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.ast.AstNode;

/** Base interface for all expression types in the AST. */
public interface Expression extends AstNode {

  /**
   * Returns true if this expression produces a boolean result. Oracle doesn't support boolean as a
   * column value, so boolean expressions need to be wrapped in CASE WHEN when used in SELECT
   * context.
   *
   * @return true if this expression produces a boolean result
   */
  default boolean isBooleanExpression() {
    return false;
  }
}
