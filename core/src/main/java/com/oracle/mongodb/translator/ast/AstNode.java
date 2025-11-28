/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Base interface for all AST nodes. Nodes render themselves to SQL via the context pattern
 * (jOOQ-inspired).
 */
@FunctionalInterface
public interface AstNode {

  /**
   * Renders this node to SQL using the provided context.
   *
   * @param ctx the SQL generation context
   */
  void render(SqlGenerationContext ctx);
}
