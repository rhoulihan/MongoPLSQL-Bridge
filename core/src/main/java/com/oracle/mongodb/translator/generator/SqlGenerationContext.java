/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.generator.dialect.OracleDialect;
import java.util.List;

/** Context for SQL generation. AST nodes use this interface to build SQL. */
public interface SqlGenerationContext {

  /** Appends a raw SQL fragment. */
  void sql(String fragment);

  /** Recursively renders a child AST node. */
  void visit(AstNode node);

  /** Adds a bind variable and appends the placeholder. */
  void bind(Object value);

  /** Appends an identifier, quoting if necessary. */
  void identifier(String name);

  /** Returns whether values should be inlined (for debugging). */
  boolean inline();

  /** Returns the target Oracle dialect. */
  OracleDialect dialect();

  /** Returns the generated SQL string. */
  String toSql();

  /** Returns the collected bind variables. */
  List<Object> getBindVariables();

  /**
   * Generates a unique table alias for the given table name. Used primarily for $lookup joins to
   * ensure unique aliases.
   *
   * @param tableName the base table name
   * @return a unique alias (e.g., "inventory_1", "inventory_2")
   */
  String generateTableAlias(String tableName);

  /**
   * Returns the alias for the base (main) table in the query. Default is "base" but may be
   * configured differently.
   *
   * @return the base table alias
   */
  String getBaseTableAlias();

  /**
   * Creates a nested context for rendering sub-expressions. The nested context shares the same
   * settings but has its own SQL buffer.
   *
   * @return a new nested context
   */
  SqlGenerationContext createNestedContext();
}
