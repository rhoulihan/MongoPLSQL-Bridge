/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.ast.expression.Expression;
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

  /**
   * Appends a validated field name for use in JSON paths. Validates that the field name contains
   * only safe characters to prevent JSON path injection.
   *
   * @param fieldName the field name to validate and append
   * @throws com.oracle.mongodb.translator.exception.ValidationException if field name is invalid
   */
  void jsonField(String fieldName);

  /**
   * Appends a validated table/collection name. Validates that the table name contains only safe
   * characters and quotes it if necessary.
   *
   * @param tableName the table name to validate and append
   * @throws com.oracle.mongodb.translator.exception.ValidationException if table name is invalid
   */
  void tableName(String tableName);

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

  /**
   * Registers a virtual field defined by $addFields. When a subsequent stage references this field,
   * the expression will be inlined rather than generating a JSON path.
   *
   * @param fieldName the virtual field name (without $ prefix)
   * @param expression the expression that defines this field
   */
  void registerVirtualField(String fieldName, Expression expression);

  /**
   * Looks up a virtual field defined by $addFields. Returns the expression if found, null
   * otherwise.
   *
   * @param fieldName the virtual field name (without $ prefix)
   * @return the expression defining this field, or null if not a virtual field
   */
  Expression getVirtualField(String fieldName);

  /**
   * Registers a $lookup field. When $size is called on this field, it will generate a correlated
   * subquery COUNT instead of a JSON path.
   *
   * @param asField the output array field name (from $lookup "as")
   * @param foreignTable the foreign table name (from $lookup "from")
   * @param localField the local field path (from $lookup "localField")
   * @param foreignField the foreign field path (from $lookup "foreignField")
   */
  void registerLookupField(
      String asField, String foreignTable, String localField, String foreignField);

  /**
   * Creates an expression for $size on a lookup field. Returns a LookupSizeExpression if the field
   * is from a $lookup, null otherwise. Also marks the lookup as "consumed" (used only for size).
   *
   * @param fieldName the field name to check
   * @return LookupSizeExpression if this is a lookup field, null otherwise
   */
  Expression getLookupSizeExpression(String fieldName);

  /**
   * Checks if a lookup field has been "consumed" by a $size operation. If a lookup is only used
   * with $size, the JOIN should be skipped since the correlated subquery replaces it.
   *
   * @param asField the lookup "as" field name
   * @return true if this lookup was used only with $size
   */
  boolean isLookupConsumedBySize(String asField);

  /**
   * Registers the SQL table alias for a $lookup stage. When a field path starts with the lookup's
   * "as" field, we need to redirect it to the joined table's data column.
   *
   * @param asField the lookup's "as" field name
   * @param tableAlias the SQL table alias generated for this lookup
   */
  void registerLookupTableAlias(String asField, String tableAlias);

  /**
   * Gets the SQL table alias for a $lookup field. Returns null if the field is not from a lookup.
   *
   * @param fieldPath the field path (e.g., "customer.tier")
   * @return the table alias if this is a lookup field, null otherwise
   */
  String getLookupTableAlias(String fieldPath);

  /**
   * Gets the SQL table alias for a $lookup by its "as" field name. Returns null if not found.
   *
   * @param asField the lookup's "as" field name
   * @return the table alias if registered, null otherwise
   */
  String getLookupTableAliasByAs(String asField);
}
