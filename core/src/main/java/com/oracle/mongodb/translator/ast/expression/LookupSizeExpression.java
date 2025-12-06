/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $size expression on a $lookup result field. Renders as a correlated subquery that
 * counts matching rows instead of accessing a non-existent JSON path.
 *
 * <p>Example MongoDB pipeline:
 *
 * <pre>
 * { $lookup: { from: "inventory", localField: "_id", foreignField: "productId", as: "inv" } }
 * { $project: { invCount: { $size: "$inv" } } }
 * </pre>
 *
 * <p>Generates SQL using dot notation for type preservation:
 *
 * <pre>
 * (SELECT COUNT(*) FROM inventory WHERE inventory.data.productId = base.data."_id")
 * </pre>
 */
public final class LookupSizeExpression implements Expression {

  private final String foreignTable;
  private final String localField;
  private final String foreignField;

  /**
   * Creates a lookup size expression.
   *
   * @param foreignTable the foreign table name (from $lookup "from")
   * @param localField the local field path (from $lookup "localField")
   * @param foreignField the foreign field path (from $lookup "foreignField")
   */
  public LookupSizeExpression(String foreignTable, String localField, String foreignField) {
    this.foreignTable = Objects.requireNonNull(foreignTable, "foreignTable must not be null");
    this.localField = Objects.requireNonNull(localField, "localField must not be null");
    this.foreignField = Objects.requireNonNull(foreignField, "foreignField must not be null");
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Generate correlated subquery using dot notation for type preservation:
    // (SELECT COUNT(*) FROM table t WHERE t.data."foreignField" = base.data."localField")
    // Note: We use an alias for the subquery table and quote ALL field names
    // because Oracle dot notation requires an alias for proper resolution in correlated
    // subqueries, and field names must be quoted to preserve case sensitivity.
    String subqueryAlias = "sz_" + foreignTable.substring(0, Math.min(3, foreignTable.length()));
    ctx.sql("(SELECT COUNT(*) FROM ");
    ctx.tableName(foreignTable);
    ctx.sql(" ");
    ctx.sql(subqueryAlias);
    ctx.sql(" WHERE ");
    ctx.sql(subqueryAlias);
    ctx.sql(".data.");
    ctx.sql(quotePath(foreignField));
    ctx.sql(" = ");
    String alias = ctx.getBaseTableAlias();
    if (alias != null && !alias.isEmpty()) {
      ctx.sql(alias);
      ctx.sql(".");
    }
    ctx.sql("data.");
    ctx.sql(quotePath(localField));
    ctx.sql(")");
  }

  /**
   * Quotes a field path for Oracle dot notation. ALL field names are quoted to preserve case
   * sensitivity, since Oracle identifiers are case-insensitive when unquoted but JSON field names
   * are case-sensitive.
   */
  private static String quotePath(String path) {
    String[] segments = path.split("\\.");
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        result.append(".");
      }
      String segment = segments[i];
      if (!segment.isEmpty()) {
        // Always quote to preserve case sensitivity
        result.append("\"").append(segment).append("\"");
      }
    }
    return result.toString();
  }

  @Override
  public String toString() {
    return "LookupSize(" + foreignTable + "." + foreignField + " = " + localField + ")";
  }
}
