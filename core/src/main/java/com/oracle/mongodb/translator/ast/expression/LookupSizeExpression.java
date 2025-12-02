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
 * <p>Generates SQL:
 *
 * <pre>
 * (SELECT COUNT(*) FROM inventory WHERE JSON_VALUE(inventory.data, '$.productId')
 *    = JSON_VALUE(base.data, '$._id'))
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
    // Generate correlated subquery: (SELECT COUNT(*) FROM table WHERE foreign = local)
    ctx.sql("(SELECT COUNT(*) FROM ");
    ctx.tableName(foreignTable);
    ctx.sql(" WHERE JSON_VALUE(");
    ctx.tableName(foreignTable);
    ctx.sql(".data, '$.");
    ctx.jsonField(foreignField);
    ctx.sql("') = JSON_VALUE(");
    String alias = ctx.getBaseTableAlias();
    if (alias != null && !alias.isEmpty()) {
      ctx.sql(alias);
      ctx.sql(".");
    }
    ctx.sql("data, '$.");
    ctx.jsonField(localField);
    ctx.sql("'))");
  }

  @Override
  public String toString() {
    return "LookupSize(" + foreignTable + "." + foreignField + " = " + localField + ")";
  }
}
