/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $replaceRoot stage that promotes a sub-document to become the new root document.
 * Translates to Oracle's SELECT clause extracting the specified expression as the new document.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$replaceRoot: {newRoot: "$subdocument"}}} - promotes subdocument to root
 *   <li>{@code {$replaceRoot: {newRoot: {$mergeObjects: ["$defaults", "$doc"]}}}} - merged object
 *       becomes root
 * </ul>
 */
public final class ReplaceRootStage implements Stage {

  private final Expression newRoot;

  /**
   * Creates a $replaceRoot stage.
   *
   * @param newRoot the expression that becomes the new root document
   */
  public ReplaceRootStage(Expression newRoot) {
    this.newRoot = Objects.requireNonNull(newRoot, "newRoot must not be null");
  }

  /** Returns the expression that becomes the new root. */
  public Expression getNewRoot() {
    return newRoot;
  }

  @Override
  public String getOperatorName() {
    return "$replaceRoot";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // $replaceRoot promotes a sub-document to become the new root
    // In Oracle JSON, we select the subdocument as the new data column
    ctx.sql("SELECT ");

    if (newRoot instanceof FieldPathExpression fieldPath) {
      // Extract the subdocument using JSON_QUERY
      ctx.sql("JSON_QUERY(");
      String alias = ctx.getBaseTableAlias();
      if (alias != null && !alias.isEmpty()) {
        ctx.sql(alias);
        ctx.sql(".");
      }
      ctx.sql("data, '$.");
      ctx.sql(fieldPath.getPath());
      ctx.sql("') AS data");
    } else {
      // For computed expressions (like $mergeObjects), render the expression
      ctx.visit(newRoot);
      ctx.sql(" AS data");
    }
  }

  @Override
  public String toString() {
    return "ReplaceRootStage(" + newRoot + ")";
  }
}
