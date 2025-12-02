/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents an $exists expression that checks if a field exists in a document.
 *
 * <p>MongoDB's $exists: true matches documents where the field exists, while $exists: false matches
 * documents where the field does not exist.
 *
 * <p>In Oracle, this is translated to JSON_EXISTS() or NOT JSON_EXISTS().
 */
public final class ExistsExpression implements Expression {

  private final String fieldPath;
  private final boolean exists;

  /**
   * Creates an exists expression.
   *
   * @param fieldPath the field path to check for existence
   * @param exists true to match documents where field exists, false to match where it doesn't
   */
  public ExistsExpression(String fieldPath, boolean exists) {
    this.fieldPath = Objects.requireNonNull(fieldPath, "fieldPath must not be null");
    this.exists = exists;
  }

  /** Returns the field path being checked. */
  public String getFieldPath() {
    return fieldPath;
  }

  /** Returns true if checking for existence, false if checking for non-existence. */
  public boolean isExists() {
    return exists;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    if (!exists) {
      ctx.sql("NOT ");
    }
    ctx.sql("JSON_EXISTS(");
    String alias = ctx.getBaseTableAlias();
    if (alias != null && !alias.isEmpty()) {
      ctx.sql(alias);
      ctx.sql(".");
    }
    ctx.sql("data, '$.");
    ctx.sql(fieldPath);
    ctx.sql("')");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ExistsExpression that = (ExistsExpression) obj;
    return exists == that.exists && Objects.equals(fieldPath, that.fieldPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldPath, exists);
  }

  @Override
  public String toString() {
    return "Exists(" + fieldPath + ", " + exists + ")";
  }
}
