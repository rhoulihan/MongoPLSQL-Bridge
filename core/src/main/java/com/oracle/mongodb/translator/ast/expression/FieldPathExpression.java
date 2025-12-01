/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import com.oracle.mongodb.translator.util.FieldNameValidator;
import java.util.Objects;

/** Represents a field path reference like "$status" or "$customer.address.city". */
public final class FieldPathExpression implements Expression {

  private final String path;
  private final JsonReturnType returnType;
  private final String dataColumn;

  private FieldPathExpression(String path, JsonReturnType returnType, String dataColumn) {
    this.path = Objects.requireNonNull(path, "path must not be null");
    this.returnType = returnType;
    this.dataColumn = dataColumn;
  }

  /** Creates a field path expression with default data column. */
  public static FieldPathExpression of(String path) {
    return new FieldPathExpression(path, null, "data");
  }

  /** Creates a field path expression with a return type. */
  public static FieldPathExpression of(String path, JsonReturnType returnType) {
    return new FieldPathExpression(path, returnType, "data");
  }

  /** Creates a field path expression with return type and custom data column. */
  public static FieldPathExpression of(String path, JsonReturnType returnType, String dataColumn) {
    return new FieldPathExpression(path, returnType, dataColumn != null ? dataColumn : "data");
  }

  /**
   * Returns the JSON path for this field (e.g., "$.status"). Validates the path to prevent JSON
   * path injection attacks.
   */
  public String getJsonPath() {
    String normalizedPath = path.startsWith("$") ? path.substring(1) : path;
    if (normalizedPath.startsWith(".")) {
      normalizedPath = normalizedPath.substring(1);
    }
    // Validate path to prevent JSON path injection
    FieldNameValidator.validateFieldName(normalizedPath);
    return "$." + normalizedPath;
  }

  public String getPath() {
    return path;
  }

  public JsonReturnType getReturnType() {
    return returnType;
  }

  public String getDataColumn() {
    return dataColumn;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Check if this path references a virtual field defined by $addFields
    String normalizedPath = path.startsWith("$") ? path.substring(1) : path;
    if (normalizedPath.startsWith(".")) {
      normalizedPath = normalizedPath.substring(1);
    }
    Expression virtualExpr = ctx.getVirtualField(normalizedPath);
    if (virtualExpr != null) {
      // Inline the virtual field expression instead of generating JSON path
      ctx.visit(virtualExpr);
      return;
    }

    ctx.sql("JSON_VALUE(");
    // Use table alias qualified column name when there's a base alias
    String baseAlias = ctx.getBaseTableAlias();
    if (baseAlias != null && !baseAlias.isEmpty() && "data".equals(dataColumn)) {
      ctx.sql(baseAlias);
      ctx.sql(".");
    }
    ctx.sql(dataColumn);
    ctx.sql(", '");
    ctx.sql(getJsonPath());
    ctx.sql("'");

    if (returnType != null) {
      ctx.sql(" RETURNING ");
      ctx.sql(returnType.getOracleSyntax());
    }

    ctx.sql(")");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    FieldPathExpression that = (FieldPathExpression) obj;
    return Objects.equals(path, that.path)
        && returnType == that.returnType
        && Objects.equals(dataColumn, that.dataColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, returnType, dataColumn);
  }

  @Override
  public String toString() {
    return "FieldPath($" + path + ")";
  }
}
