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

  /**
   * Returns the normalized path for dot notation (e.g., "status" or "customer.address.city").
   * Validates the path to prevent injection attacks. Quotes field names that start with underscore
   * or contain special characters that Oracle doesn't allow in unquoted identifiers.
   */
  public String getDotNotationPath() {
    String normalizedPath = path.startsWith("$") ? path.substring(1) : path;
    if (normalizedPath.startsWith(".")) {
      normalizedPath = normalizedPath.substring(1);
    }
    // Validate path to prevent injection
    FieldNameValidator.validateFieldName(normalizedPath);

    // Quote field names that need it for Oracle dot notation
    String[] segments = normalizedPath.split("\\.");
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        result.append(".");
      }
      result.append(quoteIfNeeded(segments[i]));
    }
    return result.toString();
  }

  /**
   * Quotes a field name if it starts with underscore or contains characters that Oracle doesn't
   * allow in unquoted identifiers. Oracle identifiers must start with a letter and contain only
   * letters, digits, and underscores.
   */
  private static String quoteIfNeeded(String fieldName) {
    if (fieldName.isEmpty()) {
      return fieldName;
    }
    // Oracle requires quoting if:
    // 1. Starts with underscore or digit
    // 2. Contains characters other than letters, digits, underscores
    char first = fieldName.charAt(0);
    boolean needsQuoting = !Character.isLetter(first);

    if (!needsQuoting) {
      for (int i = 1; i < fieldName.length(); i++) {
        char c = fieldName.charAt(i);
        if (!Character.isLetterOrDigit(c) && c != '_') {
          needsQuoting = true;
          break;
        }
      }
    }

    if (needsQuoting) {
      return "\"" + fieldName + "\"";
    }
    return fieldName;
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

    // Check if this path references a $lookup result field
    // e.g., "customer.tier" where "customer" is from $lookup
    String lookupAlias = ctx.getLookupTableAlias(normalizedPath);
    if (lookupAlias != null && "data".equals(dataColumn)) {
      renderLookupFieldPath(ctx, lookupAlias, normalizedPath);
      return;
    }

    // Check if this path references an unwound array element
    // e.g., "items.product" where "items" has been $unwind'd
    SqlGenerationContext.UnwindInfo unwindInfo = ctx.getUnwindInfo(normalizedPath);
    if (unwindInfo != null && "data".equals(dataColumn)) {
      renderUnwindFieldPath(ctx, unwindInfo);
      return;
    }

    // Use Oracle dot notation: alias.data.field instead of JSON_VALUE(alias.data, '$.field')
    String baseAlias = ctx.getBaseTableAlias();
    String dotPath = getDotNotationPath();

    // Build the dot notation expression
    StringBuilder dotExpr = new StringBuilder();
    if (baseAlias != null && !baseAlias.isEmpty() && "data".equals(dataColumn)) {
      dotExpr.append(baseAlias).append(".");
    }
    dotExpr.append(dataColumn).append(".").append(dotPath);

    if (returnType != null) {
      // Use CAST for type conversion with dot notation
      ctx.sql("CAST(");
      ctx.sql(dotExpr.toString());
      ctx.sql(" AS ");
      ctx.sql(returnType.getOracleSyntax());
      ctx.sql(")");
    } else {
      ctx.sql(dotExpr.toString());
    }
  }

  /**
   * Renders a field path that references a $lookup result. Redirects to the joined table's data
   * column with the nested path using Oracle dot notation.
   */
  private void renderLookupFieldPath(SqlGenerationContext ctx, String lookupAlias, String path) {
    // path is like "customer.tier", lookupAlias is like "customers_1"
    // We generate: customers_1.data.tier (or CAST(customers_1.data.tier AS NUMBER) for returnType)
    int dotIndex = path.indexOf('.');
    final String remainingPath = dotIndex >= 0 ? path.substring(dotIndex + 1) : "";

    // Build dot notation expression
    StringBuilder dotExpr = new StringBuilder();
    dotExpr.append(lookupAlias).append(".data");
    if (!remainingPath.isEmpty()) {
      // Validate the remaining path
      FieldNameValidator.validateFieldName(remainingPath);
      dotExpr.append(".").append(remainingPath);
    }

    if (returnType != null) {
      ctx.sql("CAST(");
      ctx.sql(dotExpr.toString());
      ctx.sql(" AS ");
      ctx.sql(returnType.getOracleSyntax());
      ctx.sql(")");
    } else {
      ctx.sql(dotExpr.toString());
    }
  }

  /**
   * Renders a field path that references an unwound array element. Redirects to the JSON_TABLE's
   * value column with the remaining path using Oracle dot notation.
   *
   * <p>Example: After "$unwind: $items", "$items.product" becomes: unwind_1.value.product
   */
  private void renderUnwindFieldPath(
      SqlGenerationContext ctx, SqlGenerationContext.UnwindInfo info) {
    // Build dot notation expression
    StringBuilder dotExpr = new StringBuilder();
    dotExpr.append(info.tableAlias()).append(".value");
    if (!info.remainingPath().isEmpty()) {
      // Validate the remaining path
      FieldNameValidator.validateFieldName(info.remainingPath());
      dotExpr.append(".").append(info.remainingPath());
    }

    if (returnType != null) {
      ctx.sql("CAST(");
      ctx.sql(dotExpr.toString());
      ctx.sql(" AS ");
      ctx.sql(returnType.getOracleSyntax());
      ctx.sql(")");
    } else {
      ctx.sql(dotExpr.toString());
    }
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
