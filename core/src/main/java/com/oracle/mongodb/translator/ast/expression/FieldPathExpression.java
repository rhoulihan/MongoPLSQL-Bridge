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
   * Quotes a field name if it starts with underscore or digit, which Oracle JSON dot notation
   * doesn't support without quoting. Oracle identifiers must start with a letter when unquoted.
   */
  private static String quoteIfNeeded(String fieldName) {
    if (fieldName.isEmpty()) {
      return fieldName;
    }
    // Oracle JSON dot notation requires quoting if field starts with underscore or digit
    char first = fieldName.charAt(0);
    if (!Character.isLetter(first)) {
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

    // In CTE context, field references are plain column names from previous CTE
    // e.g., "totalRevenue" -> totalRevenue (not data.totalRevenue or JSON_VALUE)
    if (ctx.isInCteContext()) {
      // Check for compound _id references: "_id.category" -> "category"
      // Previous CTE with compound _id flattens fields to plain column names
      String resolvedPath = normalizedPath;
      if (normalizedPath.startsWith("_id.")) {
        String fieldAfterPrefix = normalizedPath.substring(4);
        if (ctx.isCompoundIdField(fieldAfterPrefix)) {
          resolvedPath = fieldAfterPrefix;
        }
      }
      // Quote identifiers starting with underscore for Oracle compatibility
      if (!resolvedPath.isEmpty() && !Character.isLetter(resolvedPath.charAt(0))) {
        ctx.sql("\"" + resolvedPath + "\"");
      } else {
        ctx.sql(resolvedPath);
      }
      return;
    }

    // Check if this path references a pipeline form $lookup result
    // Pipeline lookups use LATERAL subquery with JSON_ARRAYAGG, so the result is a column
    // e.g., "orders" -> lateral_alias.orders (not lateral_alias.data.orders)
    String pipelineLookupAlias = checkPipelineLookupPath(ctx, normalizedPath);
    if (pipelineLookupAlias != null && "data".equals(dataColumn)) {
      renderPipelineLookupFieldPath(ctx, pipelineLookupAlias, normalizedPath);
      return;
    }

    // Check if this path references an equality form $lookup result field
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

    // In JSON output mode, use JSON_QUERY to preserve native JSON types
    if (ctx.isJsonOutputMode() && returnType == null) {
      renderAsJsonQuery(ctx, normalizedPath);
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
      // Use JSON_VALUE with RETURNING for type conversion
      // CAST doesn't work with Oracle dot notation on JSON columns
      ctx.sql("JSON_VALUE(");
      if (baseAlias != null && !baseAlias.isEmpty() && "data".equals(dataColumn)) {
        ctx.sql(baseAlias);
        ctx.sql(".");
      }
      ctx.sql(dataColumn);
      ctx.sql(", '$.");
      // Quote field names that need quoting in JSON path
      String[] segments = dotPath.split("\\.");
      for (int i = 0; i < segments.length; i++) {
        if (i > 0) {
          ctx.sql(".");
        }
        String segment = segments[i];
        // Remove existing quotes if any (from getDotNotationPath)
        if (segment.startsWith("\"") && segment.endsWith("\"")) {
          ctx.sql(segment);
        } else if (!segment.isEmpty() && !Character.isLetter(segment.charAt(0))) {
          ctx.sql("\"");
          ctx.sql(segment);
          ctx.sql("\"");
        } else {
          ctx.sql(segment);
        }
      }
      ctx.sql("' RETURNING ");
      ctx.sql(returnType.getOracleSyntax());
      ctx.sql(")");
    } else {
      ctx.sql(dotExpr.toString());
    }
  }

  /**
   * Renders the field path as JSON_QUERY to preserve native JSON types. Used when rendering values
   * for JSON_OBJECT output, where dot notation would lose type information.
   */
  private void renderAsJsonQuery(SqlGenerationContext ctx, String normalizedPath) {
    String baseAlias = ctx.getBaseTableAlias();

    // Build JSON_QUERY expression: JSON_QUERY(base.data, '$.field')
    ctx.sql("JSON_QUERY(");
    if (baseAlias != null && !baseAlias.isEmpty() && "data".equals(dataColumn)) {
      ctx.sql(baseAlias);
      ctx.sql(".");
    }
    ctx.sql(dataColumn);
    ctx.sql(", '$.");

    // Quote field names that need quoting in JSON path
    String[] segments = normalizedPath.split("\\.");
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        ctx.sql(".");
      }
      String segment = segments[i];
      // Quote if starts with underscore or digit, or contains special chars
      if (!segment.isEmpty() && !Character.isLetter(segment.charAt(0))) {
        ctx.sql("\"");
        ctx.sql(segment);
        ctx.sql("\"");
      } else {
        ctx.sql(segment);
      }
    }
    ctx.sql("')");
  }

  /**
   * Renders a field path that references a $lookup result. Redirects to the joined table's data
   * column with the nested path using Oracle dot notation.
   */
  private void renderLookupFieldPath(SqlGenerationContext ctx, String lookupAlias, String path) {
    // path is like "customer.tier", lookupAlias is like "customers_1"
    // We generate: customers_1.data.tier (or JSON_VALUE(...) for returnType)
    int dotIndex = path.indexOf('.');
    final String remainingPath = dotIndex >= 0 ? path.substring(dotIndex + 1) : "";

    if (returnType != null) {
      // Use JSON_VALUE with RETURNING for type conversion
      // CAST doesn't work with Oracle dot notation on JSON columns
      ctx.sql("JSON_VALUE(");
      ctx.sql(lookupAlias);
      ctx.sql(".data, '$");
      if (!remainingPath.isEmpty()) {
        FieldNameValidator.validateFieldName(remainingPath);
        ctx.sql(".");
        renderJsonPathSegments(ctx, remainingPath);
      }
      ctx.sql("' RETURNING ");
      ctx.sql(returnType.getOracleSyntax());
      ctx.sql(")");
    } else {
      // Build dot notation expression with proper quoting
      ctx.sql(lookupAlias);
      ctx.sql(".data");
      if (!remainingPath.isEmpty()) {
        FieldNameValidator.validateFieldName(remainingPath);
        ctx.sql(".");
        renderDotNotationSegments(ctx, remainingPath);
      }
    }
  }

  /** Helper method to render JSON path segments with proper quoting. */
  private void renderJsonPathSegments(SqlGenerationContext ctx, String path) {
    String[] segments = path.split("\\.");
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        ctx.sql(".");
      }
      String segment = segments[i];
      if (!segment.isEmpty() && !Character.isLetter(segment.charAt(0))) {
        ctx.sql("\"");
        ctx.sql(segment);
        ctx.sql("\"");
      } else {
        ctx.sql(segment);
      }
    }
  }

  /**
   * Helper method to render Oracle dot notation path segments with proper quoting. Identifiers
   * starting with non-letter characters (like _id) must be quoted in Oracle's simplified dot
   * notation.
   */
  private void renderDotNotationSegments(SqlGenerationContext ctx, String path) {
    String[] segments = path.split("\\.");
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        ctx.sql(".");
      }
      String segment = segments[i];
      if (!segment.isEmpty() && !Character.isLetter(segment.charAt(0))) {
        ctx.sql("\"");
        ctx.sql(segment);
        ctx.sql("\"");
      } else {
        ctx.sql(segment);
      }
    }
  }

  /**
   * Checks if the path references a pipeline form $lookup. Returns the table alias if found.
   *
   * @param ctx the SQL generation context
   * @param normalizedPath the field path (without $ prefix)
   * @return the pipeline lookup table alias, or null if not a pipeline lookup
   */
  private String checkPipelineLookupPath(SqlGenerationContext ctx, String normalizedPath) {
    // Check for exact match first (e.g., "orders")
    String alias = ctx.getPipelineLookupAlias(normalizedPath);
    if (alias != null) {
      return alias;
    }
    // Check for nested path (e.g., "orders.amount")
    int dotIndex = normalizedPath.indexOf('.');
    if (dotIndex > 0) {
      String rootField = normalizedPath.substring(0, dotIndex);
      return ctx.getPipelineLookupAlias(rootField);
    }
    return null;
  }

  /**
   * Renders a field path that references a pipeline form $lookup result. Pipeline lookups produce a
   * JSON array column (via JSON_ARRAYAGG), so we access alias.columnName instead of alias.data.
   *
   * <p>Example: "$orders" -> sales_1.orders, "$orders.amount" -> sales_1.orders.amount
   */
  private void renderPipelineLookupFieldPath(
      SqlGenerationContext ctx, String tableAlias, String path) {
    // For pipeline lookups, the column name is the "as" field
    // path is like "orders" or "orders.amount"
    int dotIndex = path.indexOf('.');
    final String columnName = dotIndex >= 0 ? path.substring(0, dotIndex) : path;
    final String remainingPath = dotIndex >= 0 ? path.substring(dotIndex + 1) : "";

    // Build the reference: tableAlias.columnName[.remainingPath]
    StringBuilder expr = new StringBuilder();
    expr.append(tableAlias).append(".").append(columnName);
    if (!remainingPath.isEmpty()) {
      FieldNameValidator.validateFieldName(remainingPath);
      expr.append(".").append(remainingPath);
    }
    ctx.sql(expr.toString());
  }

  /**
   * Renders a field path that references an unwound array element. Redirects to the JSON_TABLE's
   * value column with the remaining path using Oracle dot notation.
   *
   * <p>Example: After "$unwind: $items", "$items.product" becomes: unwind_1.value.product
   */
  private void renderUnwindFieldPath(
      SqlGenerationContext ctx, SqlGenerationContext.UnwindInfo info) {
    if (returnType != null) {
      // Use JSON_VALUE with RETURNING for type conversion
      // CAST doesn't work with Oracle dot notation on JSON columns
      ctx.sql("JSON_VALUE(");
      ctx.sql(info.tableAlias());
      ctx.sql(".value, '$");
      if (!info.remainingPath().isEmpty()) {
        FieldNameValidator.validateFieldName(info.remainingPath());
        ctx.sql(".");
        renderJsonPathSegments(ctx, info.remainingPath());
      }
      ctx.sql("' RETURNING ");
      ctx.sql(returnType.getOracleSyntax());
      ctx.sql(")");
    } else {
      // Build dot notation expression
      StringBuilder dotExpr = new StringBuilder();
      dotExpr.append(info.tableAlias()).append(".value");
      if (!info.remainingPath().isEmpty()) {
        FieldNameValidator.validateFieldName(info.remainingPath());
        dotExpr.append(".").append(info.remainingPath());
      }
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
