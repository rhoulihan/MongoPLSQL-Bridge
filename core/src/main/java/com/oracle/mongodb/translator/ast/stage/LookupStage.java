/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import com.oracle.mongodb.translator.util.FieldNameValidator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a $lookup stage that performs a left outer join to another collection.
 *
 * <p>Supports two forms:
 *
 * <p>1. Simple equality match - translates to LEFT OUTER JOIN:
 *
 * <pre>
 * {
 *   $lookup: {
 *     from: "inventory",
 *     localField: "item",
 *     foreignField: "sku",
 *     as: "inventory_docs"
 *   }
 * }
 * </pre>
 *
 * <p>2. Pipeline with let variables - translates to LATERAL join or correlated subquery:
 *
 * <pre>
 * {
 *   $lookup: {
 *     from: "reviews",
 *     let: { productId: "$productId" },
 *     pipeline: [
 *       { $match: { $expr: { $eq: ["$productId", "$$productId"] } } }
 *     ],
 *     as: "reviews"
 *   }
 * }
 * </pre>
 */
public final class LookupStage implements Stage {

  private final String from;
  private final String localField; // null for pipeline form
  private final String foreignField; // null for pipeline form
  private final String as;
  private final Map<String, String> letVariables; // variable name -> field path
  private final List<Stage> pipeline; // stages in the pipeline

  /** Creates a simple equality lookup stage. */
  private LookupStage(String from, String localField, String foreignField, String as) {
    this.from = Objects.requireNonNull(from, "from must not be null");
    this.localField = Objects.requireNonNull(localField, "localField must not be null");
    this.foreignField = Objects.requireNonNull(foreignField, "foreignField must not be null");
    this.as = Objects.requireNonNull(as, "as must not be null");
    this.letVariables = Collections.emptyMap();
    this.pipeline = Collections.emptyList();
  }

  /** Creates a pipeline lookup stage. */
  private LookupStage(
      String from, Map<String, String> letVariables, List<Stage> pipeline, String as) {
    this.from = Objects.requireNonNull(from, "from must not be null");
    this.localField = null;
    this.foreignField = null;
    this.as = Objects.requireNonNull(as, "as must not be null");
    this.letVariables = letVariables != null ? Map.copyOf(letVariables) : Collections.emptyMap();
    this.pipeline = pipeline != null ? List.copyOf(pipeline) : Collections.emptyList();
  }

  /** Creates a simple equality lookup stage. */
  public static LookupStage equality(
      String from, String localField, String foreignField, String as) {
    return new LookupStage(from, localField, foreignField, as);
  }

  /** Creates a pipeline lookup stage with let variables. */
  public static LookupStage withPipeline(
      String from, Map<String, String> letVariables, List<Stage> pipeline, String as) {
    return new LookupStage(from, letVariables, pipeline, as);
  }

  /** Returns true if this is a pipeline form lookup. */
  public boolean isPipelineForm() {
    return localField == null;
  }

  /** Returns the foreign collection name. */
  public String getFrom() {
    return from;
  }

  /** Returns the local field path (null for pipeline form). */
  public String getLocalField() {
    return localField;
  }

  /** Returns the foreign field path (null for pipeline form). */
  public String getForeignField() {
    return foreignField;
  }

  /** Returns the output array field name. */
  public String getAs() {
    return as;
  }

  /**
   * Returns the let variables (empty for equality form). The returned map is immutable.
   *
   * @return unmodifiable map of let variables
   */
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "letVariables is already immutable via Map.copyOf")
  public Map<String, String> getLetVariables() {
    return letVariables;
  }

  /**
   * Returns the pipeline stages (empty for equality form). The returned list is immutable.
   *
   * @return unmodifiable list of pipeline stages
   */
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "pipeline is already immutable via List.copyOf")
  public List<Stage> getPipeline() {
    return pipeline;
  }

  @Override
  public String getOperatorName() {
    return "$lookup";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    if (isPipelineForm()) {
      renderPipelineForm(ctx);
    } else {
      renderEqualityForm(ctx);
    }
  }

  private void renderEqualityForm(SqlGenerationContext ctx) {
    // Validate table name and field names to prevent injection
    FieldNameValidator.validateTableName(from);
    final String validLocalField = FieldNameValidator.validateAndNormalizeFieldPath(localField);
    final String validForeignField = FieldNameValidator.validateAndNormalizeFieldPath(foreignField);

    // Use pre-registered alias if available, otherwise generate a new one
    String alias = ctx.getLookupTableAliasByAs(as);
    if (alias == null) {
      alias = ctx.generateTableAlias(from);
      // Register this alias so field paths like "$customer.tier" can resolve to this table
      ctx.registerLookupTableAlias(as, alias);
    }

    ctx.sql("LEFT OUTER JOIN ");
    ctx.tableName(from);
    ctx.sql(" ");
    ctx.sql(alias);
    ctx.sql(" ON JSON_VALUE(");
    ctx.sql(ctx.getBaseTableAlias());
    ctx.sql(".data, '$.");
    ctx.sql(validLocalField);
    ctx.sql("') = JSON_VALUE(");
    ctx.sql(alias);
    ctx.sql(".data, '$.");
    ctx.sql(validForeignField);
    ctx.sql("')");
  }

  private void renderPipelineForm(SqlGenerationContext ctx) {
    // Pipeline form with let/pipeline uses LATERAL join with correlated subquery
    // Result pattern:
    // , LATERAL (SELECT JSON_ARRAYAGG(f.data) FROM collection f WHERE ...) lookup_alias

    // Validate table name
    FieldNameValidator.validateTableName(from);

    // Generate alias for the LATERAL subquery
    String alias = ctx.generateTableAlias(from);
    ctx.registerLookupTableAlias(as, alias);

    // Start LATERAL join - use comma join since LATERAL works as correlated subquery
    ctx.sql(", LATERAL (SELECT JSON_ARRAYAGG(");
    ctx.sql(alias);
    ctx.sql("_inner.data");

    // Add ORDER BY inside JSON_ARRAYAGG if we have a sort stage in the pipeline
    SortStage sortStage = findSortStage();
    if (sortStage != null) {
      ctx.sql(" ORDER BY ");
      renderSortFields(sortStage, alias + "_inner", ctx);
    }

    ctx.sql(") AS ");
    ctx.sql(as);
    ctx.sql(" FROM ");
    ctx.tableName(from);
    ctx.sql(" ");
    ctx.sql(alias);
    ctx.sql("_inner");

    // Render WHERE clause from pipeline $match stages with let variable substitution
    boolean hasWhere = false;

    // Process each match stage in the pipeline
    for (Stage stage : pipeline) {
      if (stage instanceof MatchStage matchStage) {
        if (!hasWhere) {
          ctx.sql(" WHERE ");
          hasWhere = true;
        } else {
          ctx.sql(" AND ");
        }
        // Render the match expression with variable substitution
        renderExpressionWithVarSubstitution(
            matchStage.getFilter(), alias + "_inner", ctx);
      }
    }

    // Add FETCH FIRST for $limit stage in pipeline
    LimitStage limitStage = findLimitStage();
    if (limitStage != null) {
      ctx.sql(" FETCH FIRST ");
      ctx.sql(String.valueOf(limitStage.getLimit()));
      ctx.sql(" ROWS ONLY");
    }

    ctx.sql(") ");
    ctx.sql(alias);
  }

  /** Finds the $sort stage in the pipeline, if present. */
  private SortStage findSortStage() {
    for (Stage stage : pipeline) {
      if (stage instanceof SortStage) {
        return (SortStage) stage;
      }
    }
    return null;
  }

  /** Finds the $limit stage in the pipeline, if present. */
  private LimitStage findLimitStage() {
    for (Stage stage : pipeline) {
      if (stage instanceof LimitStage) {
        return (LimitStage) stage;
      }
    }
    return null;
  }

  /** Renders sort fields for ORDER BY inside JSON_ARRAYAGG. */
  private void renderSortFields(SortStage sortStage, String tableAlias, SqlGenerationContext ctx) {
    var sortFields = sortStage.getSortFields();
    for (int i = 0; i < sortFields.size(); i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      var sortField = sortFields.get(i);
      ctx.sql("JSON_VALUE(");
      ctx.sql(tableAlias);
      ctx.sql(".data, '$.");
      // Get field name directly from the FieldPathExpression
      String fieldName = sortField.getFieldPath().getPath();
      ctx.sql(fieldName);
      ctx.sql("')");
      if (sortField.getDirection() == SortStage.SortDirection.DESC) {
        ctx.sql(" DESC");
      }
    }
  }

  /**
   * Renders an expression with variable substitution for $$varName references.
   *
   * <p>This method recursively handles:
   *
   * <ul>
   *   <li>LogicalExpression (AND/OR) - renders each operand with proper operators
   *   <li>ComparisonExpression - renders with inner/outer table references
   * </ul>
   */
  private void renderExpressionWithVarSubstitution(
      Expression expr, String innerAlias, SqlGenerationContext ctx) {
    if (expr instanceof LogicalExpression logical) {
      // Render each operand with the logical operator between them
      List<Expression> operands = logical.getOperands();
      String sqlOperator = logical.getOp() == LogicalOp.AND ? " AND " : " OR ";

      ctx.sql("(");
      for (int i = 0; i < operands.size(); i++) {
        if (i > 0) {
          ctx.sql(sqlOperator);
        }
        renderExpressionWithVarSubstitution(operands.get(i), innerAlias, ctx);
      }
      ctx.sql(")");
    } else if (expr instanceof ComparisonExpression comp) {
      renderComparisonWithVarSubstitution(comp, innerAlias, ctx);
    } else {
      // Fallback for other expressions - visit directly
      // This may not work for all cases but handles simple expressions
      ctx.visit(expr);
    }
  }

  /** Renders a comparison expression with variable substitution. */
  private void renderComparisonWithVarSubstitution(
      ComparisonExpression comp, String innerAlias, SqlGenerationContext ctx) {
    Expression left = comp.getLeft();

    // Left side is typically the inner table field reference
    renderFieldReference(left, innerAlias, ctx);

    // Render comparison operator
    ctx.sql(" ");
    ctx.sql(getSqlOperator(comp.getOp()));
    ctx.sql(" ");

    // Right side may be a $$variable reference or a regular value
    // Parser converts $$varName to $varName, so check for $prefix and lookup in letVariables
    final Expression right = comp.getRight();
    if (right instanceof FieldPathExpression rightField) {
      String path = rightField.getPath();
      if (path.startsWith("$")) {
        String potentialVarName = path.substring(1); // Remove $ to get potential variable name
        String outerFieldPath = letVariables.get(potentialVarName);
        if (outerFieldPath != null) {
          // Variable reference - substitute with outer table field
          String outerField =
              outerFieldPath.startsWith("$") ? outerFieldPath.substring(1) : outerFieldPath;
          ctx.sql("JSON_VALUE(");
          ctx.sql(ctx.getBaseTableAlias());
          ctx.sql(".data, '$.");
          ctx.sql(outerField);
          ctx.sql("')");
        } else {
          // Not a variable, treat as inner table field reference
          ctx.sql("JSON_VALUE(");
          ctx.sql(innerAlias);
          ctx.sql(".data, '$.");
          ctx.sql(potentialVarName);
          ctx.sql("')");
        }
      } else {
        // Regular field reference without prefix
        renderFieldReference(right, innerAlias, ctx);
      }
    } else {
      // Non-field expression (literal, etc.)
      ctx.visit(right);
    }
  }

  /** Renders a field reference expression. */
  private void renderFieldReference(Expression expr, String tableAlias, SqlGenerationContext ctx) {
    if (expr instanceof FieldPathExpression fieldExpr) {
      String path = fieldExpr.getPath();
      // Remove $ prefix if present for inner table fields
      if (path.startsWith("$") && !path.startsWith("$$")) {
        path = path.substring(1);
      }
      ctx.sql("JSON_VALUE(");
      ctx.sql(tableAlias);
      ctx.sql(".data, '$.");
      ctx.sql(path);
      ctx.sql("')");
    } else {
      ctx.visit(expr);
    }
  }

  /** Returns the SQL operator for a comparison operator. */
  private String getSqlOperator(ComparisonOp op) {
    return switch (op) {
      case EQ -> "=";
      case NE -> "<>";
      case GT -> ">";
      case GTE -> ">=";
      case LT -> "<";
      case LTE -> "<=";
      case IN -> "IN";
      case NIN -> "NOT IN";
    };
  }

  @Override
  public String toString() {
    if (isPipelineForm()) {
      return "LookupStage(from="
          + from
          + ", let="
          + letVariables
          + ", pipeline="
          + pipeline.size()
          + " stages"
          + ", as="
          + as
          + ")";
    }
    return "LookupStage(from="
        + from
        + ", localField="
        + localField
        + ", foreignField="
        + foreignField
        + ", as="
        + as
        + ")";
  }
}
