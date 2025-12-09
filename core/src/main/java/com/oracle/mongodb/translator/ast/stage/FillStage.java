/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a $fill stage that fills null and missing values.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * {
 *   $fill: {
 *     partitionBy: "$state",
 *     sortBy: { "date": 1 },
 *     output: {
 *       "quantity": { method: "linear" },
 *       "price": { value: 0 }
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation uses:
 *
 * <ul>
 *   <li>LOCF (Last Observation Carried Forward): LAG with IGNORE NULLS
 *   <li>Linear interpolation: Complex calculation with previous/next non-null values
 *   <li>Constant value: COALESCE/NVL
 * </ul>
 */
public final class FillStage implements Stage {

  private final String partitionBy;
  private final Map<String, Integer> sortBy;
  private final Map<String, FillSpec> output;

  /**
   * Specification for how to fill a single field.
   *
   * @param method fill method ("locf" or "linear"), null if using constant value
   * @param value constant value to fill with, null if using method
   */
  public record FillSpec(String method, Object value) {}

  /**
   * Creates a fill stage.
   *
   * @param partitionBy optional partition expression
   * @param sortBy optional sort specification
   * @param output the output fill specifications (must not be null or empty)
   */
  @SuppressFBWarnings(
      value = {"EI_EXPOSE_REP2"},
      justification = "sortBy is copied defensively")
  public FillStage(String partitionBy, Map<String, Integer> sortBy, Map<String, FillSpec> output) {
    if (output == null || output.isEmpty()) {
      throw new IllegalArgumentException("output must not be null or empty");
    }
    this.partitionBy = partitionBy;
    this.sortBy = sortBy != null ? new LinkedHashMap<>(sortBy) : new LinkedHashMap<>();
    this.output = new LinkedHashMap<>(output);
  }

  /** Returns the partition expression, or null if not partitioned. */
  public String getPartitionBy() {
    return partitionBy;
  }

  /** Returns the sort specification as an unmodifiable map. */
  public Map<String, Integer> getSortBy() {
    return Collections.unmodifiableMap(sortBy);
  }

  /** Returns the output fill specifications as an unmodifiable map. */
  public Map<String, FillSpec> getOutput() {
    return Collections.unmodifiableMap(output);
  }

  @Override
  public String getOperatorName() {
    return "$fill";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    String alias = ctx.getBaseTableAlias();
    final String tablePrefix = (alias != null && !alias.isEmpty()) ? alias + "." : "";

    ctx.sql("SELECT ");

    // Render all fill expressions
    boolean first = true;
    for (Map.Entry<String, FillSpec> entry : output.entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      renderFillExpression(ctx, entry.getKey(), entry.getValue(), tablePrefix);
      first = false;
    }

    ctx.sql(" FROM ");
    if (tablePrefix.isEmpty()) {
      ctx.sql("base");
    } else {
      ctx.sql(tablePrefix.substring(0, tablePrefix.length() - 1));
    }
  }

  private void renderFillExpression(
      SqlGenerationContext ctx, String fieldName, FillSpec spec, String tablePrefix) {
    if (spec.value() != null) {
      // Constant value fill using COALESCE
      renderConstantFill(ctx, fieldName, spec.value(), tablePrefix);
    } else if ("locf".equals(spec.method())) {
      // Last Observation Carried Forward using LAG with IGNORE NULLS
      renderLocfFill(ctx, fieldName, tablePrefix);
    } else if ("linear".equals(spec.method())) {
      // Linear interpolation (simplified - full implementation would be more complex)
      renderLinearFill(ctx, fieldName, tablePrefix);
    } else {
      // Default: just pass through
      ctx.sql(tablePrefix);
      ctx.sql("data.\"");
      ctx.sql(fieldName);
      ctx.sql("\" AS \"");
      ctx.sql(fieldName);
      ctx.sql("\"");
    }
  }

  private void renderConstantFill(
      SqlGenerationContext ctx, String fieldName, Object value, String tablePrefix) {
    ctx.sql("COALESCE(");
    ctx.sql(tablePrefix);
    ctx.sql("data.\"");
    ctx.sql(fieldName);
    ctx.sql("\", ");
    if (value instanceof String s) {
      ctx.sql("'");
      ctx.sql(s);
      ctx.sql("'");
    } else {
      ctx.sql(String.valueOf(value));
    }
    ctx.sql(") AS \"");
    ctx.sql(fieldName);
    ctx.sql("\"");
  }

  private void renderLocfFill(SqlGenerationContext ctx, String fieldName, String tablePrefix) {
    // LOCF: use LAG with IGNORE NULLS to get last non-null value
    ctx.sql("COALESCE(");
    ctx.sql(tablePrefix);
    ctx.sql("data.\"");
    ctx.sql(fieldName);
    ctx.sql("\", LAG(");
    ctx.sql(tablePrefix);
    ctx.sql("data.\"");
    ctx.sql(fieldName);
    ctx.sql("\") IGNORE NULLS OVER (");
    renderOverClause(ctx, tablePrefix);
    ctx.sql(")) AS \"");
    ctx.sql(fieldName);
    ctx.sql("\"");
  }

  private void renderLinearFill(SqlGenerationContext ctx, String fieldName, String tablePrefix) {
    // Simplified linear interpolation - in practice this would need more complex logic
    // For now, just use COALESCE with the average of prev/next values
    ctx.sql("COALESCE(");
    ctx.sql(tablePrefix);
    ctx.sql("data.\"");
    ctx.sql(fieldName);
    ctx.sql("\", (LAG(");
    ctx.sql(tablePrefix);
    ctx.sql("data.\"");
    ctx.sql(fieldName);
    ctx.sql("\") IGNORE NULLS OVER (");
    renderOverClause(ctx, tablePrefix);
    ctx.sql(") + LEAD(");
    ctx.sql(tablePrefix);
    ctx.sql("data.\"");
    ctx.sql(fieldName);
    ctx.sql("\") IGNORE NULLS OVER (");
    renderOverClause(ctx, tablePrefix);
    ctx.sql(")) / 2) AS \"");
    ctx.sql(fieldName);
    ctx.sql("\"");
  }

  private void renderOverClause(SqlGenerationContext ctx, String tablePrefix) {
    boolean hasClause = false;

    // PARTITION BY clause
    if (partitionBy != null) {
      ctx.sql("PARTITION BY ");
      final String field = partitionBy.startsWith("$") ? partitionBy.substring(1) : partitionBy;
      ctx.sql(tablePrefix);
      ctx.sql("data.\"");
      ctx.sql(field);
      ctx.sql("\"");
      hasClause = true;
    }

    // ORDER BY clause
    if (!sortBy.isEmpty()) {
      if (hasClause) {
        ctx.sql(" ");
      }
      ctx.sql("ORDER BY ");
      boolean firstSort = true;
      for (Map.Entry<String, Integer> sortEntry : sortBy.entrySet()) {
        if (!firstSort) {
          ctx.sql(", ");
        }
        ctx.sql(tablePrefix);
        ctx.sql("data.\"");
        ctx.sql(sortEntry.getKey());
        ctx.sql("\"");
        if (sortEntry.getValue() < 0) {
          ctx.sql(" DESC");
        }
        firstSort = false;
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FillStage(");
    if (partitionBy != null) {
      sb.append("partitionBy=").append(partitionBy).append(", ");
    }
    if (!sortBy.isEmpty()) {
      sb.append("sortBy=").append(sortBy).append(", ");
    }
    sb.append("output=").append(output.keySet());
    sb.append(")");
    return sb.toString();
  }
}
