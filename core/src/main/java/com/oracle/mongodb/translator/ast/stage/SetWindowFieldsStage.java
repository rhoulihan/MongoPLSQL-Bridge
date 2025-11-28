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
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a $setWindowFields stage that performs window function calculations.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * {
 *   $setWindowFields: {
 *     partitionBy: "$state",
 *     sortBy: { "orderDate": 1 },
 *     output: {
 *       cumulativeQuantity: {
 *         $sum: "$quantity",
 *         window: {
 *           documents: ["unbounded", "current"]
 *         }
 *       },
 *       rank: { $rank: {} }
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation uses window functions:
 *
 * <pre>
 * SELECT ...,
 *   SUM(JSON_VALUE(data, '$.quantity' RETURNING NUMBER)) OVER (
 *     PARTITION BY JSON_VALUE(data, '$.state')
 *     ORDER BY JSON_VALUE(data, '$.orderDate')
 *     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *   ) AS cumulativeQuantity,
 *   RANK() OVER (PARTITION BY JSON_VALUE(data, '$.state')
 *     ORDER BY JSON_VALUE(data, '$.orderDate')) AS rank
 * FROM ...
 * </pre>
 */
public final class SetWindowFieldsStage implements Stage {

  private final String partitionBy;
  private final Map<String, Integer> sortBy;
  private final Map<String, WindowField> output;

  /** Represents a single output window field. */
  public record WindowField(String operator, String argument, WindowSpec window) {}

  /**
   * Represents window specification with immutable bounds.
   *
   * <p>The bounds list is immutable via List.copyOf in the constructor.
   */
  @SuppressFBWarnings(
      value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
      justification = "bounds is already immutable via List.copyOf")
  public record WindowSpec(String type, List<String> bounds) {
    /** Creates a WindowSpec with defensive copy of bounds. */
    public WindowSpec(String type, List<String> bounds) {
      this.type = type;
      this.bounds = bounds != null ? List.copyOf(bounds) : Collections.emptyList();
    }
  }

  /**
   * Creates a set window fields stage.
   *
   * @param partitionBy optional partition expression
   * @param sortBy optional sort specification
   * @param output the output window fields
   */
  public SetWindowFieldsStage(
      String partitionBy, Map<String, Integer> sortBy, Map<String, WindowField> output) {
    this.partitionBy = partitionBy;
    this.sortBy = sortBy != null ? new LinkedHashMap<>(sortBy) : new LinkedHashMap<>();
    this.output = Objects.requireNonNull(output, "output must not be null");
  }

  public String getPartitionBy() {
    return partitionBy;
  }

  /** Returns the sort specification as an unmodifiable map. */
  public Map<String, Integer> getSortBy() {
    return Collections.unmodifiableMap(sortBy);
  }

  /** Returns the output window fields as an unmodifiable map. */
  public Map<String, WindowField> getOutput() {
    return Collections.unmodifiableMap(output);
  }

  @Override
  public String getOperatorName() {
    return "$setWindowFields";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Render each window function as a computed column
    boolean first = true;
    for (Map.Entry<String, WindowField> entry : output.entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      renderWindowFunction(ctx, entry.getKey(), entry.getValue());
      first = false;
    }
  }

  private void renderWindowFunction(SqlGenerationContext ctx, String alias, WindowField field) {
    String op = field.operator();

    // Map MongoDB window operators to Oracle
    switch (op) {
      case "$sum" -> {
        ctx.sql("SUM(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(")");
      }
      case "$avg" -> {
        ctx.sql("AVG(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(")");
      }
      case "$min" -> {
        ctx.sql("MIN(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(")");
      }
      case "$max" -> {
        ctx.sql("MAX(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(")");
      }
      case "$count" -> ctx.sql("COUNT(*)");
      case "$rank" -> ctx.sql("RANK()");
      case "$denseRank" -> ctx.sql("DENSE_RANK()");
      case "$rowNumber", "$documentNumber" -> ctx.sql("ROW_NUMBER()");
      case "$first" -> {
        ctx.sql("FIRST_VALUE(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(")");
      }
      case "$last" -> {
        ctx.sql("LAST_VALUE(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(")");
      }
      case "$shift" -> {
        // $shift is LAG or LEAD depending on the by value
        ctx.sql("LAG(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(", 1)");
      }
      case "$stdDevPop" -> {
        ctx.sql("STDDEV_POP(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(")");
      }
      case "$stdDevSamp" -> {
        ctx.sql("STDDEV_SAMP(");
        renderFieldPath(ctx, field.argument());
        ctx.sql(")");
      }
      default -> {
        // Unknown operator - render as comment
        ctx.sql("/* unsupported: ");
        ctx.sql(op);
        ctx.sql(" */ NULL");
      }
    }

    // Add OVER clause
    ctx.sql(" OVER (");
    renderOverClause(ctx, field.window());
    ctx.sql(") AS ");
    ctx.sql(alias);
  }

  private void renderFieldPath(SqlGenerationContext ctx, String fieldPath) {
    if (fieldPath == null) {
      ctx.sql("1");
      return;
    }

    String field = fieldPath.startsWith("$") ? fieldPath.substring(1) : fieldPath;
    ctx.sql("JSON_VALUE(data, '$.");
    ctx.sql(field);
    ctx.sql("' RETURNING NUMBER)");
  }

  private void renderOverClause(SqlGenerationContext ctx, WindowSpec window) {
    boolean hasClause = false;

    // PARTITION BY clause
    if (partitionBy != null) {
      ctx.sql("PARTITION BY ");
      String partField = partitionBy.startsWith("$") ? partitionBy.substring(1) : partitionBy;
      ctx.sql("JSON_VALUE(data, '$.");
      ctx.sql(partField);
      ctx.sql("')");
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
        String sortField = sortEntry.getKey();
        ctx.sql("JSON_VALUE(data, '$.");
        ctx.sql(sortField);
        ctx.sql("')");
        if (sortEntry.getValue() < 0) {
          ctx.sql(" DESC");
        }
        firstSort = false;
      }
      hasClause = true;
    }

    // Window frame clause
    if (window != null && window.bounds() != null && !window.bounds().isEmpty()) {
      if (hasClause) {
        ctx.sql(" ");
      }
      renderWindowFrame(ctx, window);
    }
  }

  private void renderWindowFrame(SqlGenerationContext ctx, WindowSpec window) {
    String type = window.type();
    List<String> bounds = window.bounds();

    // Determine frame type (ROWS or RANGE)
    if ("documents".equals(type)) {
      ctx.sql("ROWS BETWEEN ");
    } else if ("range".equals(type)) {
      ctx.sql("RANGE BETWEEN ");
    } else {
      // Default to ROWS
      ctx.sql("ROWS BETWEEN ");
    }

    // Lower bound
    if (bounds.size() >= 1) {
      renderBound(ctx, bounds.get(0));
    } else {
      ctx.sql("UNBOUNDED PRECEDING");
    }

    ctx.sql(" AND ");

    // Upper bound
    if (bounds.size() >= 2) {
      renderBound(ctx, bounds.get(1));
    } else {
      ctx.sql("CURRENT ROW");
    }
  }

  private void renderBound(SqlGenerationContext ctx, String bound) {
    if (bound == null || "unbounded".equals(bound)) {
      ctx.sql("UNBOUNDED PRECEDING");
    } else if ("current".equals(bound)) {
      ctx.sql("CURRENT ROW");
    } else {
      try {
        int value = Integer.parseInt(bound);
        if (value < 0) {
          ctx.sql(String.valueOf(-value));
          ctx.sql(" PRECEDING");
        } else if (value > 0) {
          ctx.sql(String.valueOf(value));
          ctx.sql(" FOLLOWING");
        } else {
          ctx.sql("CURRENT ROW");
        }
      } catch (NumberFormatException e) {
        // Unknown bound - treat as unbounded
        ctx.sql("UNBOUNDED PRECEDING");
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SetWindowFieldsStage(");
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
