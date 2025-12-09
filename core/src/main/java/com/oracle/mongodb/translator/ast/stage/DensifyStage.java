/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.List;

/**
 * Represents a $densify stage that fills gaps in a sequence.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * {
 *   $densify: {
 *     field: "timestamp",
 *     partitionByFields: ["region"],
 *     range: {
 *       step: 1,
 *       unit: "hour",
 *       bounds: "full"
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation uses recursive CTE to generate missing values:
 *
 * <pre>
 * WITH RECURSIVE seq AS (
 *   SELECT MIN(timestamp) AS val FROM collection
 *   UNION ALL
 *   SELECT val + INTERVAL '1' HOUR FROM seq WHERE val < (SELECT MAX(timestamp) FROM collection)
 * )
 * SELECT ... FROM seq LEFT JOIN collection ON ...
 * </pre>
 */
public final class DensifyStage implements Stage {

  private final String field;
  private final List<String> partitionByFields;
  private final RangeSpec range;

  /**
   * Represents the range specification for densification.
   *
   * <p>Bounds is immutable via List.copyOf in the constructor when explicit bounds are provided.
   */
  @SuppressFBWarnings(
      value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"},
      justification = "explicitBounds is already immutable via List.copyOf")
  public record RangeSpec(int step, String unit, String bounds, List<Number> explicitBounds) {

    /**
     * Creates a RangeSpec with string bounds ("full" or "partition").
     *
     * @param step the step value (must be positive)
     * @param unit optional time unit (e.g., "hour", "day")
     * @param bounds bounds type ("full" or "partition")
     */
    public RangeSpec(int step, String unit, String bounds) {
      this(step, unit, bounds, null);
    }

    /**
     * Creates a RangeSpec with explicit numeric bounds.
     *
     * @param step the step value (must be positive)
     * @param unit optional time unit (e.g., "hour", "day")
     * @param explicitBounds explicit [lowerBound, upperBound] list
     */
    public RangeSpec(int step, String unit, List<Number> explicitBounds) {
      this(step, unit, null, explicitBounds != null ? List.copyOf(explicitBounds) : null);
    }

    /** Canonical constructor with validation. */
    public RangeSpec {
      if (step <= 0) {
        throw new IllegalArgumentException("step must be positive, got: " + step);
      }
    }
  }

  /**
   * Creates a densify stage.
   *
   * @param field the field to densify (must not be null)
   * @param partitionByFields optional list of partition fields
   * @param range the range specification (must not be null)
   */
  public DensifyStage(String field, List<String> partitionByFields, RangeSpec range) {
    if (field == null || field.isEmpty()) {
      throw new IllegalArgumentException("field must not be null or empty");
    }
    if (range == null) {
      throw new IllegalArgumentException("range must not be null");
    }
    this.field = field;
    this.partitionByFields =
        partitionByFields != null ? List.copyOf(partitionByFields) : Collections.emptyList();
    this.range = range;
  }

  /** Returns the field to densify. */
  public String getField() {
    return field;
  }

  /** Returns the partition fields as an unmodifiable list. */
  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "partitionByFields is already immutable via List.copyOf in constructor")
  public List<String> getPartitionByFields() {
    return partitionByFields;
  }

  /** Returns the range specification. */
  public RangeSpec getRange() {
    return range;
  }

  @Override
  public String getOperatorName() {
    return "$densify";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Generate a recursive CTE to fill gaps in the sequence
    // For numeric fields, uses simple arithmetic
    // For date fields, uses INTERVAL arithmetic

    String alias = ctx.getBaseTableAlias();
    final String tablePrefix = (alias != null && !alias.isEmpty()) ? alias + "." : "";

    ctx.sql("SELECT ");

    // Include partition fields in the output
    for (String partField : partitionByFields) {
      ctx.sql("COALESCE(base_data.");
      ctx.sql(partField);
      ctx.sql(", seq_data.");
      ctx.sql(partField);
      ctx.sql(") AS ");
      ctx.sql(partField);
      ctx.sql(", ");
    }

    // The densified field
    ctx.sql("COALESCE(base_data.\"");
    ctx.sql(field);
    ctx.sql("\", seq_data.\"");
    ctx.sql(field);
    ctx.sql("\") AS \"");
    ctx.sql(field);
    ctx.sql("\" FROM (");

    // Generate sequence using recursive CTE
    renderSequenceGeneration(ctx, tablePrefix);

    ctx.sql(") seq_data LEFT JOIN (SELECT ");
    ctx.sql(tablePrefix);
    ctx.sql("data.* FROM ");
    ctx.sql(tablePrefix.isEmpty() ? "base" : alias);
    ctx.sql(") base_data ON seq_data.\"");
    ctx.sql(field);
    ctx.sql("\" = base_data.\"");
    ctx.sql(field);
    ctx.sql("\"");

    // Add partition join conditions
    for (String partField : partitionByFields) {
      ctx.sql(" AND seq_data.");
      ctx.sql(partField);
      ctx.sql(" = base_data.");
      ctx.sql(partField);
    }
  }

  private void renderSequenceGeneration(SqlGenerationContext ctx, String tablePrefix) {
    // Use a recursive CTE to generate sequence values
    ctx.sql("WITH RECURSIVE densify_seq (\"");
    ctx.sql(field);
    ctx.sql("\"");

    // Include partition fields
    for (String partField : partitionByFields) {
      ctx.sql(", ");
      ctx.sql(partField);
    }

    ctx.sql(") AS (");

    // Base case - start from minimum value
    ctx.sql("SELECT MIN(");
    ctx.sql(tablePrefix);
    ctx.sql("data.\"");
    ctx.sql(field);
    ctx.sql("\")");

    for (String partField : partitionByFields) {
      ctx.sql(", ");
      ctx.sql(tablePrefix);
      ctx.sql("data.");
      ctx.sql(partField);
    }

    ctx.sql(" FROM ");
    ctx.sql(tablePrefix.isEmpty() ? "base" : tablePrefix.substring(0, tablePrefix.length() - 1));

    if (!partitionByFields.isEmpty()) {
      ctx.sql(" GROUP BY ");
      boolean first = true;
      for (String partField : partitionByFields) {
        if (!first) {
          ctx.sql(", ");
        }
        ctx.sql(tablePrefix);
        ctx.sql("data.");
        ctx.sql(partField);
        first = false;
      }
    }

    // Recursive case - increment by step
    ctx.sql(" UNION ALL SELECT ");

    if (range.unit() != null) {
      // Date field - use interval arithmetic
      ctx.sql("\"");
      ctx.sql(field);
      ctx.sql("\" + INTERVAL '");
      ctx.sql(String.valueOf(range.step()));
      ctx.sql("' ");
      ctx.sql(range.unit().toUpperCase());
    } else {
      // Numeric field - simple addition
      ctx.sql("\"");
      ctx.sql(field);
      ctx.sql("\" + ");
      ctx.sql(String.valueOf(range.step()));
    }

    for (String partField : partitionByFields) {
      ctx.sql(", ");
      ctx.sql(partField);
    }

    ctx.sql(" FROM densify_seq WHERE \"");
    ctx.sql(field);
    ctx.sql("\" < (SELECT MAX(");
    ctx.sql(tablePrefix);
    ctx.sql("data.\"");
    ctx.sql(field);
    ctx.sql("\") FROM ");
    ctx.sql(tablePrefix.isEmpty() ? "base" : tablePrefix.substring(0, tablePrefix.length() - 1));
    ctx.sql(")) SELECT * FROM densify_seq");
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("DensifyStage(");
    sb.append("field=").append(field);
    if (!partitionByFields.isEmpty()) {
      sb.append(", partitionByFields=").append(partitionByFields);
    }
    sb.append(", range={step=").append(range.step());
    if (range.unit() != null) {
      sb.append(", unit=").append(range.unit());
    }
    if (range.bounds() != null) {
      sb.append(", bounds=").append(range.bounds());
    }
    sb.append("})");
    return sb.toString();
  }
}
