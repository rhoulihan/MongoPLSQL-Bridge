/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a $bucketAuto stage that automatically determines bucket boundaries to evenly
 * distribute documents.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * {
 *   $bucketAuto: {
 *     groupBy: &lt;expression&gt;,
 *     buckets: &lt;number&gt;,
 *     output: {
 *       &lt;field1&gt;: { &lt;accumulator expression&gt; },
 *       ...
 *     },
 *     granularity: &lt;string&gt;  // optional: "R5", "R10", "R20", etc.
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation uses NTILE window function:
 *
 * <pre>
 * SELECT bucket_id AS _id, aggregations...
 * FROM (
 *   SELECT NTILE(buckets) OVER (ORDER BY expr) AS bucket_id, ...
 *   FROM collection
 * )
 * GROUP BY bucket_id
 * </pre>
 *
 * <p>Note: The granularity parameter is not fully supported in Oracle translation.
 */
public final class BucketAutoStage implements Stage {

  private final Expression groupBy;
  private final int buckets;
  private final Map<String, AccumulatorExpression> output;
  private final String granularity;

  /**
   * Creates a bucket auto stage.
   *
   * @param groupBy the expression to group by
   * @param buckets the number of buckets to create (must be positive)
   * @param output optional output accumulators (may be empty)
   * @param granularity optional granularity string (may be null)
   */
  public BucketAutoStage(
      Expression groupBy,
      int buckets,
      Map<String, AccumulatorExpression> output,
      String granularity) {
    this.groupBy = Objects.requireNonNull(groupBy, "groupBy must not be null");
    if (buckets <= 0) {
      throw new IllegalArgumentException("buckets must be positive, got: " + buckets);
    }
    this.buckets = buckets;
    this.output = output != null ? new LinkedHashMap<>(output) : new LinkedHashMap<>();
    this.granularity = granularity;
  }

  public Expression getGroupBy() {
    return groupBy;
  }

  public int getBuckets() {
    return buckets;
  }

  /** Returns the output accumulators as an unmodifiable map. */
  public Map<String, AccumulatorExpression> getOutput() {
    return Collections.unmodifiableMap(output);
  }

  public String getGranularity() {
    return granularity;
  }

  public boolean hasGranularity() {
    return granularity != null && !granularity.isEmpty();
  }

  @Override
  public String getOperatorName() {
    return "$bucketAuto";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // $bucketAuto requires special handling in PipelineRenderer
    // because it needs to create a subquery with NTILE.
    // This render method provides basic output for debugging.
    ctx.sql("NTILE(");
    ctx.sql(String.valueOf(buckets));
    ctx.sql(") OVER (ORDER BY ");
    ctx.visit(groupBy);
    ctx.sql(") AS ");
    ctx.identifier("_id");

    // Render output accumulators
    for (Map.Entry<String, AccumulatorExpression> entry : output.entrySet()) {
      ctx.sql(", ");
      ctx.visit(entry.getValue());
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
    }

    if (hasGranularity()) {
      ctx.sql(" /* granularity '");
      ctx.sql(granularity);
      ctx.sql("' not supported */");
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("BucketAutoStage(groupBy=");
    sb.append(groupBy);
    sb.append(", buckets=").append(buckets);
    if (!output.isEmpty()) {
      sb.append(", output=").append(output);
    }
    if (hasGranularity()) {
      sb.append(", granularity=").append(granularity);
    }
    sb.append(")");
    return sb.toString();
  }
}
