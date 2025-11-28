/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
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
    // Generate a unique alias for the joined table
    String alias = ctx.generateTableAlias(from);

    ctx.sql("LEFT OUTER JOIN ");
    ctx.sql(from);
    ctx.sql(" ");
    ctx.sql(alias);
    ctx.sql(" ON JSON_VALUE(");
    ctx.sql(ctx.getBaseTableAlias());
    ctx.sql(".data, '$.");
    ctx.sql(localField);
    ctx.sql("') = JSON_VALUE(");
    ctx.sql(alias);
    ctx.sql(".data, '$.");
    ctx.sql(foreignField);
    ctx.sql("')");
  }

  private void renderPipelineForm(SqlGenerationContext ctx) {
    // Pipeline form with let/pipeline is complex and requires correlated subquery support
    // For now, throw an exception with a clear message
    throw new UnsupportedOperatorException(
        "$lookup with let/pipeline (correlated subquery) is not yet fully supported. "
            + "Use the simple form with localField/foreignField instead.");
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
