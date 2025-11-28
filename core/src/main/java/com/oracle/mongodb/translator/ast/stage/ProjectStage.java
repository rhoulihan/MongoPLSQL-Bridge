/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a $project stage that shapes the output documents. Translates to Oracle's SELECT
 * clause with JSON_VALUE/JSON_QUERY for field extraction.
 *
 * <p>Supports:
 *
 * <ul>
 *   <li>Field inclusion: { field: 1 } or { field: true }
 *   <li>Field exclusion: { field: 0 } or { field: false }
 *   <li>Field rename/expression: { newName: "$existingField" }
 *   <li>Computed fields: { newField: { $add: [...] } }
 * </ul>
 */
public final class ProjectStage implements Stage {

  private final Map<String, ProjectionField> projections;
  private final boolean isExclusionMode;

  /**
   * Creates a project stage with the given projections.
   *
   * @param projections map of field names to projection definitions
   * @param isExclusionMode true if this is an exclusion-mode projection (only _id: 0 allowed)
   */
  public ProjectStage(Map<String, ProjectionField> projections, boolean isExclusionMode) {
    this.projections =
        projections != null ? new LinkedHashMap<>(projections) : new LinkedHashMap<>();
    this.isExclusionMode = isExclusionMode;
  }

  /** Creates an inclusion-mode project stage. */
  public ProjectStage(Map<String, ProjectionField> projections) {
    this(projections, false);
  }

  /** Returns the projections as an unmodifiable map. */
  public Map<String, ProjectionField> getProjections() {
    return Collections.unmodifiableMap(projections);
  }

  /** Returns true if this is an exclusion-mode projection. */
  public boolean isExclusionMode() {
    return isExclusionMode;
  }

  @Override
  public String getOperatorName() {
    return "$project";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    ctx.sql("SELECT ");

    boolean first = true;
    for (Map.Entry<String, ProjectionField> entry : projections.entrySet()) {
      final String alias = entry.getKey();
      ProjectionField field = entry.getValue();

      if (field.isExcluded()) {
        continue; // Skip excluded fields
      }

      if (!first) {
        ctx.sql(", ");
      }

      if (field.getExpression() != null) {
        ctx.visit(field.getExpression());
      }
      ctx.sql(" AS ");
      ctx.sql(alias);
      first = false;
    }

    // If no fields were included, select nothing (edge case)
    if (first) {
      ctx.sql("NULL AS dummy");
    }
  }

  @Override
  public String toString() {
    return "ProjectStage(" + projections.keySet() + ")";
  }

  /** Represents a single field in a projection. */
  public static final class ProjectionField {
    private final Expression expression;
    private final boolean excluded;

    private ProjectionField(Expression expression, boolean excluded) {
      this.expression = expression;
      this.excluded = excluded;
    }

    /** Creates an included field with the given expression. */
    public static ProjectionField include(Expression expression) {
      return new ProjectionField(expression, false);
    }

    /** Creates an excluded field marker. */
    public static ProjectionField exclude() {
      return new ProjectionField(null, true);
    }

    /** Returns the expression for this field, or null if excluded. */
    public Expression getExpression() {
      return expression;
    }

    /** Returns true if this field is excluded. */
    public boolean isExcluded() {
      return excluded;
    }

    @Override
    public String toString() {
      if (excluded) {
        return "EXCLUDED";
      }
      return expression.toString();
    }
  }
}
