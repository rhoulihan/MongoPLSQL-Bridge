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
import java.util.Objects;

/**
 * Represents an $addFields (or $set) stage that adds new fields to documents. In Oracle, this
 * translates to additional computed columns in the SELECT clause.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * { $addFields: { totalWithTax: { $add: ["$total", "$tax"] }, status: "processed" } }
 * // $set is an alias for $addFields
 * { $set: { totalWithTax: { $add: ["$total", "$tax"] } } }
 * </pre>
 *
 * <p>Note: Unlike $project, $addFields preserves all existing fields and just adds new ones. This
 * distinction is handled at the pipeline rendering level.
 */
public final class AddFieldsStage implements Stage {

  private final Map<String, Expression> fields;

  /**
   * Creates an addFields stage with the given field specifications.
   *
   * @param fields map of field names to their expression values
   */
  public AddFieldsStage(Map<String, Expression> fields) {
    Objects.requireNonNull(fields, "fields must not be null");
    this.fields = new LinkedHashMap<>(fields);
  }

  /** Returns the fields to add, as an unmodifiable map. */
  public Map<String, Expression> getFields() {
    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String getOperatorName() {
    return "$addFields";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Render as comma-separated computed columns
    // The full SELECT clause with data.* is handled by PipelineRenderer
    boolean first = true;
    for (Map.Entry<String, Expression> entry : fields.entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.visit(entry.getValue());
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
      first = false;
    }
  }

  @Override
  public String toString() {
    return "AddFieldsStage(fields=" + fields.keySet() + ")";
  }
}
