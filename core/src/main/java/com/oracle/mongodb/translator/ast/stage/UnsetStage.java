/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an $unset stage that removes specified fields from documents. This is essentially the
 * inverse of $project with field exclusion.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$unset: "fieldToRemove"}} - removes a single field
 *   <li>{@code {$unset: ["field1", "field2"]}} - removes multiple fields
 *   <li>{@code {$unset: "parent.child"}} - removes a nested field
 * </ul>
 *
 * <p>Translates to Oracle's JSON_TRANSFORM with REMOVE operations.
 */
public final class UnsetStage implements Stage {

  private final List<String> fields;

  /**
   * Creates an $unset stage.
   *
   * @param fields the fields to remove from documents
   */
  public UnsetStage(List<String> fields) {
    Objects.requireNonNull(fields, "fields must not be null");
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("$unset requires at least one field");
    }
    this.fields = new ArrayList<>(fields);
  }

  /** Returns the list of fields to unset. */
  public List<String> getFields() {
    return Collections.unmodifiableList(fields);
  }

  @Override
  public String getOperatorName() {
    return "$unset";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Use JSON_TRANSFORM to remove fields from the document
    // JSON_TRANSFORM(data, REMOVE '$.field1', REMOVE '$.field2')
    ctx.sql("SELECT JSON_TRANSFORM(");

    String alias = ctx.getBaseTableAlias();
    if (alias != null && !alias.isEmpty()) {
      ctx.sql(alias);
      ctx.sql(".");
    }
    ctx.sql("data");

    // Add REMOVE operations for each field
    for (String field : fields) {
      ctx.sql(", REMOVE '$.");
      ctx.sql(field);
      ctx.sql("'");
    }

    ctx.sql(") AS data");
  }

  @Override
  public String toString() {
    return "UnsetStage(" + fields + ")";
  }
}
