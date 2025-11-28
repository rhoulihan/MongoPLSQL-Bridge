/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents an $unwind stage that deconstructs an array field into multiple documents. Translates
 * to Oracle's JSON_TABLE with NESTED PATH.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * { $unwind: "$items" }
 * // or with options:
 * { $unwind: { path: "$items", includeArrayIndex: "index", preserveNullAndEmptyArrays: true } }
 * </pre>
 *
 * <p>Oracle translation:
 *
 * <pre>
 * JSON_TABLE(data, '$.items[*]' COLUMNS (
 *     item_value JSON PATH '$'
 * )) unwind_1
 * </pre>
 */
public final class UnwindStage implements Stage {

  private final String path;
  private final String includeArrayIndex;
  private final boolean preserveNullAndEmptyArrays;

  /**
   * Creates an unwind stage with just the path.
   *
   * @param path the array field path (without the $ prefix)
   */
  public UnwindStage(String path) {
    this(path, null, false);
  }

  /**
   * Creates an unwind stage with all options.
   *
   * @param path the array field path (without the $ prefix)
   * @param includeArrayIndex optional field name for the array index
   * @param preserveNullAndEmptyArrays if true, include documents with null/empty arrays
   */
  public UnwindStage(String path, String includeArrayIndex, boolean preserveNullAndEmptyArrays) {
    this.path = Objects.requireNonNull(path, "path must not be null");
    this.includeArrayIndex = includeArrayIndex;
    this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
  }

  /** Returns the array field path. */
  public String getPath() {
    return path;
  }

  /** Returns the optional array index field name. */
  public String getIncludeArrayIndex() {
    return includeArrayIndex;
  }

  /** Returns whether to preserve documents with null or empty arrays. */
  public boolean isPreserveNullAndEmptyArrays() {
    return preserveNullAndEmptyArrays;
  }

  @Override
  public String getOperatorName() {
    return "$unwind";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    ctx.sql("JSON_TABLE(");
    ctx.sql(ctx.getBaseTableAlias());
    ctx.sql(".data, '$.");
    ctx.sql(path);
    ctx.sql("[*]'");

    // Error handling
    if (preserveNullAndEmptyArrays) {
      ctx.sql(" ERROR ON ERROR");
    }

    ctx.sql(" COLUMNS (");

    // The unwound value
    ctx.sql("value JSON PATH '$'");

    // Optional array index
    if (includeArrayIndex != null) {
      ctx.sql(", ");
      ctx.sql(includeArrayIndex);
      ctx.sql(" FOR ORDINALITY");
    }

    ctx.sql(")) ");
    ctx.sql(ctx.generateTableAlias("unwind"));
  }

  @Override
  public String toString() {
    return "UnwindStage(path="
        + path
        + ", includeArrayIndex="
        + includeArrayIndex
        + ", preserveNullAndEmptyArrays="
        + preserveNullAndEmptyArrays
        + ")";
  }
}
