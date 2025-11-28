/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.List;
import java.util.Objects;

/**
 * Represents a $merge stage that writes the results of the aggregation pipeline to a specified
 * collection.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * {
 *   $merge: {
 *     into: "targetCollection",  // or { db: "db", coll: "collection" }
 *     on: "_id",                 // or ["field1", "field2"]
 *     whenMatched: "replace",    // or "keepExisting", "merge", "fail", pipeline
 *     whenNotMatched: "insert"   // or "discard", "fail"
 *   }
 * }
 * </pre>
 *
 * <p>Oracle translation uses MERGE statement:
 *
 * <pre>
 * MERGE INTO targetCollection t
 * USING (SELECT ... FROM source) s
 * ON (t._id = s._id)
 * WHEN MATCHED THEN UPDATE SET t.data = s.data
 * WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data)
 * </pre>
 *
 * <p>Note: This is a stub implementation for the AST structure. Full MERGE support requires
 * additional work.
 */
public final class MergeStage implements Stage {

  private final String targetCollection;
  private final List<String> onFields;
  private final WhenMatched whenMatched;
  private final WhenNotMatched whenNotMatched;

  /** Behavior when a document matches. */
  public enum WhenMatched {
    REPLACE,
    KEEP_EXISTING,
    MERGE,
    FAIL
  }

  /** Behavior when no document matches. */
  public enum WhenNotMatched {
    INSERT,
    DISCARD,
    FAIL
  }

  /**
   * Creates a merge stage with default options.
   *
   * @param targetCollection the target collection name
   */
  public MergeStage(String targetCollection) {
    this(targetCollection, List.of("_id"), WhenMatched.MERGE, WhenNotMatched.INSERT);
  }

  /**
   * Creates a merge stage with all options.
   *
   * @param targetCollection the target collection name
   * @param onFields the fields to match on
   * @param whenMatched action when document matches
   * @param whenNotMatched action when document doesn't match
   */
  public MergeStage(
      String targetCollection,
      List<String> onFields,
      WhenMatched whenMatched,
      WhenNotMatched whenNotMatched) {
    this.targetCollection =
        Objects.requireNonNull(targetCollection, "targetCollection must not be null");
    this.onFields = onFields != null ? List.copyOf(onFields) : List.of("_id");
    this.whenMatched = whenMatched != null ? whenMatched : WhenMatched.MERGE;
    this.whenNotMatched = whenNotMatched != null ? whenNotMatched : WhenNotMatched.INSERT;
  }

  public String getTargetCollection() {
    return targetCollection;
  }

  public List<String> getOnFields() {
    return onFields;
  }

  public WhenMatched getWhenMatched() {
    return whenMatched;
  }

  public WhenNotMatched getWhenNotMatched() {
    return whenNotMatched;
  }

  @Override
  public String getOperatorName() {
    return "$merge";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // $merge requires special handling - generates MERGE statement
    // This render provides debugging output
    ctx.sql("/* MERGE INTO ");
    ctx.sql(targetCollection);
    ctx.sql(" ON ");
    ctx.sql(String.join(", ", onFields));
    ctx.sql(" */");
  }

  @Override
  public String toString() {
    return "MergeStage(target="
        + targetCollection
        + ", on="
        + onFields
        + ", whenMatched="
        + whenMatched
        + ", whenNotMatched="
        + whenNotMatched
        + ")";
  }
}
