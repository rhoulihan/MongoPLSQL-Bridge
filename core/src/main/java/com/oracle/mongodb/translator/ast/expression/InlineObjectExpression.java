/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an inline object literal expression in MongoDB aggregation.
 *
 * <p>Used when a document with arbitrary field names appears in contexts like $mergeObjects, $cond,
 * etc. For example: {$mergeObjects: ["$profile", {status: "active", timestamp: "$$NOW"}]}
 *
 * <p>Renders to Oracle's JSON_OBJECT function with key-value pairs.
 */
public final class InlineObjectExpression implements Expression {

  private final Map<String, Expression> fields;

  /**
   * Creates an inline object expression with the given field-to-expression map.
   *
   * @param fields the fields and their expressions (order preserved)
   */
  public InlineObjectExpression(Map<String, Expression> fields) {
    this.fields = new LinkedHashMap<>(Objects.requireNonNull(fields, "fields must not be null"));
  }

  /** Returns the fields of this inline object. */
  public Map<String, Expression> getFields() {
    return new LinkedHashMap<>(fields);
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Render as JSON_OBJECT('key1' VALUE expr1, 'key2' VALUE expr2, ...)
    ctx.sql("JSON_OBJECT(");

    boolean first = true;
    for (Map.Entry<String, Expression> entry : fields.entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      first = false;

      // Key
      ctx.sql("'");
      ctx.sql(entry.getKey());
      ctx.sql("' VALUE ");

      // Value - render the expression
      ctx.visit(entry.getValue());
    }

    ctx.sql(")");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    InlineObjectExpression that = (InlineObjectExpression) obj;
    return Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  @Override
  public String toString() {
    return "InlineObject(" + fields + ")";
  }
}
