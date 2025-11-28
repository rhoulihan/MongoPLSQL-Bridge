/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.List;
import java.util.Objects;

/** Represents an IN or NOT IN expression. */
public final class InExpression implements Expression {

  private final Expression field;
  private final List<Object> values;
  private final boolean negated;

  /**
   * Creates an IN or NOT IN expression.
   *
   * @param field the field to match
   * @param values the list of values to match against
   * @param negated true for NOT IN, false for IN
   */
  public InExpression(Expression field, List<Object> values, boolean negated) {
    this.field = Objects.requireNonNull(field, "field must not be null");
    this.values = List.copyOf(Objects.requireNonNull(values, "values must not be null"));
    this.negated = negated;

    if (values.isEmpty()) {
      throw new IllegalArgumentException("IN expression must have at least one value");
    }
  }

  /** Creates an IN expression. */
  public static InExpression in(Expression field, List<Object> values) {
    return new InExpression(field, values, false);
  }

  /** Creates a NOT IN expression. */
  public static InExpression notIn(Expression field, List<Object> values) {
    return new InExpression(field, values, true);
  }

  /** Returns the field expression. */
  public Expression getField() {
    return field;
  }

  /** Returns the list of values. */
  public List<Object> getValues() {
    return values;
  }

  /** Returns true if this is a NOT IN expression. */
  public boolean isNegated() {
    return negated;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    ctx.visit(field);
    if (negated) {
      ctx.sql(" NOT IN (");
    } else {
      ctx.sql(" IN (");
    }

    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      ctx.bind(values.get(i));
    }

    ctx.sql(")");
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    InExpression that = (InExpression) obj;
    return negated == that.negated
        && Objects.equals(field, that.field)
        && Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, values, negated);
  }

  @Override
  public String toString() {
    return (negated ? "NotIn(" : "In(") + field + ", " + values + ")";
  }
}
