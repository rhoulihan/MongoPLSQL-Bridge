/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.ExistsExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.InExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;

/**
 * Builds JSON_EXISTS predicates with type-safe filter expressions.
 *
 * <p>This builder generates predicates following Oracle MongoDB API patterns:
 * <ul>
 *   <li>Uses stringOnly() for String values</li>
 *   <li>Uses numberOnly() for numeric values</li>
 *   <li>Uses booleanOnly() for Boolean values</li>
 *   <li>Uses PASSING clause for bind variables</li>
 *   <li>Uses TYPE(strict) for strict type checking</li>
 * </ul>
 *
 * <p>Example output:
 * <pre>
 * JSON_EXISTS("DATA", '$?(@.status.stringOnly() == $B0)' PASSING :1 AS "B0" TYPE(strict))
 * </pre>
 */
public final class JsonExistsPredicateBuilder {

  private final SqlGenerationContext ctx;
  private final java.util.List<Object> bindValues = new java.util.ArrayList<>();

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification = "ctx is intentionally shared with caller for building SQL")
  public JsonExistsPredicateBuilder(SqlGenerationContext ctx) {
    this.ctx = ctx;
  }

  /**
   * Renders a JSON_EXISTS predicate for the given expression.
   */
  public void render(Expression filter) {
    ctx.sql("JSON_EXISTS(\"DATA\", '$?(");
    renderFilterExpression(filter);
    ctx.sql(")'");
    renderPassingClause();
    ctx.sql(")");
  }

  private void renderFilterExpression(Expression expr) {
    if (expr instanceof ComparisonExpression comp) {
      renderComparison(comp);
    } else if (expr instanceof InExpression inExpr) {
      renderInExpression(inExpr);
    } else if (expr instanceof LogicalExpression logical) {
      renderLogical(logical);
    } else if (expr instanceof ExistsExpression existsExpr) {
      renderExistsExpression(existsExpr);
    } else {
      // Fallback for unsupported expressions
      ctx.sql("true");
    }
  }

  /**
   * Renders an ExistsExpression for $exists operator.
   * $exists: true  -> exists(@.field)
   * $exists: false -> !(exists(@.field))
   */
  private void renderExistsExpression(ExistsExpression existsExpr) {
    String fieldPath = existsExpr.getFieldPath();
    boolean exists = existsExpr.isExists();

    if (!exists) {
      ctx.sql("!(");
    }
    ctx.sql("exists(@.");
    ctx.sql(convertToOracleJsonPath(fieldPath));
    ctx.sql(")");
    if (!exists) {
      ctx.sql(")");
    }
  }

  /**
   * Converts MongoDB field path to Oracle JSON path syntax.
   * MongoDB uses dot notation for array indices (items.0),
   * Oracle requires bracket notation (items[0]).
   */
  private String convertToOracleJsonPath(String path) {
    String[] segments = path.split("\\.");
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < segments.length; i++) {
      String segment = segments[i];
      if (isNumeric(segment)) {
        result.append("[").append(segment).append("]");
      } else {
        if (i > 0) {
          result.append(".");
        }
        result.append(segment);
      }
    }
    return result.toString();
  }

  private boolean isNumeric(String str) {
    if (str == null || str.isEmpty()) {
      return false;
    }
    for (int i = 0; i < str.length(); i++) {
      if (!Character.isDigit(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private void renderComparison(ComparisonExpression comp) {
    Expression left = comp.getLeft();
    Expression right = comp.getRight();
    ComparisonOp op = comp.getOp();

    // Handle field path on left side
    if (left instanceof FieldPathExpression fieldPath) {
      String path = fieldPath.getPath();

      // Determine type method based on right side value
      if (right instanceof LiteralExpression literal) {
        Object value = literal.getValue();

        // Handle null comparisons specially - render 'null' literal directly
        if (value == null) {
          renderNullComparison(path, op);
          return;
        }

        String typeMethod = getTypeMethod(value);

        // Check if path contains nested array access (e.g., "items.product")
        // For MVI index pickup, use $.array[*]?(@.field) format
        if (path.contains(".")) {
          renderNestedArrayComparison(path, typeMethod, op, value);
        } else if ((op == ComparisonOp.IN || op == ComparisonOp.NIN) && value instanceof List) {
          // Handle $in/$nin with list values - expand to separate bind variables
          renderInComparison(path, op, (List<?>) value);
        } else {
          // Simple field path
          ctx.sql("@.");
          ctx.sql(path);
          ctx.sql(typeMethod);

          // Render operator
          ctx.sql(" ");
          ctx.sql(getJsonExistsOperator(op));
          ctx.sql(" ");

          // Render bind variable reference (B0, B1, etc.)
          String bindVar = "B" + bindValues.size();
          ctx.sql("$" + bindVar);

          // Track the value for the PASSING clause (don't call ctx.bind yet)
          bindValues.add(value);
        }
      }
    }
  }

  /**
   * Renders a null comparison using JSON path.
   * MongoDB {field: null} matches both explicit null AND missing field.
   * MongoDB {field: {$ne: null}} matches documents where field exists and is not null.
   * For $eq: null, renders "(@.field == null || !(exists(@.field)))"
   * For $ne: null, renders "@.field != null" (Oracle semantics match MongoDB for NE)
   */
  private void renderNullComparison(String path, ComparisonOp op) {
    if (op == ComparisonOp.EQ) {
      // Match documents where field is null OR field doesn't exist
      ctx.sql("(@.");
      ctx.sql(path);
      ctx.sql(" == null || !(exists(@.");
      ctx.sql(path);
      ctx.sql(")))");
    } else if (op == ComparisonOp.NE) {
      // Match documents where field exists AND is not null
      // Oracle's "!= null" already has correct semantics - returns false for missing fields
      ctx.sql("@.");
      ctx.sql(path);
      ctx.sql(" != null");
    } else {
      // For other comparison operators with null, just render the comparison
      ctx.sql("@.");
      ctx.sql(path);
      ctx.sql(" ");
      ctx.sql(getJsonExistsOperator(op));
      ctx.sql(" null");
    }
  }

  /**
   * Renders an InExpression for $in/$nin operators.
   */
  private void renderInExpression(InExpression inExpr) {
    Expression field = inExpr.getField();
    if (field instanceof FieldPathExpression fieldPath) {
      String path = fieldPath.getPath();
      List<Object> values = inExpr.getValues();
      ComparisonOp op = inExpr.isNegated() ? ComparisonOp.NIN : ComparisonOp.IN;
      renderInComparison(path, op, values);
    }
  }

  /**
   * Renders an IN/NIN comparison with expanded bind variables.
   * Converts list values to "in ($B0, $B1, $B2)" format.
   * For NIN, wraps the expression in negation: !(@.field in (...))
   * because Oracle JSON path doesn't support "not in" syntax.
   */
  private void renderInComparison(String path, ComparisonOp op, List<?> values) {
    // For NIN, wrap in negation: !(@.field in (...))
    boolean isNegated = (op == ComparisonOp.NIN);
    if (isNegated) {
      ctx.sql("!(");
    }

    ctx.sql("@.");
    ctx.sql(path);

    // Infer type method from first list element
    if (!values.isEmpty()) {
      String typeMethod = getTypeMethod(values.get(0));
      ctx.sql(typeMethod);
    }

    // Always use "in" operator (negation is handled by the wrapper)
    ctx.sql(" in (");

    // Expand list values as separate bind variables
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      String bindVar = "B" + bindValues.size();
      ctx.sql("$" + bindVar);
      bindValues.add(values.get(i));
    }
    ctx.sql(")");

    if (isNegated) {
      ctx.sql(")");
    }
  }

  /**
   * Renders a nested array comparison for MVI index pickup.
   * Converts "items.product" to "$.items[*]?(@.product.stringOnly() == $B0)" format.
   */
  private void renderNestedArrayComparison(
      String path, String typeMethod, ComparisonOp op, Object value) {
    int dotIndex = path.indexOf('.');
    String arrayPart = path.substring(0, dotIndex);
    String nestedPart = path.substring(dotIndex + 1);

    // Generate MVI-compatible format: $.array[*]?(@.field.typeMethod() op $bindVar)
    ctx.sql("$.");
    ctx.sql(arrayPart);
    ctx.sql("[*]?(@.");
    ctx.sql(nestedPart);
    ctx.sql(typeMethod);
    ctx.sql(" ");
    ctx.sql(getJsonExistsOperator(op));
    ctx.sql(" ");

    String bindVar = "B" + bindValues.size();
    ctx.sql("$" + bindVar);
    ctx.sql(")");

    bindValues.add(value);
  }

  private void renderLogical(LogicalExpression logical) {
    LogicalOp op = logical.getOp();
    List<Expression> operands = logical.getOperands();

    if (op == LogicalOp.NOT && operands.size() == 1) {
      ctx.sql("!(");
      renderFilterExpression(operands.get(0));
      ctx.sql(")");
      return;
    }

    // NOR is NOT (cond1 OR cond2 OR ...) - wrap in negation
    boolean isNor = (op == LogicalOp.NOR);
    if (isNor) {
      ctx.sql("!(");
    }

    String separator = op == LogicalOp.AND ? " && " : " || ";

    ctx.sql("(");
    for (int i = 0; i < operands.size(); i++) {
      if (i > 0) {
        ctx.sql(separator);
      }
      renderFilterExpression(operands.get(i));
    }
    ctx.sql(")");

    if (isNor) {
      ctx.sql(")");
    }
  }

  /**
   * Returns the appropriate type method based on the value type.
   */
  private String getTypeMethod(Object value) {
    if (value == null) {
      return "";
    } else if (value instanceof String) {
      return ".stringOnly()";
    } else if (value instanceof Number) {
      return ".numberOnly()";
    } else if (value instanceof Boolean) {
      return ".booleanOnly()";
    } else if (value instanceof java.util.Date || value instanceof java.time.Instant) {
      return ".timestamp()";
    } else if (value instanceof List) {
      // For $in operator with list, no type method on the field
      return "";
    }
    return "";
  }

  /**
   * Returns the JSON_EXISTS filter operator for a MongoDB comparison operator.
   */
  private String getJsonExistsOperator(ComparisonOp op) {
    return switch (op) {
      case EQ -> "==";
      case NE -> "!=";
      case GT -> ">";
      case GTE -> ">=";
      case LT -> "<";
      case LTE -> "<=";
      case IN -> "in";
      case NIN -> "not in";
      default -> "==";
    };
  }

  /**
   * Renders the PASSING clause with bind variables.
   */
  private void renderPassingClause() {
    if (!bindValues.isEmpty()) {
      ctx.sql(" PASSING ");
      for (int i = 0; i < bindValues.size(); i++) {
        if (i > 0) {
          ctx.sql(", ");
        }
        // Add the bind variable to context (this creates :1, :2, etc.)
        ctx.bind(bindValues.get(i));
        ctx.sql(" AS \"B" + i + "\"");
      }
      ctx.sql(" TYPE(strict)");
    }
  }
}
