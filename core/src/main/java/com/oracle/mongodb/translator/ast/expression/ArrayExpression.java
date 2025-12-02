/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an array expression. Translates MongoDB array operators to Oracle JSON path
 * expressions.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$arrayElemAt: ["$items", 0]}} becomes {@code JSON_VALUE(data, '$.items[0]')}
 *   <li>{@code {$size: "$items"}} becomes {@code JSON_VALUE(data, '$.items.size()')}
 *   <li>{@code {$first: "$items"}} becomes {@code JSON_VALUE(data, '$.items[0]')}
 *   <li>{@code {$last: "$items"}} becomes {@code JSON_VALUE(data, '$.items[last]')}
 * </ul>
 */
public final class ArrayExpression implements Expression {

  private final ArrayOp op;
  private final Expression arrayExpression;
  private final Expression indexExpression;
  private final List<Expression> additionalArgs;

  /**
   * Creates an array expression.
   *
   * @param op the array operator
   * @param arrayExpression the array field expression
   * @param indexExpression the index expression (can be null for $size, $first, $last)
   */
  public ArrayExpression(ArrayOp op, Expression arrayExpression, Expression indexExpression) {
    this(op, arrayExpression, indexExpression, null);
  }

  /** Creates an array expression with additional arguments. */
  public ArrayExpression(
      ArrayOp op,
      Expression arrayExpression,
      Expression indexExpression,
      List<Expression> additionalArgs) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.arrayExpression = arrayExpression; // Can be null for $concatArrays
    this.indexExpression = indexExpression;
    this.additionalArgs = additionalArgs != null ? new ArrayList<>(additionalArgs) : null;
  }

  /** Creates a $arrayElemAt expression. */
  public static ArrayExpression arrayElemAt(Expression array, Expression index) {
    return new ArrayExpression(ArrayOp.ARRAY_ELEM_AT, array, index);
  }

  /** Creates a $size expression. */
  public static ArrayExpression size(Expression array) {
    return new ArrayExpression(ArrayOp.SIZE, array, null);
  }

  /** Creates a $first expression. */
  public static ArrayExpression first(Expression array) {
    return new ArrayExpression(ArrayOp.FIRST, array, null);
  }

  /** Creates a $last expression. */
  public static ArrayExpression last(Expression array) {
    return new ArrayExpression(ArrayOp.LAST, array, null);
  }

  /**
   * Creates a $concatArrays expression.
   *
   * @param arrays the arrays to concatenate
   */
  public static ArrayExpression concatArrays(List<Expression> arrays) {
    return new ArrayExpression(ArrayOp.CONCAT_ARRAYS, null, null, arrays);
  }

  /**
   * Creates a $slice expression with count only.
   *
   * @param array the source array
   * @param count number of elements to slice (positive = from start, negative = from end)
   */
  public static ArrayExpression slice(Expression array, Expression count) {
    return new ArrayExpression(ArrayOp.SLICE, array, count);
  }

  /**
   * Creates a $slice expression with skip and count.
   *
   * @param array the source array
   * @param skip number of elements to skip
   * @param count number of elements to take
   */
  public static ArrayExpression sliceWithSkip(Expression array, Expression skip, Expression count) {
    return new ArrayExpression(ArrayOp.SLICE, array, skip, List.of(count));
  }

  /**
   * Creates a $filter expression. Note: $filter requires special handling with 'as' and 'cond'
   * parameters.
   *
   * @param input the array to filter
   * @param condition the filter condition
   */
  public static ArrayExpression filter(Expression input, Expression condition) {
    return new ArrayExpression(ArrayOp.FILTER, input, condition);
  }

  /**
   * Creates a $map expression. Note: $map requires special handling with 'as' and 'in' parameters.
   *
   * @param input the array to map
   * @param expression the mapping expression
   */
  public static ArrayExpression map(Expression input, Expression expression) {
    return new ArrayExpression(ArrayOp.MAP, input, expression);
  }

  /**
   * Creates a $reduce expression.
   *
   * @param input the array to reduce
   * @param initialValue the initial value for the accumulator
   * @param inExpression the reduction expression
   */
  public static ArrayExpression reduce(
      Expression input, Expression initialValue, Expression inExpression) {
    return new ArrayExpression(ArrayOp.REDUCE, input, initialValue, List.of(inExpression));
  }

  /** Returns the array operator. */
  public ArrayOp getOp() {
    return op;
  }

  /** Returns the array expression. */
  public Expression getArrayExpression() {
    return arrayExpression;
  }

  /** Returns the index expression (may be null). */
  public Expression getIndexExpression() {
    return indexExpression;
  }

  /** Returns additional arguments (may be null). */
  public List<Expression> getAdditionalArgs() {
    return additionalArgs != null ? Collections.unmodifiableList(additionalArgs) : null;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    // Handle operators that don't require a single array expression
    switch (op) {
      case CONCAT_ARRAYS -> {
        renderConcatArrays(ctx);
        return;
      }
      case FILTER, MAP, REDUCE -> {
        renderComplexArrayOp(ctx);
        return;
      }
      default -> {
        // Continue with existing logic
      }
    }

    // For field path expressions, use optimized JSON path rendering
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      String path = fieldPath.getPath();
      switch (op) {
        case ARRAY_ELEM_AT -> renderArrayElemAt(ctx, path);
        case SIZE -> renderSize(ctx, path);
        case FIRST -> renderFirst(ctx, path);
        case LAST -> renderLast(ctx, path);
        case SLICE -> renderSlice(ctx, path);
        default -> throw new IllegalStateException("Unexpected array operator: " + op);
      }
    } else {
      // For expression-based arrays (like $split result), use subquery-based approach
      switch (op) {
        case ARRAY_ELEM_AT -> renderArrayElemAtExpression(ctx);
        case SIZE -> renderSizeExpression(ctx);
        case FIRST -> renderFirstExpression(ctx);
        case LAST -> renderLastExpression(ctx);
        case SLICE -> renderSliceExpression(ctx);
        default -> throw new IllegalStateException("Unexpected array operator: " + op);
      }
    }
  }

  /** Renders "alias.data" or just "data" if no alias is set. */
  private void renderDataColumn(SqlGenerationContext ctx) {
    String alias = ctx.getBaseTableAlias();
    if (alias != null && !alias.isEmpty()) {
      ctx.sql(alias);
      ctx.sql(".");
    }
    ctx.sql("data");
  }

  private void renderArrayElemAt(SqlGenerationContext ctx, String path) {
    // JSON_VALUE(alias.data, '$.items[index]')
    if (indexExpression instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
      final int idx = num.intValue();
      ctx.sql("JSON_VALUE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(path);
      ctx.sql("[");
      if (idx >= 0) {
        ctx.sql(String.valueOf(idx));
      } else if (idx == -1) {
        // Oracle supports [last] for the last element
        ctx.sql("last");
      } else {
        // For -2, -3, etc. use [last-1], [last-2], etc.
        ctx.sql("last");
        ctx.sql(
            String.valueOf(
                idx + 1)); // idx is negative, so idx+1 gives the offset (e.g., -2+1 = -1)
      }
      ctx.sql("]')");
    } else {
      throw new IllegalArgumentException("$arrayElemAt index must be a literal number");
    }
  }

  private void renderSize(SqlGenerationContext ctx, String path) {
    // Check if this is a $lookup result field - use correlated subquery instead
    Expression lookupSizeExpr = ctx.getLookupSizeExpression(path);
    if (lookupSizeExpr != null) {
      ctx.visit(lookupSizeExpr);
      return;
    }

    // JSON_VALUE(alias.data, '$.items.size()')
    ctx.sql("JSON_VALUE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(path);
    ctx.sql(".size()')");
  }

  private void renderFirst(SqlGenerationContext ctx, String path) {
    // JSON_VALUE(alias.data, '$.items[0]')
    ctx.sql("JSON_VALUE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(path);
    ctx.sql("[0]')");
  }

  private void renderLast(SqlGenerationContext ctx, String path) {
    // JSON_VALUE(alias.data, '$.items[last]')
    ctx.sql("JSON_VALUE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(path);
    ctx.sql("[last]')");
  }

  private void renderSlice(SqlGenerationContext ctx, String path) {
    // MongoDB: {$slice: ["$items", n]} - first n elements (if n positive) or last |n| (if n
    // negative)
    // MongoDB: {$slice: ["$items", skip, n]} - skip elements, then take n
    // Oracle: JSON_QUERY with array slice syntax
    ctx.sql("JSON_QUERY(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(path);

    if (additionalArgs != null && !additionalArgs.isEmpty()) {
      // Three argument form: array, skip, count
      if (indexExpression instanceof LiteralExpression skipLit
          && skipLit.getValue() instanceof Number skipNum
          && additionalArgs.get(0) instanceof LiteralExpression countLit
          && countLit.getValue() instanceof Number countNum) {
        // Oracle array slice: $.items[skip to skip+count-1]
        int skip = skipNum.intValue();
        int count = countNum.intValue();
        ctx.sql("[");
        ctx.sql(String.valueOf(skip));
        ctx.sql(" to ");
        ctx.sql(String.valueOf(skip + count - 1));
        ctx.sql("]')");
      } else {
        throw new IllegalArgumentException("$slice with skip requires literal numbers");
      }
    } else {
      // Two argument form: array, count
      if (indexExpression instanceof LiteralExpression lit
          && lit.getValue() instanceof Number num) {
        int count = num.intValue();
        if (count >= 0) {
          // First n elements: $.items[0 to n-1]
          ctx.sql("[0 to ");
          ctx.sql(String.valueOf(count - 1));
          ctx.sql("]')");
        } else {
          // Last |n| elements: $.items[last-|n|+1 to last]
          ctx.sql("[last");
          ctx.sql(String.valueOf(count + 1));
          ctx.sql(" to last]')");
        }
      } else {
        throw new IllegalArgumentException("$slice count must be a literal number");
      }
    }
  }

  private void renderConcatArrays(SqlGenerationContext ctx) {
    // MongoDB: {$concatArrays: ["$arr1", "$arr2", ...]}
    // Oracle: Use JSON_QUERY to merge arrays
    if (additionalArgs == null || additionalArgs.isEmpty()) {
      ctx.sql("JSON_QUERY('[]', '$')");
      return;
    }

    // For multiple arrays, we need to use a subquery to concatenate
    // Using JSON_ARRAYAGG with JSON_TABLE to flatten and re-aggregate
    ctx.sql("(SELECT JSON_ARRAYAGG(val ORDER BY rn) FROM (");
    boolean first = true;
    int idx = 0;
    for (Expression arr : additionalArgs) {
      if (!first) {
        ctx.sql(" UNION ALL ");
      }
      if (arr instanceof FieldPathExpression fieldPath) {
        ctx.sql("SELECT val, ROWNUM + ");
        ctx.sql(String.valueOf(idx * 1000)); // Ensure ordering is preserved
        ctx.sql(" AS rn FROM JSON_TABLE(data, '$.");
        ctx.sql(fieldPath.getPath());
        ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
      } else {
        throw new IllegalArgumentException("$concatArrays arguments must be field paths");
      }
      first = false;
      idx++;
    }
    ctx.sql("))");
  }

  private void renderComplexArrayOp(SqlGenerationContext ctx) {
    // $filter, $map, $reduce are complex operations that require
    // variable bindings. For now, render a placeholder or throw.
    // These would need recursive subquery support in Oracle.
    switch (op) {
      case FILTER -> {
        // Oracle doesn't have direct filter, use JSON_TABLE with conditions
        if (arrayExpression instanceof FieldPathExpression fieldPath) {
          ctx.sql("(SELECT JSON_ARRAYAGG(val) FROM JSON_TABLE(data, '$.");
          ctx.sql(fieldPath.getPath());
          ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')) WHERE ");
          ctx.visit(indexExpression); // condition
          ctx.sql(")");
        }
      }
      case MAP -> {
        // Map is complex, would need to apply expression to each element
        if (arrayExpression instanceof FieldPathExpression fieldPath) {
          ctx.sql("(SELECT JSON_ARRAYAGG(");
          ctx.visit(indexExpression); // mapping expression
          ctx.sql(") FROM JSON_TABLE(data, '$.");
          ctx.sql(fieldPath.getPath());
          ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
        }
      }
      case REDUCE -> {
        // Reduce requires accumulator handling - complex in SQL
        // For now, render a placeholder
        ctx.sql("/* $reduce not fully supported */ NULL");
      }
      default -> throw new IllegalStateException("Unexpected complex array operator: " + op);
    }
  }

  /**
   * Renders $arrayElemAt when the array is an expression (not a field path). Uses Oracle's
   * REGEXP_SUBSTR or JSON_TABLE to extract elements from the expression result.
   */
  private void renderArrayElemAtExpression(SqlGenerationContext ctx) {
    if (!(indexExpression instanceof LiteralExpression lit)
        || !(lit.getValue() instanceof Number num)) {
      throw new IllegalArgumentException("$arrayElemAt index must be a literal number");
    }

    int idx = num.intValue();

    // For $split results, use REGEXP_SUBSTR to get the nth element
    if (arrayExpression instanceof StringExpression stringExpr
        && stringExpr.getOp() == StringOp.SPLIT) {
      List<Expression> splitArgs = stringExpr.getArguments();
      if (splitArgs.size() >= 2) {
        Expression inputExpr = splitArgs.get(0);
        Expression delimiterExpr = splitArgs.get(1);

        // REGEXP_SUBSTR(input, '[^delimiter]+', 1, position)
        ctx.sql("REGEXP_SUBSTR(");
        ctx.visit(inputExpr);
        ctx.sql(", '[^'||");
        ctx.visit(delimiterExpr);
        ctx.sql("||']+', 1, ");
        ctx.sql(String.valueOf(idx + 1)); // Oracle REGEXP_SUBSTR is 1-based
        ctx.sql(")");
        return;
      }
    }

    // For other expressions, wrap in JSON_TABLE
    ctx.sql("(SELECT val FROM JSON_TABLE(");
    ctx.visit(arrayExpression);
    ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$', rn FOR ORDINALITY)) WHERE rn = ");
    ctx.sql(String.valueOf(idx + 1));
    ctx.sql(")");
  }

  /** Renders $size when the array is an expression. */
  private void renderSizeExpression(SqlGenerationContext ctx) {
    // For $split results, count delimiters + 1
    if (arrayExpression instanceof StringExpression stringExpr
        && stringExpr.getOp() == StringOp.SPLIT) {
      List<Expression> splitArgs = stringExpr.getArguments();
      if (splitArgs.size() >= 2) {
        Expression inputExpr = splitArgs.get(0);
        Expression delimiterExpr = splitArgs.get(1);

        // REGEXP_COUNT(input, delimiter) + 1
        ctx.sql("(REGEXP_COUNT(");
        ctx.visit(inputExpr);
        ctx.sql(", ");
        ctx.visit(delimiterExpr);
        ctx.sql(") + 1)");
        return;
      }
    }

    // For other expressions, use JSON_TABLE
    ctx.sql("(SELECT COUNT(*) FROM JSON_TABLE(");
    ctx.visit(arrayExpression);
    ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
  }

  /** Renders $first when the array is an expression. */
  private void renderFirstExpression(SqlGenerationContext ctx) {
    // $first is equivalent to $arrayElemAt with index 0
    if (arrayExpression instanceof StringExpression stringExpr
        && stringExpr.getOp() == StringOp.SPLIT) {
      List<Expression> splitArgs = stringExpr.getArguments();
      if (splitArgs.size() >= 2) {
        Expression inputExpr = splitArgs.get(0);
        Expression delimiterExpr = splitArgs.get(1);

        ctx.sql("REGEXP_SUBSTR(");
        ctx.visit(inputExpr);
        ctx.sql(", '[^'||");
        ctx.visit(delimiterExpr);
        ctx.sql("||']+', 1, 1)");
        return;
      }
    }

    ctx.sql("(SELECT val FROM JSON_TABLE(");
    ctx.visit(arrayExpression);
    ctx.sql(", '$[0]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
  }

  /** Renders $last when the array is an expression. */
  private void renderLastExpression(SqlGenerationContext ctx) {
    // For $split results, get the last element
    if (arrayExpression instanceof StringExpression stringExpr
        && stringExpr.getOp() == StringOp.SPLIT) {
      List<Expression> splitArgs = stringExpr.getArguments();
      if (splitArgs.size() >= 2) {
        Expression inputExpr = splitArgs.get(0);
        Expression delimiterExpr = splitArgs.get(1);

        // Get last element: REGEXP_SUBSTR(input, '[^delim]+', 1, REGEXP_COUNT(...)+1)
        ctx.sql("REGEXP_SUBSTR(");
        ctx.visit(inputExpr);
        ctx.sql(", '[^'||");
        ctx.visit(delimiterExpr);
        ctx.sql("||']+', 1, REGEXP_COUNT(");
        ctx.visit(inputExpr);
        ctx.sql(", ");
        ctx.visit(delimiterExpr);
        ctx.sql(") + 1)");
        return;
      }
    }

    ctx.sql("(SELECT val FROM JSON_TABLE(");
    ctx.visit(arrayExpression);
    ctx.sql(", '$[last]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
  }

  /** Renders $slice when the array is an expression. */
  private void renderSliceExpression(SqlGenerationContext ctx) {
    // For expression arrays, use JSON_TABLE with row limiting
    ctx.sql("(SELECT JSON_ARRAYAGG(val ORDER BY rn) FROM (SELECT val, rn FROM JSON_TABLE(");
    ctx.visit(arrayExpression);
    ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$', rn FOR ORDINALITY))");

    if (additionalArgs != null && !additionalArgs.isEmpty()) {
      // Three argument form: array, skip, count
      if (indexExpression instanceof LiteralExpression skipLit
          && skipLit.getValue() instanceof Number skipNum
          && additionalArgs.get(0) instanceof LiteralExpression countLit
          && countLit.getValue() instanceof Number countNum) {
        int skip = skipNum.intValue();
        int count = countNum.intValue();
        ctx.sql(" WHERE rn > ");
        ctx.sql(String.valueOf(skip));
        ctx.sql(" FETCH FIRST ");
        ctx.sql(String.valueOf(count));
        ctx.sql(" ROWS ONLY))");
      }
    } else if (indexExpression instanceof LiteralExpression lit
        && lit.getValue() instanceof Number num) {
      int count = num.intValue();
      if (count >= 0) {
        ctx.sql(" FETCH FIRST ");
        ctx.sql(String.valueOf(count));
        ctx.sql(" ROWS ONLY))");
      } else {
        // Negative count: last |n| elements
        ctx.sql(" ORDER BY rn DESC FETCH FIRST ");
        ctx.sql(String.valueOf(-count));
        ctx.sql(" ROWS ONLY) ORDER BY rn))");
      }
    }
  }

  @Override
  public String toString() {
    if (indexExpression != null) {
      return "Array("
          + op.getMongoOperator()
          + ", "
          + arrayExpression
          + ", "
          + indexExpression
          + ")";
    }
    return "Array(" + op.getMongoOperator() + ", " + arrayExpression + ")";
  }
}
