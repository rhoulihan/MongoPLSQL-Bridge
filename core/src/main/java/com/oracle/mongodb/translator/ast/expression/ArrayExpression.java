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
  private final String sortField; // For $sortArray with field-based sortBy

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
    this(op, arrayExpression, indexExpression, additionalArgs, null);
  }

  /** Creates an array expression with additional arguments and sort field. */
  public ArrayExpression(
      ArrayOp op,
      Expression arrayExpression,
      Expression indexExpression,
      List<Expression> additionalArgs,
      String sortField) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.arrayExpression = arrayExpression; // Can be null for $concatArrays
    this.indexExpression = indexExpression;
    this.additionalArgs = additionalArgs != null ? new ArrayList<>(additionalArgs) : null;
    this.sortField = sortField;
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

  /**
   * Creates a $reverseArray expression.
   *
   * @param array the array to reverse
   */
  public static ArrayExpression reverseArray(Expression array) {
    return new ArrayExpression(ArrayOp.REVERSE_ARRAY, array, null);
  }

  /**
   * Creates a $sum expression that sums all values in an array.
   *
   * @param array the array field path to sum
   */
  public static ArrayExpression sumArray(Expression array) {
    return new ArrayExpression(ArrayOp.SUM_ARRAY, array, null);
  }

  /**
   * Creates a $avg expression that averages all values in an array.
   *
   * @param array the array field path to average
   */
  public static ArrayExpression avgArray(Expression array) {
    return new ArrayExpression(ArrayOp.AVG_ARRAY, array, null);
  }

  /**
   * Creates a $range expression with start and end (step defaults to 1).
   *
   * @param start the starting value (inclusive)
   * @param end the ending value (exclusive)
   */
  public static ArrayExpression range(Expression start, Expression end) {
    // Store start as arrayExpression, end as indexExpression, step as 1 in additionalArgs
    return new ArrayExpression(ArrayOp.RANGE, start, end, List.of(LiteralExpression.of(1)));
  }

  /**
   * Creates a $range expression with start, end, and step.
   *
   * @param start the starting value (inclusive)
   * @param end the ending value (exclusive)
   * @param step the increment value
   */
  public static ArrayExpression range(Expression start, Expression end, Expression step) {
    return new ArrayExpression(ArrayOp.RANGE, start, end, List.of(step));
  }

  /**
   * Creates a $zip expression.
   *
   * @param inputs the list of arrays to zip together
   */
  public static ArrayExpression zip(List<Expression> inputs) {
    return zip(inputs, false, null);
  }

  /**
   * Creates a $zip expression with options.
   *
   * @param inputs the list of arrays to zip together
   * @param useLongestLength if true, use longest array length; if false, use shortest
   * @param defaults default values for shorter arrays (null if not provided)
   */
  public static ArrayExpression zip(
      List<Expression> inputs, boolean useLongestLength, List<Expression> defaults) {
    // Store inputs in additionalArgs, useLongestLength as a LiteralExpression in arrayExpression
    List<Expression> args = new ArrayList<>();
    args.addAll(inputs);
    if (defaults != null) {
      args.addAll(defaults);
    }
    return new ArrayExpression(
        ArrayOp.ZIP,
        LiteralExpression.of(useLongestLength),
        LiteralExpression.of(inputs.size()),
        args);
  }

  /**
   * Creates a $sortArray expression.
   *
   * @param array the array to sort
   * @param ascending true for ascending, false for descending
   */
  public static ArrayExpression sortArray(Expression array, boolean ascending) {
    // Store sort direction in indexExpression as a literal
    return new ArrayExpression(ArrayOp.SORT_ARRAY, array, LiteralExpression.of(ascending ? 1 : -1));
  }

  /**
   * Creates a $sortArray expression with field-based sorting.
   *
   * <p>MongoDB: {$sortArray: {input: "$products", sortBy: {totalRevenue: -1}}}
   *
   * @param array the array to sort
   * @param sortField the field name to sort by
   * @param ascending true for ascending, false for descending
   */
  public static ArrayExpression sortArrayByField(
      Expression array, String sortField, boolean ascending) {
    return new ArrayExpression(
        ArrayOp.SORT_ARRAY,
        array,
        LiteralExpression.of(ascending ? 1 : -1),
        null,
        sortField);
  }

  /**
   * Creates a $in expression that checks if a value is in an array.
   *
   * @param value the value to search for
   * @param array the array to search in
   */
  public static ArrayExpression in(Expression value, Expression array) {
    return new ArrayExpression(ArrayOp.IN, array, value);
  }

  /**
   * Creates a $isArray expression.
   *
   * @param expression the expression to check
   */
  public static ArrayExpression isArray(Expression expression) {
    return new ArrayExpression(ArrayOp.IS_ARRAY, expression, null);
  }

  /**
   * Creates a $indexOfArray expression.
   *
   * @param array the array to search
   * @param value the value to find
   */
  public static ArrayExpression indexOfArray(Expression array, Expression value) {
    return new ArrayExpression(ArrayOp.INDEX_OF_ARRAY, array, value);
  }

  /**
   * Creates a $indexOfArray expression with start and end range.
   *
   * @param array the array to search
   * @param value the value to find
   * @param start the starting index
   * @param end the ending index
   */
  public static ArrayExpression indexOfArrayWithRange(
      Expression array, Expression value, Expression start, Expression end) {
    return new ArrayExpression(ArrayOp.INDEX_OF_ARRAY, array, value, List.of(start, end));
  }

  /**
   * Creates a $setUnion expression that combines arrays into a set with unique elements.
   *
   * @param arrays the arrays to union
   */
  public static ArrayExpression setUnion(List<Expression> arrays) {
    return new ArrayExpression(ArrayOp.SET_UNION, null, null, arrays);
  }

  /**
   * Creates a $setIntersection expression that returns common elements from arrays.
   *
   * @param arrays the arrays to intersect
   */
  public static ArrayExpression setIntersection(List<Expression> arrays) {
    return new ArrayExpression(ArrayOp.SET_INTERSECTION, null, null, arrays);
  }

  /**
   * Creates a $setDifference expression that returns elements in first array but not in second.
   *
   * @param array1 the first array
   * @param array2 the second array
   */
  public static ArrayExpression setDifference(Expression array1, Expression array2) {
    return new ArrayExpression(ArrayOp.SET_DIFFERENCE, array1, array2);
  }

  /**
   * Creates a $setEquals expression that checks if arrays have the same elements.
   *
   * @param arrays the arrays to compare
   */
  public static ArrayExpression setEquals(List<Expression> arrays) {
    return new ArrayExpression(ArrayOp.SET_EQUALS, null, null, arrays);
  }

  /**
   * Creates a $setIsSubset expression that checks if first array is a subset of second.
   *
   * @param array1 the first array (potential subset)
   * @param array2 the second array (superset)
   */
  public static ArrayExpression setIsSubset(Expression array1, Expression array2) {
    return new ArrayExpression(ArrayOp.SET_IS_SUBSET, array1, array2);
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

  /** Returns the sort field for field-based $sortArray (may be null). */
  public String getSortField() {
    return sortField;
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
      case REVERSE_ARRAY -> {
        renderReverseArray(ctx);
        return;
      }
      case SORT_ARRAY -> {
        renderSortArray(ctx);
        return;
      }
      case IN -> {
        renderIn(ctx);
        return;
      }
      case IS_ARRAY -> {
        renderIsArray(ctx);
        return;
      }
      case INDEX_OF_ARRAY -> {
        renderIndexOfArray(ctx);
        return;
      }
      case SET_UNION -> {
        renderSetUnion(ctx);
        return;
      }
      case SET_INTERSECTION -> {
        renderSetIntersection(ctx);
        return;
      }
      case SET_DIFFERENCE -> {
        renderSetDifference(ctx);
        return;
      }
      case SET_EQUALS -> {
        renderSetEquals(ctx);
        return;
      }
      case SET_IS_SUBSET -> {
        renderSetIsSubset(ctx);
        return;
      }
      case RANGE -> {
        renderRange(ctx);
        return;
      }
      case ZIP -> {
        renderZip(ctx);
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
        case SUM_ARRAY -> renderSumArray(ctx, path);
        case AVG_ARRAY -> renderAvgArray(ctx, path);
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
        case SUM_ARRAY -> renderSumArrayExpression(ctx);
        case AVG_ARRAY -> renderAvgArrayExpression(ctx);
        default -> throw new IllegalStateException("Unexpected array operator: " + op);
      }
    }
  }

  /**
   * Renders the data column reference.
   * In new CTE context (with DATA column): outputs "DATA" (quoted column name)
   * Otherwise: outputs "alias.data" or just "data"
   */
  private void renderDataColumn(SqlGenerationContext ctx) {
    if (ctx.usesCteDataColumn()) {
      ctx.sql("\"DATA\"");
      return;
    }
    String alias = ctx.getBaseTableAlias();
    if (alias != null && !alias.isEmpty()) {
      ctx.sql(alias);
      ctx.sql(".");
    }
    ctx.sql("data");
  }

  private void renderArrayElemAt(SqlGenerationContext ctx, String path) {
    // Use JSON_QUERY with array subscript to preserve types (numbers, strings, booleans)
    // Unlike JSON_VALUE which returns VARCHAR2, JSON_QUERY returns native JSON types
    // Oracle dot notation doesn't support array subscripts like data.items[0]

    // Normalize path by removing $ prefix if present
    String normalizedPath = path.startsWith("$") ? path.substring(1) : path;

    // Extract the root field name (e.g., "customerInfo" from "customerInfo.tier")
    int dotIndex = normalizedPath.indexOf('.');
    String rootField = dotIndex >= 0 ? normalizedPath.substring(0, dotIndex) : normalizedPath;

    // Check if this is a pipeline form $lookup
    // Pipeline lookups produce a JSON array via LATERAL subquery
    String pipelineLookupAlias = ctx.getPipelineLookupAlias(rootField);
    if (pipelineLookupAlias != null) {
      // Pipeline lookup: access the LATERAL result column
      // e.g., $arrayElemAt: ["$inventoryData.totalStock", 0]
      // -> JSON_QUERY(inventory_1.inventoryData, '$[0].totalStock')
      if (indexExpression instanceof LiteralExpression lit
          && lit.getValue() instanceof Number num) {
        final int idx = num.intValue();
        ctx.sql("JSON_QUERY(");
        ctx.sql(pipelineLookupAlias);
        ctx.sql(".");
        ctx.sql(rootField);
        ctx.sql(", '$[");
        if (idx >= 0) {
          ctx.sql(String.valueOf(idx));
        } else if (idx == -1) {
          ctx.sql("last");
        } else {
          ctx.sql("last");
          ctx.sql(String.valueOf(idx + 1));
        }
        ctx.sql("]");
        // Append remaining path after the root field if present
        if (dotIndex >= 0) {
          ctx.sql(".");
          ctx.sql(normalizedPath.substring(dotIndex + 1));
        }
        ctx.sql("')");
        return;
      }
    }

    // Check if this path references an equality form $lookup result field
    // e.g., "customerInfo.tier" where "customerInfo" is from $lookup
    // For equality lookups with index 0, we can directly access the joined table's field
    // since the LEFT JOIN produces one row per match
    String lookupAlias = ctx.getLookupTableAlias(normalizedPath);
    if (lookupAlias != null) {
      if (indexExpression instanceof LiteralExpression lit
          && lit.getValue() instanceof Number num
          && num.intValue() == 0) {
        // Extract the remaining path after the lookup alias
        // e.g., "customerInfo.tier" -> "tier"
        String remainingPath = dotIndex >= 0 ? normalizedPath.substring(dotIndex + 1) : "";

        // For index 0, just access the joined table's data column directly
        // The LEFT JOIN gives us the matching row, so no array access needed
        ctx.sql(lookupAlias);
        ctx.sql(".data");
        if (!remainingPath.isEmpty()) {
          ctx.sql(".");
          ctx.sql(remainingPath);
        }
        return;
      }
      // For non-zero indices, fall through to generate array subscript
      // (though this is rare for equality lookups which typically match 0 or 1 row)
    }

    if (indexExpression instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
      final int idx = num.intValue();
      ctx.sql("JSON_QUERY(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(normalizedPath);
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
    // Normalize path by removing $ prefix if present
    String normalizedPath = path.startsWith("$") ? path.substring(1) : path;

    // Check if this is a $lookup result field - use correlated subquery instead
    Expression lookupSizeExpr = ctx.getLookupSizeExpression(normalizedPath);
    if (lookupSizeExpr != null) {
      ctx.visit(lookupSizeExpr);
      return;
    }

    // Check if this is a pipeline form $lookup result field
    // Pipeline lookups produce a JSON array column via LATERAL subquery
    String pipelineLookupAlias = ctx.getPipelineLookupAlias(normalizedPath);
    if (pipelineLookupAlias != null) {
      // For pipeline lookup: NVL(JSON_VALUE(alias.columnName, '$.size()' RETURNING NUMBER), 0)
      // Use NVL to return 0 when array is null (LATERAL join with no matches)
      // This matches MongoDB's behavior where $size of missing/null array returns 0
      ctx.sql("NVL(JSON_VALUE(");
      ctx.sql(pipelineLookupAlias);
      ctx.sql(".");
      ctx.sql(normalizedPath); // column name is the same as the "as" field
      ctx.sql(", '$.size()' RETURNING NUMBER), 0)");
      return;
    }

    // In CTE context, the array is a direct column (e.g., from $addToSet accumulator)
    // Reference the column directly: JSON_VALUE(columnName, '$.size()' RETURNING NUMBER)
    if (ctx.isInCteContext()) {
      ctx.sql("JSON_VALUE(");
      ctx.sql(normalizedPath);
      ctx.sql(", '$.size()' RETURNING NUMBER)");
      return;
    }

    // Note: .size() is a JSON path function, not available in simple dot notation.
    // We must use JSON_VALUE here. The result is NUMBER since it's an array size.
    ctx.sql("JSON_VALUE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(normalizedPath);
    ctx.sql(".size()' RETURNING NUMBER)");
  }

  private void renderFirst(SqlGenerationContext ctx, String path) {
    // Use JSON_QUERY: JSON_QUERY(data, '$.items[0]') - preserves types
    // Oracle dot notation doesn't support array subscripts
    ctx.sql("JSON_QUERY(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(path);
    ctx.sql("[0]')");
  }

  private void renderLast(SqlGenerationContext ctx, String path) {
    // Use JSON_QUERY: JSON_QUERY(data, '$.items[last]') - preserves types
    // Oracle dot notation doesn't support array subscripts
    ctx.sql("JSON_QUERY(");
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
    // Use WITH ARRAY WRAPPER to ensure result is always an array (not scalar for single elements)
    // Use EMPTY ARRAY ON EMPTY to return [] instead of null for empty slices
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
        ctx.sql("]' WITH ARRAY WRAPPER EMPTY ARRAY ON EMPTY)");
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
          ctx.sql("]' WITH ARRAY WRAPPER EMPTY ARRAY ON EMPTY)");
        } else {
          // Last |n| elements: $.items[last-|n|+1 to last]
          ctx.sql("[last");
          ctx.sql(String.valueOf(count + 1));
          ctx.sql(" to last]' WITH ARRAY WRAPPER EMPTY ARRAY ON EMPTY)");
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
        ctx.sql(" AS rn FROM JSON_TABLE(");
        renderDataColumn(ctx);
        ctx.sql(", '$.");
        ctx.sql(fieldPath.getPath());
        ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
      } else if (arr instanceof LiteralExpression lit && lit.getValue() instanceof List<?> list) {
        // Handle literal arrays like ["extra"]
        ctx.sql("SELECT val, ROWNUM + ");
        ctx.sql(String.valueOf(idx * 1000));
        ctx.sql(" AS rn FROM JSON_TABLE('");
        ctx.sql(toJsonArray(list));
        ctx.sql("', '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
      } else {
        throw new IllegalArgumentException(
            "$concatArrays arguments must be field paths or literal arrays");
      }
      first = false;
      idx++;
    }
    ctx.sql("))");
  }

  /** Converts a Java List to a JSON array string. */
  private String toJsonArray(List<?> list) {
    StringBuilder sb = new StringBuilder("[");
    boolean first = true;
    for (Object item : list) {
      if (!first) {
        sb.append(",");
      }
      if (item instanceof String s) {
        // Escape any single quotes in the string value
        sb.append("\"").append(s.replace("\"", "\\\"")).append("\"");
      } else if (item instanceof Number) {
        sb.append(item);
      } else if (item instanceof Boolean) {
        sb.append(item);
      } else if (item == null) {
        sb.append("null");
      } else {
        sb.append("\"").append(item.toString().replace("\"", "\\\"")).append("\"");
      }
      first = false;
    }
    sb.append("]");
    return sb.toString();
  }

  private void renderComplexArrayOp(SqlGenerationContext ctx) {
    // $filter, $map, $reduce are complex operations that require
    // variable bindings. For now, render a placeholder or throw.
    // These would need recursive subquery support in Oracle.
    switch (op) {
      case FILTER -> {
        // Oracle doesn't have direct filter, use JSON_TABLE with conditions
        if (arrayExpression instanceof FieldPathExpression fieldPath) {
          // Check if condition uses variable field access (e.g., $$item.price)
          String varField = extractVariableFieldFromCondition(indexExpression);
          if (varField != null) {
            // Generate JSON_TABLE with additional column for the variable field
            // Use COALESCE to return empty array instead of NULL when no matches
            ctx.sql("(SELECT COALESCE(JSON_ARRAYAGG(val FORMAT JSON), JSON_ARRAY())");
            ctx.sql(" FROM JSON_TABLE(");
            renderDataColumn(ctx);
            ctx.sql(", '$.");
            ctx.sql(fieldPath.getPath());
            ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$', ");
            ctx.sql(varField);
            ctx.sql(" NUMBER PATH '$.");
            ctx.sql(varField);
            ctx.sql("')) WHERE ");
            // Render condition, replacing variable field reference with column name
            renderConditionWithFieldSubstitution(ctx, indexExpression, varField);
            ctx.sql(")");
          } else {
            // Try to extract ALL variable fields from the condition (for complex expressions)
            java.util.Set<String> allVarFields = new java.util.LinkedHashSet<>();
            collectVariableFieldsFromExpression(indexExpression, allVarFields);

            if (!allVarFields.isEmpty()) {
              // Generate JSON_TABLE with columns for all variable fields
              renderFilterWithMultipleFields(ctx, fieldPath.getPath(), allVarFields);
            } else {
              // Use COALESCE to return empty array instead of NULL when no matches
              ctx.sql("(SELECT COALESCE(JSON_ARRAYAGG(val), JSON_ARRAY()) FROM JSON_TABLE(");
              renderDataColumn(ctx);
              ctx.sql(", '$.");
              ctx.sql(fieldPath.getPath());
              ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')) WHERE ");
              ctx.visit(indexExpression); // condition
              ctx.sql(")");
            }
          }
        } else {
          // For non-field-path arrays (e.g., expression results), render fallback
          ctx.sql("/* $filter on expression arrays not supported */ NULL");
        }
      }
      case MAP -> {
        // Map is complex, would need to apply expression to each element
        if (arrayExpression instanceof FieldPathExpression fieldPath) {
          // Check if mapping expression uses variable field access (e.g., $$item.product)
          String varField = extractVariableField(indexExpression);
          if (varField != null) {
            // Generate JSON_TABLE with column for the extracted field
            // Use COALESCE to return empty array instead of NULL for empty input
            ctx.sql("(SELECT COALESCE(JSON_ARRAYAGG(");
            ctx.sql(varField);
            ctx.sql("), JSON_ARRAY()) FROM JSON_TABLE(");
            renderDataColumn(ctx);
            ctx.sql(", '$.");
            ctx.sql(fieldPath.getPath());
            ctx.sql("[*]' COLUMNS (");
            ctx.sql(varField);
            ctx.sql(" VARCHAR2(4000) PATH '$.");
            ctx.sql(varField);
            ctx.sql("')))");
          } else if (indexExpression instanceof InlineObjectExpression inlineObj) {
            // Handle $map with object transformation
            // Extract all variable field references from the object
            java.util.Set<String> varFields = new java.util.LinkedHashSet<>();
            collectVariableFieldsFromObject(inlineObj, varFields);

            if (!varFields.isEmpty()) {
              renderMapWithObjectTransformation(ctx, fieldPath.getPath(), inlineObj, varFields);
            } else {
              // Fallback for objects without variable fields
              ctx.sql("(SELECT COALESCE(JSON_ARRAYAGG(");
              ctx.visit(indexExpression);
              ctx.sql("), JSON_ARRAY()) FROM JSON_TABLE(");
              renderDataColumn(ctx);
              ctx.sql(", '$.");
              ctx.sql(fieldPath.getPath());
              ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
            }
          } else {
            // Use COALESCE to return empty array instead of NULL for empty input
            ctx.sql("(SELECT COALESCE(JSON_ARRAYAGG(");
            ctx.visit(indexExpression); // mapping expression
            ctx.sql("), JSON_ARRAY()) FROM JSON_TABLE(");
            renderDataColumn(ctx);
            ctx.sql(", '$.");
            ctx.sql(fieldPath.getPath());
            ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
          }
        } else {
          // For non-field-path arrays (e.g., expression results), render fallback
          ctx.sql("/* $map on expression arrays not supported */ NULL");
        }
      }
      case REDUCE -> {
        // $reduce: {input: <array>, initialValue: <expr>, in: <expr>}
        // - arrayExpression = input array
        // - indexExpression = initialValue
        // - arguments.get(0) = in expression
        renderReduceOperation(ctx);
      }
      default -> throw new IllegalStateException("Unexpected complex array operator: " + op);
    }
  }

  /**
   * Renders the $reduce operation by detecting common reduction patterns and translating to SQL
   * aggregates. Supports sum pattern (ADD -> SUM) and concat pattern (CONCAT -> LISTAGG).
   */
  private void renderReduceOperation(SqlGenerationContext ctx) {
    // Get the "in" expression from additionalArgs
    Expression inExpr =
        (additionalArgs != null && !additionalArgs.isEmpty()) ? additionalArgs.get(0) : null;

    // Check if input is a field path (most common case)
    if (!(arrayExpression instanceof FieldPathExpression fieldPath)) {
      // For non-field-path arrays, render a fallback
      ctx.sql("/* $reduce on expression arrays not supported */ NULL");
      return;
    }

    String path = fieldPath.getPath();

    // Detect sum pattern: {$add: ["$$value", "$$this"]}
    if (inExpr instanceof ArithmeticExpression arithExpr
        && arithExpr.getOp() == ArithmeticOp.ADD
        && isValueAndThisPattern(arithExpr.getOperands())) {
      // Translate to SUM aggregate
      ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), ");
      ctx.visit(indexExpression); // initialValue
      ctx.sql(") FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(path);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
      return;
    }

    // Detect sum pattern with nested field: {$add: ["$$value", "$$this.field"]}
    if (inExpr instanceof ArithmeticExpression arithExpr
        && arithExpr.getOp() == ArithmeticOp.ADD) {
      String nestedField = extractThisFieldPattern(arithExpr.getOperands());
      if (nestedField != null) {
        // Translate to SUM aggregate with nested field path
        ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), ");
        ctx.visit(indexExpression); // initialValue
        ctx.sql(") FROM JSON_TABLE(");
        renderDataColumn(ctx);
        ctx.sql(", '$.");
        ctx.sql(path);
        ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$.");
        ctx.sql(nestedField);
        ctx.sql("')))");
        return;
      }

      // Detect sum with nested arithmetic: {$add: ["$$value", {$multiply: ["$$this.qty", ...]}]}
      Expression sumExpr = extractNestedArithmeticPattern(arithExpr.getOperands());
      if (sumExpr != null) {
        // Extract all $$this.field references from the expression
        java.util.Set<String> thisFields = new java.util.LinkedHashSet<>();
        collectThisFieldReferences(sumExpr, thisFields);

        if (!thisFields.isEmpty()) {
          // Generate SUM with nested arithmetic expression
          ctx.sql("(SELECT NVL(SUM(");
          renderReduceArithmeticExpression(sumExpr, ctx);
          ctx.sql("), ");
          ctx.visit(indexExpression); // initialValue
          ctx.sql(") FROM JSON_TABLE(");
          renderDataColumn(ctx);
          ctx.sql(", '$.");
          ctx.sql(path);
          ctx.sql("[*]' COLUMNS (");

          // Generate columns for each referenced field
          boolean first = true;
          for (String field : thisFields) {
            if (!first) {
              ctx.sql(", ");
            }
            ctx.sql(field);
            ctx.sql(" NUMBER PATH '$.");
            ctx.sql(field);
            ctx.sql("'");
            first = false;
          }

          ctx.sql(")))");
          return;
        }
      }
    }

    // Detect concat pattern: {$concat: ["$$value", "$$this"]}
    if (inExpr instanceof StringExpression strExpr
        && strExpr.getOp() == StringOp.CONCAT
        && isValueAndThisPattern(strExpr.getArguments())) {
      // Translate to LISTAGG with JSON type conversion for empty arrays.
      // LISTAGG returns NULL for empty input; MongoDB returns initialValue ''.
      // Oracle treats '' as NULL, so we use JSON('""') to output empty string.
      ctx.sql("(SELECT CASE WHEN listagg_result IS NULL THEN JSON('\"\"') ");
      ctx.sql("ELSE JSON('\"' || listagg_result || '\"') END FROM (");
      ctx.sql("SELECT LISTAGG(val, '') WITHIN GROUP (ORDER BY ROWNUM) AS listagg_result ");
      ctx.sql("FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(path);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))))");
      return;
    }

    // Detect tier selection pattern: {$cond: [condition, "$$this", "$$value"]}
    // This pattern finds the first array element matching a condition
    if (inExpr instanceof ConditionalExpression condExpr
        && condExpr.getType() == ConditionalExpression.ConditionalType.COND) {
      Expression thenExpr = condExpr.getThenExpr();
      Expression elseExpr = condExpr.getElseExpr();

      // Check for pattern: $cond[condition, $$this, $$value]
      if (isThisReference(thenExpr) && isValueReference(elseExpr)) {
        renderFindFirstMatchingPattern(ctx, path, condExpr.getCondition());
        return;
      }
    }

    // Detect object construction pattern with conditional sums:
    // {$reduce: {input: ..., initialValue: {a: 0, b: 0}, in: {a: {$add: [...]}, ...}}}
    if (inExpr instanceof InlineObjectExpression objExpr) {
      if (renderReduceObjectConstruction(ctx, path, objExpr)) {
        return;
      }
    }

    // Detect $setUnion flatten pattern: {$reduce: {in: {$setUnion: ["$$value", "$$this"]}}}
    if (inExpr instanceof ArrayExpression arrayInExpr
        && arrayInExpr.getOp() == ArrayOp.SET_UNION) {
      List<Expression> args = arrayInExpr.getAdditionalArgs();
      if (args != null && args.size() >= 2
          && isValueReference(args.get(0)) && isThisReference(args.get(1))) {
        // Flatten nested arrays with deduplication
        renderSetUnionFlatten(ctx, path);
        return;
      }
    }

    // For other patterns, render a descriptive placeholder
    ctx.sql("/* $reduce with custom expression not supported */ NULL");
  }

  /**
   * Checks if an expression is a $$this reference.
   */
  private boolean isThisReference(Expression expr) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      return "$this".equals(exprPath) || "$$this".equals(exprPath);
    }
    return false;
  }

  /**
   * Checks if an expression is a $$value reference.
   */
  private boolean isValueReference(Expression expr) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      return "$value".equals(exprPath) || "$$value".equals(exprPath);
    }
    return false;
  }

  /**
   * Renders the "find first matching" pattern for $reduce.
   * Pattern: {$reduce: {in: {$cond: [condition, $$this, $$value]}}}
   * Translates to: (SELECT tier_data FROM JSON_TABLE(...) WHERE condition FETCH FIRST 1 ROW ONLY)
   */
  private void renderFindFirstMatchingPattern(
      SqlGenerationContext ctx, String arrayPath, Expression condition) {
    // Collect all $$this.field references used in the condition
    java.util.Set<String> thisFields = new java.util.LinkedHashSet<>();
    collectThisFieldReferencesFromCondition(condition, thisFields);

    // Collect outer document field references (non-$$this fields)
    java.util.Set<String> outerFields = new java.util.LinkedHashSet<>();
    collectOuterFieldReferences(condition, outerFields);

    ctx.sql("(SELECT jt.tier_data FROM JSON_TABLE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(arrayPath);
    ctx.sql("[*]' COLUMNS (");

    // Generate columns for $$this fields
    boolean first = true;
    for (String field : thisFields) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.sql("jt_");
      ctx.sql(field);
      ctx.sql(" NUMBER PATH '$.");
      ctx.sql(field);
      ctx.sql("'");
      first = false;
    }

    // Add the full element as JSON for return
    if (!first) {
      ctx.sql(", ");
    }
    ctx.sql("tier_data JSON PATH '$'");
    ctx.sql(")) jt WHERE ");

    // Render the condition with proper field references
    renderReduceCondition(ctx, condition, outerFields);
    ctx.sql(" FETCH FIRST 1 ROW ONLY)");
  }

  /**
   * Collects $$this.field references from a condition expression.
   */
  private void collectThisFieldReferencesFromCondition(
      Expression expr, java.util.Set<String> fields) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && exprPath.startsWith("$this.")) {
        fields.add(exprPath.substring(6));
      }
    } else if (expr instanceof LogicalExpression logical) {
      for (Expression operand : logical.getOperands()) {
        collectThisFieldReferencesFromCondition(operand, fields);
      }
    } else if (expr instanceof ComparisonExpression comp) {
      collectThisFieldReferencesFromCondition(comp.getLeft(), fields);
      collectThisFieldReferencesFromCondition(comp.getRight(), fields);
    }
  }

  /**
   * Collects outer document field references (non-$$this, non-$$value fields).
   */
  private void collectOuterFieldReferences(Expression expr, java.util.Set<String> fields) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && !exprPath.startsWith("$this") && !exprPath.startsWith("$value")) {
        fields.add(exprPath);
      }
    } else if (expr instanceof LogicalExpression logical) {
      for (Expression operand : logical.getOperands()) {
        collectOuterFieldReferences(operand, fields);
      }
    } else if (expr instanceof ComparisonExpression comp) {
      collectOuterFieldReferences(comp.getLeft(), fields);
      collectOuterFieldReferences(comp.getRight(), fields);
    }
  }

  /**
   * Renders a condition expression for use in $reduce WHERE clause.
   * Translates $$this.field to jt_field and outer fields to q."DATA".field.
   */
  private void renderReduceCondition(
      SqlGenerationContext ctx, Expression condition, java.util.Set<String> outerFields) {
    if (condition instanceof LogicalExpression logical) {
      LogicalOp op = logical.getOp();
      List<Expression> operands = logical.getOperands();

      if (op == LogicalOp.AND) {
        ctx.sql("(");
        for (int i = 0; i < operands.size(); i++) {
          if (i > 0) {
            ctx.sql(" AND ");
          }
          renderReduceCondition(ctx, operands.get(i), outerFields);
        }
        ctx.sql(")");
      } else if (op == LogicalOp.OR) {
        ctx.sql("(");
        for (int i = 0; i < operands.size(); i++) {
          if (i > 0) {
            ctx.sql(" OR ");
          }
          renderReduceCondition(ctx, operands.get(i), outerFields);
        }
        ctx.sql(")");
      }
    } else if (condition instanceof ComparisonExpression comp) {
      renderReduceComparisonOperand(ctx, comp.getLeft(), outerFields);
      ctx.sql(" ");
      ctx.sql(getSqlComparisonOp(comp.getOp()));
      ctx.sql(" ");
      renderReduceComparisonOperand(ctx, comp.getRight(), outerFields);
    }
  }

  /**
   * Renders a comparison operand for $reduce condition.
   */
  private void renderReduceComparisonOperand(
      SqlGenerationContext ctx, Expression expr, java.util.Set<String> outerFields) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && exprPath.startsWith("$this.")) {
        // $$this.field -> jt_field column reference
        ctx.sql("jt.jt_");
        ctx.sql(exprPath.substring(6));
      } else if (exprPath != null && outerFields.contains(exprPath)) {
        // Outer document field -> q."DATA".field
        ctx.sql("q.\"DATA\".");
        ctx.sql(exprPath);
      } else {
        // Other field reference
        ctx.sql("q.\"DATA\".");
        ctx.sql(exprPath != null ? exprPath : "");
      }
    } else if (expr instanceof LiteralExpression lit) {
      Object value = lit.getValue();
      if (value instanceof String) {
        ctx.sql("'");
        ctx.sql(((String) value).replace("'", "''"));
        ctx.sql("'");
      } else if (value instanceof Number) {
        ctx.sql(String.valueOf(value));
      } else if (value == null) {
        ctx.sql("NULL");
      } else {
        ctx.sql(String.valueOf(value));
      }
    }
  }

  /**
   * Gets the SQL comparison operator for a ComparisonOp.
   */
  private String getSqlComparisonOp(ComparisonOp op) {
    return switch (op) {
      case EQ -> "=";
      case NE -> "!=";
      case GT -> ">";
      case GTE -> ">=";
      case LT -> "<";
      case LTE -> "<=";
      default -> "=";
    };
  }

  /**
   * Renders the object construction pattern for $reduce.
   * Pattern: {in: {field1: {$add: [$$value.field1, ...]}, field2: ...}}
   * Returns true if successfully rendered.
   */
  private boolean renderReduceObjectConstruction(
      SqlGenerationContext ctx, String arrayPath, InlineObjectExpression objExpr) {
    java.util.Map<String, Expression> fields = objExpr.getFields();

    // Check if all fields follow the pattern {$add: ["$$value.field", conditional]}
    java.util.List<String> fieldNames = new java.util.ArrayList<>(fields.keySet());
    java.util.List<Expression> conditions = new java.util.ArrayList<>();

    for (String fieldName : fieldNames) {
      Expression fieldExpr = fields.get(fieldName);
      if (!(fieldExpr instanceof ArithmeticExpression arith)
          || arith.getOp() != ArithmeticOp.ADD) {
        return false;
      }

      List<Expression> operands = arith.getOperands();
      if (operands.size() != 2) {
        return false;
      }

      // Check for $$value.fieldName pattern
      Expression valueRef = operands.get(0);
      if (!(valueRef instanceof FieldPathExpression fp)
          || !("$value." + fieldName).equals(fp.getPath())) {
        return false;
      }

      conditions.add(operands.get(1));
    }

    // All fields match the pattern - generate aggregate subquery
    ctx.sql("JSON_OBJECT(");
    for (int i = 0; i < fieldNames.size(); i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      final String fieldName = fieldNames.get(i);
      final Expression condExpr = conditions.get(i);

      ctx.sql("'");
      ctx.sql(fieldName);
      ctx.sql("' VALUE (SELECT NVL(SUM(");

      // Render the conditional sum expression
      renderReduceConditionalSum(ctx, condExpr, arrayPath);

      ctx.sql("), 0) FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(arrayPath);
      ctx.sql("[*]' COLUMNS (");
      // Add columns needed for the condition
      renderReduceJsonTableColumns(ctx, condExpr);
      ctx.sql(")))");
    }
    ctx.sql(")");
    return true;
  }

  /**
   * Renders a conditional sum expression for $reduce object construction.
   */
  private void renderReduceConditionalSum(
      SqlGenerationContext ctx, Expression expr, String arrayPath) {
    if (expr instanceof ConditionalExpression condExpr
        && condExpr.getType() == ConditionalExpression.ConditionalType.COND) {
      // {$cond: [condition, thenValue, elseValue]}
      ctx.sql("CASE WHEN ");
      renderReduceConditionSimple(ctx, condExpr.getCondition());
      ctx.sql(" THEN ");
      renderReduceValue(ctx, condExpr.getThenExpr());
      ctx.sql(" ELSE ");
      renderReduceValue(ctx, condExpr.getElseExpr());
      ctx.sql(" END");
    } else if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && exprPath.startsWith("$this.")) {
        ctx.sql("jt_");
        ctx.sql(exprPath.substring(6));
      }
    } else if (expr instanceof LiteralExpression lit) {
      ctx.sql(String.valueOf(lit.getValue()));
    }
  }

  /**
   * Renders a simple condition for $reduce CASE WHEN.
   */
  private void renderReduceConditionSimple(SqlGenerationContext ctx, Expression condition) {
    if (condition instanceof ComparisonExpression comp) {
      renderReduceValueSimple(ctx, comp.getLeft());
      ctx.sql(" ");
      ctx.sql(getSqlComparisonOp(comp.getOp()));
      ctx.sql(" ");
      renderReduceValueSimple(ctx, comp.getRight());
    } else if (condition instanceof LogicalExpression) {
      renderReduceCondition(ctx, condition, java.util.Collections.emptySet());
    }
  }

  /**
   * Renders a simple value reference.
   */
  private void renderReduceValueSimple(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && exprPath.startsWith("$this.")) {
        ctx.sql("jt_");
        ctx.sql(exprPath.substring(6));
      }
    } else if (expr instanceof LiteralExpression lit) {
      Object value = lit.getValue();
      if (value instanceof String) {
        ctx.sql("'");
        ctx.sql(((String) value).replace("'", "''"));
        ctx.sql("'");
      } else {
        ctx.sql(String.valueOf(value));
      }
    }
  }

  /**
   * Renders a value expression for $reduce.
   */
  private void renderReduceValue(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && exprPath.startsWith("$this.")) {
        ctx.sql("jt_");
        ctx.sql(exprPath.substring(6));
      }
    } else if (expr instanceof LiteralExpression lit) {
      ctx.sql(String.valueOf(lit.getValue()));
    }
  }

  /**
   * Renders JSON_TABLE columns needed for a $reduce conditional expression.
   * Tracks which fields are used in sum context to use NUMBER type.
   */
  private void renderReduceJsonTableColumns(SqlGenerationContext ctx, Expression expr) {
    java.util.Set<String> fields = new java.util.LinkedHashSet<>();
    java.util.Set<String> numericFields = new java.util.LinkedHashSet<>();
    collectThisFieldReferencesFromExpression(expr, fields);
    collectNumericThisFieldReferences(expr, numericFields);

    boolean first = true;
    for (String field : fields) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.sql("jt_");
      ctx.sql(field);
      // Use NUMBER for fields that will be summed, VARCHAR2 for others
      if (numericFields.contains(field)) {
        ctx.sql(" NUMBER PATH '$.");
      } else {
        ctx.sql(" VARCHAR2(4000) PATH '$.");
      }
      ctx.sql(field);
      ctx.sql("'");
      first = false;
    }
  }

  /**
   * Collects $$this.field references that are used in numeric contexts (sum, add, etc.).
   */
  private void collectNumericThisFieldReferences(Expression expr, java.util.Set<String> fields) {
    if (expr instanceof ConditionalExpression cond) {
      // The then/else of a $cond in numeric context are likely numeric
      Expression thenExpr = cond.getThenExpr();
      Expression elseExpr = cond.getElseExpr();
      // If else is a numeric literal, then the then is also numeric
      if (elseExpr instanceof LiteralExpression lit && lit.getValue() instanceof Number) {
        if (thenExpr instanceof FieldPathExpression fp) {
          String path = fp.getPath();
          if (path != null && path.startsWith("$this.")) {
            fields.add(path.substring(6));
          }
        }
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      for (Expression operand : arith.getOperands()) {
        collectNumericThisFieldReferences(operand, fields);
        // Also collect direct $$this.field references in arithmetic context
        if (operand instanceof FieldPathExpression fp) {
          String path = fp.getPath();
          if (path != null && path.startsWith("$this.")) {
            fields.add(path.substring(6));
          }
        }
      }
    }
  }

  /**
   * Collects $$this.field references from any expression.
   */
  private void collectThisFieldReferencesFromExpression(
      Expression expr, java.util.Set<String> fields) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && exprPath.startsWith("$this.")) {
        fields.add(exprPath.substring(6));
      }
    } else if (expr instanceof ConditionalExpression cond) {
      collectThisFieldReferencesFromExpression(cond.getCondition(), fields);
      collectThisFieldReferencesFromExpression(cond.getThenExpr(), fields);
      collectThisFieldReferencesFromExpression(cond.getElseExpr(), fields);
    } else if (expr instanceof ComparisonExpression comp) {
      collectThisFieldReferencesFromExpression(comp.getLeft(), fields);
      collectThisFieldReferencesFromExpression(comp.getRight(), fields);
    } else if (expr instanceof LogicalExpression logical) {
      for (Expression operand : logical.getOperands()) {
        collectThisFieldReferencesFromExpression(operand, fields);
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      for (Expression operand : arith.getOperands()) {
        collectThisFieldReferencesFromExpression(operand, fields);
      }
    }
  }

  /**
   * Renders the $setUnion flatten pattern for $reduce.
   * Pattern: {$reduce: {in: {$setUnion: ["$$value", "$$this"]}}}
   * Flattens nested arrays with deduplication.
   * Note: Oracle doesn't allow DISTINCT with JSON_ARRAYAGG, so we use a nested subquery.
   */
  private void renderSetUnionFlatten(SqlGenerationContext ctx, String arrayPath) {
    // Use nested subquery with DISTINCT, then aggregate unique values
    // JSON_ARRAYAGG doesn't support DISTINCT, so we must use subquery approach
    ctx.sql("(SELECT JSON_ARRAYAGG(val RETURNING JSON) FROM (");
    ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(arrayPath);
    ctx.sql("[*][*]' COLUMNS (val JSON PATH '$'))))");
  }

  /**
   * Extracts the non-$$value operand from an ADD expression pattern like
   * {$add: ["$$value", expr]}. Returns the expression to be summed, or null
   * if not a valid pattern.
   */
  private Expression extractNestedArithmeticPattern(List<Expression> operands) {
    if (operands == null || operands.size() != 2) {
      return null;
    }

    Expression valueExpr = null;
    Expression otherExpr = null;

    for (Expression expr : operands) {
      if (expr instanceof FieldPathExpression fp && "$value".equals(fp.getPath())) {
        valueExpr = expr;
      } else {
        otherExpr = expr;
      }
    }

    // Only return if we have both $$value and another expression (not just a simple field)
    if (valueExpr != null && otherExpr != null && otherExpr instanceof ArithmeticExpression) {
      return otherExpr;
    }
    return null;
  }

  /**
   * Recursively collects all $$this.field references from an expression into the provided set.
   * Field paths like "$this.qty" get added as "qty".
   */
  private void collectThisFieldReferences(Expression expr, java.util.Set<String> fields) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && exprPath.startsWith("$this.")) {
        fields.add(exprPath.substring(6)); // Remove "$this."
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      for (Expression operand : arith.getOperands()) {
        collectThisFieldReferences(operand, fields);
      }
    } else if (expr instanceof ComparisonExpression comp) {
      collectThisFieldReferences(comp.getLeft(), fields);
      collectThisFieldReferences(comp.getRight(), fields);
    }
    // Add other expression types as needed
  }

  /**
   * Renders an arithmetic expression for use inside a $reduce SUM aggregate.
   * Substitutes $$this.field references with direct column references from JSON_TABLE.
   */
  private void renderReduceArithmeticExpression(Expression expr, SqlGenerationContext ctx) {
    if (expr instanceof FieldPathExpression fp) {
      String exprPath = fp.getPath();
      if (exprPath != null && exprPath.startsWith("$this.")) {
        // Render as direct column reference
        ctx.sql(exprPath.substring(6));
      } else {
        // Fallback: render as-is
        ctx.visit(expr);
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      List<Expression> operands = arith.getOperands();
      String sqlOp = getSqlArithmeticOp(arith.getOp());

      ctx.sql("(");
      for (int i = 0; i < operands.size(); i++) {
        if (i > 0) {
          ctx.sql(" ");
          ctx.sql(sqlOp);
          ctx.sql(" ");
        }
        renderReduceArithmeticExpression(operands.get(i), ctx);
      }
      ctx.sql(")");
    } else if (expr instanceof LiteralExpression lit) {
      ctx.visit(lit);
    } else {
      // Fallback for other expressions
      ctx.visit(expr);
    }
  }

  /** Returns the SQL operator for an arithmetic operation. */
  private String getSqlArithmeticOp(ArithmeticOp op) {
    return switch (op) {
      case ADD -> "+";
      case SUBTRACT -> "-";
      case MULTIPLY -> "*";
      case DIVIDE -> "/";
      case MOD -> "MOD";
      case FLOOR, CEIL, ABS, ROUND, TRUNC, SQRT, EXP, LN, LOG10, POW, MAX, MIN ->
          throw new IllegalStateException("Unary op in multi-operand context: " + op);
    };
  }

  /**
   * Checks if the operands list contains references to $$value and $$this (in either order). These
   * are represented as FieldPathExpression with paths "$value" and "$this".
   */
  private boolean isValueAndThisPattern(List<Expression> operands) {
    if (operands == null || operands.size() != 2) {
      return false;
    }
    boolean hasValue = false;
    boolean hasThis = false;
    for (Expression expr : operands) {
      if (expr instanceof FieldPathExpression fp) {
        String exprPath = fp.getPath();
        if ("$value".equals(exprPath)) {
          hasValue = true;
        } else if ("$this".equals(exprPath)) {
          hasThis = true;
        }
      }
    }
    return hasValue && hasThis;
  }

  /**
   * Checks if the operands list contains references to $$value and $$this.field pattern (in either
   * order). Returns the nested field path (e.g., "price" from "$$this.price") or null if not
   * matched. These are represented as FieldPathExpression with paths "$value" and "$this.field".
   */
  private String extractThisFieldPattern(List<Expression> operands) {
    if (operands == null || operands.size() != 2) {
      return null;
    }
    boolean hasValue = false;
    String thisFieldPath = null;
    for (Expression expr : operands) {
      if (expr instanceof FieldPathExpression fp) {
        String exprPath = fp.getPath();
        if ("$value".equals(exprPath)) {
          hasValue = true;
        } else if (exprPath != null && exprPath.startsWith("$this.")) {
          // Extract the field path after "$this."
          thisFieldPath = exprPath.substring(6); // Remove "$this."
        }
      }
    }
    return (hasValue && thisFieldPath != null) ? thisFieldPath : null;
  }

  /**
   * Extracts variable field access pattern from a condition expression. For example, if the
   * condition is {$gt: ["$$item.price", 100]}, returns "price". Handles common variable names like
   * $item, $elem, etc. Returns null if no variable field access is found.
   */
  private String extractVariableFieldFromCondition(Expression condition) {
    if (condition instanceof ComparisonExpression compExpr) {
      // Check left operand for variable field access
      String field = extractVariableField(compExpr.getLeft());
      if (field != null) {
        return field;
      }
      // Check right operand
      return extractVariableField(compExpr.getRight());
    }
    return null;
  }

  /**
   * Extracts field name from a variable field access like $$item.price. Returns the field name
   * ("price") or null if not a variable field access.
   */
  private String extractVariableField(Expression expr) {
    if (expr instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      // Check for patterns like $item.field, $elem.field, $this.field
      if (path != null && path.startsWith("$") && path.contains(".")) {
        int dotIndex = path.indexOf('.');
        return path.substring(dotIndex + 1);
      }
    }
    return null;
  }

  /**
   * Renders a condition expression, substituting variable field references with the column name.
   * For example, {$gt: ["$$item.price", 100]} becomes "price > 100".
   */
  private void renderConditionWithFieldSubstitution(
      SqlGenerationContext ctx, Expression condition, String fieldName) {
    if (condition instanceof ComparisonExpression compExpr) {
      // Check if left side has the variable field reference
      final String leftField = extractVariableField(compExpr.getLeft());
      if (leftField != null) {
        // Left side is the variable reference, render as column name
        ctx.sql(fieldName);
      } else {
        ctx.visit(compExpr.getLeft());
      }

      // Render the comparison operator
      ctx.sql(" ");
      ctx.sql(compExpr.getOp().getSqlOperator());
      ctx.sql(" ");

      // Check if right side has the variable field reference
      final String rightField = extractVariableField(compExpr.getRight());
      if (rightField != null) {
        // Right side is the variable reference, render as column name
        ctx.sql(fieldName);
      } else {
        ctx.visit(compExpr.getRight());
      }
    } else {
      // Fallback: just visit the expression normally
      ctx.visit(condition);
    }
  }

  /**
   * Renders a $filter with multiple variable fields in the condition. Generates JSON_TABLE
   * with columns for each variable field and a WHERE clause using column references.
   */
  private void renderFilterWithMultipleFields(
      SqlGenerationContext ctx, String arrayPath, java.util.Set<String> varFields) {
    ctx.sql("(SELECT COALESCE(JSON_ARRAYAGG(val FORMAT JSON), JSON_ARRAY())");
    ctx.sql(" FROM JSON_TABLE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(arrayPath);
    ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$'");

    // Add columns for each variable field
    for (String field : varFields) {
      ctx.sql(", ");
      ctx.sql(field);
      ctx.sql(" NUMBER PATH '$.");
      ctx.sql(field);
      ctx.sql("'");
    }
    ctx.sql(")) WHERE ");

    // Render the condition with column substitution
    renderFilterConditionWithColumnSubstitution(ctx, indexExpression, varFields);
    ctx.sql(")");
  }

  /**
   * Renders a filter condition expression, substituting variable field references with column
   * names. Handles nested expressions like:
   * {@code {$gte: [{$multiply: ["$item.qty", "$item.price"]}, 100]}}.
   */
  private void renderFilterConditionWithColumnSubstitution(
      SqlGenerationContext ctx, Expression condition, java.util.Set<String> varFields) {
    if (condition instanceof ComparisonExpression compExpr) {
      // Render left side
      renderFilterExpressionWithColumnSubstitution(ctx, compExpr.getLeft(), varFields);
      // Render operator
      ctx.sql(" ");
      ctx.sql(compExpr.getOp().getSqlOperator());
      ctx.sql(" ");
      // Render right side
      renderFilterExpressionWithColumnSubstitution(ctx, compExpr.getRight(), varFields);
    } else {
      // Fallback: render with column substitution
      renderFilterExpressionWithColumnSubstitution(ctx, condition, varFields);
    }
  }

  /**
   * Renders an expression within a $filter condition, substituting variable field references.
   */
  private void renderFilterExpressionWithColumnSubstitution(
      SqlGenerationContext ctx, Expression expr, java.util.Set<String> varFields) {
    if (expr instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      if (path != null && path.startsWith("$") && path.contains(".")) {
        int dotIndex = path.indexOf('.');
        String fieldName = path.substring(dotIndex + 1);
        // Use the column reference
        ctx.sql(fieldName);
        return;
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      List<Expression> operands = arith.getOperands();
      String sqlOp = getSqlArithmeticOp(arith.getOp());
      ctx.sql("(");
      for (int i = 0; i < operands.size(); i++) {
        if (i > 0) {
          ctx.sql(" ");
          ctx.sql(sqlOp);
          ctx.sql(" ");
        }
        renderFilterExpressionWithColumnSubstitution(ctx, operands.get(i), varFields);
      }
      ctx.sql(")");
      return;
    } else if (expr instanceof LiteralExpression) {
      ctx.visit(expr);
      return;
    }
    // Fallback: use default rendering
    ctx.visit(expr);
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
    // FORMAT JSON is required to preserve JSON structure when slicing arrays of objects
    ctx.sql("(SELECT JSON_ARRAYAGG(val FORMAT JSON ORDER BY rn) FROM ");
    ctx.sql("(SELECT val, rn FROM JSON_TABLE(");
    ctx.visit(arrayExpression);
    ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$', rn FOR ORDINALITY))");

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

  /**
   * Renders $reverseArray operator. MongoDB: {$reverseArray: "$items"} Oracle: Uses JSON_ARRAYAGG
   * with ORDER BY DESC to reverse element order.
   */
  private void renderReverseArray(SqlGenerationContext ctx) {
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      final String path = fieldPath.getPath();
      ctx.sql("(SELECT JSON_ARRAYAGG(val ORDER BY rn DESC) FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(path);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$', rn FOR ORDINALITY)))");
    } else {
      // For non-field expressions
      ctx.sql("(SELECT JSON_ARRAYAGG(val ORDER BY rn DESC) FROM JSON_TABLE(");
      ctx.visit(arrayExpression);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$', rn FOR ORDINALITY)))");
    }
  }

  /**
   * Renders $sortArray operator. MongoDB: {$sortArray: {input: "$scores", sortBy: 1}} or
   * {$sortArray: {input: "$products", sortBy: {totalRevenue: -1}}} Oracle: Uses JSON_ARRAYAGG with
   * ORDER BY to sort elements.
   */
  private void renderSortArray(SqlGenerationContext ctx) {
    boolean ascending = true;
    if (indexExpression instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
      ascending = num.intValue() >= 0;
    }

    // Field-based sorting: sortBy is a document like {totalRevenue: -1}
    if (sortField != null) {
      renderSortArrayByField(ctx, ascending);
      return;
    }

    // Simple value-based sorting: sortBy is 1 or -1
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      final String path = fieldPath.getPath();
      ctx.sql("(SELECT JSON_ARRAYAGG(val ORDER BY val ");
      ctx.sql(ascending ? "ASC" : "DESC");
      ctx.sql(") FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(path);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
    } else {
      ctx.sql("(SELECT JSON_ARRAYAGG(val ORDER BY val ");
      ctx.sql(ascending ? "ASC" : "DESC");
      ctx.sql(") FROM JSON_TABLE(");
      ctx.visit(arrayExpression);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
    }
  }

  /**
   * Renders $sortArray with field-based sorting. MongoDB: {$sortArray: {input: "$products", sortBy:
   * {totalRevenue: -1}}} Oracle: (SELECT JSON_ARRAYAGG(val FORMAT JSON ORDER BY sortCol DESC) FROM
   * JSON_TABLE(data, '$.products[*]' COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$', sortCol
   * NUMBER PATH '$.totalRevenue')))
   *
   * <p>In CTE context, field references are plain column names (not data.field), so we use '$[*]'
   * as the JSON path since the column itself IS the array.
   */
  private void renderSortArrayByField(SqlGenerationContext ctx, boolean ascending) {
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      final String path = fieldPath.getPath();
      ctx.sql("(SELECT JSON_ARRAYAGG(val FORMAT JSON ORDER BY ");
      ctx.sql(sortField);
      ctx.sql(" ");
      ctx.sql(ascending ? "ASC" : "DESC");
      ctx.sql(") FROM JSON_TABLE(");

      if (ctx.usesCteDataColumn()) {
        // New CTE mode: access the array from the DATA column
        ctx.sql("JSON_QUERY(\"DATA\", '$." + path + "')");
        ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$', ");
      } else if (ctx.isInCteContext()) {
        // Legacy CTE mode: the path refers to a column (e.g., $products -> products)
        ctx.sql(path);
        ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$', ");
      } else {
        // Normal context: access via data.field
        renderDataColumn(ctx);
        ctx.sql(", '$.");
        ctx.sql(path);
        ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$', ");
      }
      ctx.sql(sortField);
      ctx.sql(" NUMBER PATH '$.");
      ctx.sql(sortField);
      ctx.sql("')))");
    } else {
      ctx.sql("(SELECT JSON_ARRAYAGG(val FORMAT JSON ORDER BY ");
      ctx.sql(sortField);
      ctx.sql(" ");
      ctx.sql(ascending ? "ASC" : "DESC");
      ctx.sql(") FROM JSON_TABLE(");
      ctx.visit(arrayExpression);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) FORMAT JSON PATH '$', ");
      ctx.sql(sortField);
      ctx.sql(" NUMBER PATH '$.");
      ctx.sql(sortField);
      ctx.sql("')))");
    }
  }

  /**
   * Renders $in operator. MongoDB: {$in: [value, "$array"]} - returns true if value is in array
   * Oracle: Uses JSON_EXISTS to check for element presence.
   */
  private void renderIn(SqlGenerationContext ctx) {
    // indexExpression contains the value to search for
    // arrayExpression contains the array to search in
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      final String path = fieldPath.getPath();
      ctx.sql("JSON_EXISTS(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(path);
      ctx.sql("[*]?(@ == ");

      if (indexExpression instanceof LiteralExpression lit) {
        Object val = lit.getValue();
        if (val instanceof String s) {
          ctx.sql("\"");
          ctx.sql(s.replace("\"", "\\\""));
          ctx.sql("\"");
        } else if (val instanceof Number) {
          ctx.sql(val.toString());
        } else if (val instanceof Boolean) {
          ctx.sql(val.toString());
        } else {
          ctx.sql("null");
        }
      } else if (indexExpression instanceof FieldPathExpression valueField) {
        // Compare with another field - use variable binding
        ctx.sql("$val)' PASSING JSON_VALUE(");
        renderDataColumn(ctx);
        ctx.sql(", '$.");
        ctx.sql(valueField.getPath());
        ctx.sql("') AS \"val\"");
        ctx.sql(")");
        return;
      }

      ctx.sql(")')");
    } else if (arrayExpression instanceof LiteralExpression litArray
        && litArray.getValue() instanceof List<?> list) {
      // Array is a literal array - use SQL IN operator for efficiency
      // {"$in": ["$status", ["delivered", "shipped"]]} -> status IN ('delivered', 'shipped')
      if (indexExpression instanceof FieldPathExpression valueField) {
        ctx.visit(valueField);
        ctx.sql(" IN (");
        boolean first = true;
        for (Object item : list) {
          if (!first) {
            ctx.sql(", ");
          }
          if (item instanceof String s) {
            ctx.sql("'");
            ctx.sql(s.replace("'", "''"));
            ctx.sql("'");
          } else if (item instanceof Number) {
            ctx.sql(item.toString());
          } else if (item instanceof Boolean) {
            ctx.sql(item.toString());
          } else {
            ctx.sql("NULL");
          }
          first = false;
        }
        ctx.sql(")");
      } else {
        // Value is a literal - use JSON_EXISTS
        ctx.sql("JSON_EXISTS('");
        ctx.sql(toJsonArray(list));
        ctx.sql("', '$[*]?(@ == ");
        if (indexExpression instanceof LiteralExpression lit) {
          Object val = lit.getValue();
          if (val instanceof String s) {
            ctx.sql("\"");
            ctx.sql(s.replace("\"", "\\\""));
            ctx.sql("\"");
          } else {
            ctx.sql(val != null ? val.toString() : "null");
          }
        }
        ctx.sql(")')");
      }
    } else {
      // Array is some other expression
      ctx.sql("JSON_EXISTS(");
      ctx.visit(arrayExpression);
      ctx.sql(", '$[*]?(@ == ");
      if (indexExpression instanceof LiteralExpression lit) {
        Object val = lit.getValue();
        if (val instanceof String s) {
          ctx.sql("\"");
          ctx.sql(s.replace("\"", "\\\""));
          ctx.sql("\"");
        } else {
          ctx.sql(val != null ? val.toString() : "null");
        }
      } else if (indexExpression instanceof FieldPathExpression valueField) {
        // Compare with another field - use variable binding
        ctx.sql("$val)' PASSING ");
        ctx.visit(valueField);
        ctx.sql(" AS \"val\"");
        ctx.sql(")");
        return;
      }
      ctx.sql(")')");
    }
  }

  /**
   * Renders $isArray operator. MongoDB: {$isArray: "$field"} Oracle: Uses JSON_VALUE with .type()
   * to check if the value is an array. This properly distinguishes arrays from scalars, unlike
   * JSON_EXISTS which treats scalars as single-element arrays.
   */
  private void renderIsArray(SqlGenerationContext ctx) {
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      final String path = fieldPath.getPath();
      // Use JSON_VALUE with .type() to get the actual type
      // Returns 'array' for arrays, 'object', 'string', 'number', 'boolean', 'null' for others
      // Return SQL boolean TRUE/FALSE so JSON_OBJECT serializes as JSON booleans, not strings
      ctx.sql("CASE WHEN JSON_VALUE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(path);
      ctx.sql(".type()') = 'array' THEN TRUE ELSE FALSE END");
    } else {
      // For expressions, check the type of the expression result
      ctx.sql("CASE WHEN JSON_VALUE(");
      ctx.visit(arrayExpression);
      ctx.sql(", '$.type()') = 'array' THEN TRUE ELSE FALSE END");
    }
  }

  /**
   * Renders $indexOfArray operator. MongoDB: {$indexOfArray: ["$array", value]} - returns 0-based
   * index or -1 Oracle: Uses JSON_TABLE with row position tracking.
   */
  private void renderIndexOfArray(SqlGenerationContext ctx) {
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      final String path = fieldPath.getPath();
      ctx.sql("(SELECT COALESCE(MIN(rn) - 1, -1) FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(path);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$', rn FOR ORDINALITY)) WHERE val = ");

      if (indexExpression instanceof LiteralExpression lit) {
        Object val = lit.getValue();
        if (val instanceof String s) {
          ctx.sql("'");
          ctx.sql(s.replace("'", "''"));
          ctx.sql("'");
        } else if (val instanceof Number) {
          ctx.sql(val.toString());
        } else {
          ctx.sql("NULL");
        }
      } else {
        ctx.visit(indexExpression);
      }

      // Handle optional range arguments
      if (additionalArgs != null && additionalArgs.size() >= 2) {
        if (additionalArgs.get(0) instanceof LiteralExpression startLit
            && startLit.getValue() instanceof Number startNum) {
          ctx.sql(" AND rn > ");
          ctx.sql(String.valueOf(startNum.intValue()));
        }
        if (additionalArgs.get(1) instanceof LiteralExpression endLit
            && endLit.getValue() instanceof Number endNum) {
          ctx.sql(" AND rn <= ");
          ctx.sql(String.valueOf(endNum.intValue()));
        }
      }

      ctx.sql(")");
    } else {
      // Non-field expression
      ctx.sql("(SELECT COALESCE(MIN(rn) - 1, -1) FROM JSON_TABLE(");
      ctx.visit(arrayExpression);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$', rn FOR ORDINALITY)) WHERE val = ");
      if (indexExpression instanceof LiteralExpression lit) {
        Object val = lit.getValue();
        if (val instanceof String s) {
          ctx.sql("'");
          ctx.sql(s.replace("'", "''"));
          ctx.sql("'");
        } else {
          ctx.sql(val != null ? val.toString() : "NULL");
        }
      }
      ctx.sql(")");
    }
  }

  /**
   * Renders $setUnion operator. MongoDB: {$setUnion: ["$arr1", "$arr2"]} Oracle: Uses UNION to
   * combine unique elements.
   */
  private void renderSetUnion(SqlGenerationContext ctx) {
    if (additionalArgs == null || additionalArgs.isEmpty()) {
      ctx.sql("JSON_QUERY('[]', '$')");
      return;
    }

    // Use SELECT DISTINCT with JSON_ARRAYAGG to get unique values
    // For field paths, wrap in JSON_QUERY to avoid correlation issues in scalar subqueries
    ctx.sql("(SELECT JSON_ARRAYAGG(val) FROM (SELECT DISTINCT val FROM (");
    boolean first = true;
    for (Expression arr : additionalArgs) {
      if (!first) {
        ctx.sql(" UNION ");
      }
      if (arr instanceof FieldPathExpression fieldPath) {
        // Use JSON_QUERY to extract array, avoiding correlation issues
        ctx.sql("SELECT val FROM JSON_TABLE(JSON_QUERY(");
        renderDataColumn(ctx);
        ctx.sql(", '$.");
        ctx.sql(fieldPath.getPath());
        ctx.sql("'), '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
      } else {
        ctx.sql("SELECT val FROM JSON_TABLE(");
        renderArrayExpression(ctx, arr);
        ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
      }
      first = false;
    }
    ctx.sql(")))");
  }

  /**
   * Renders $setIntersection operator. MongoDB: {$setIntersection: ["$arr1", "$arr2"]} Oracle: Uses
   * INTERSECT to find common elements.
   */
  private void renderSetIntersection(SqlGenerationContext ctx) {
    if (additionalArgs == null || additionalArgs.isEmpty()) {
      ctx.sql("JSON_QUERY('[]', '$')");
      return;
    }

    ctx.sql("(SELECT JSON_ARRAYAGG(val) FROM (");
    boolean first = true;
    for (Expression arr : additionalArgs) {
      if (!first) {
        ctx.sql(" INTERSECT ");
      }
      if (arr instanceof FieldPathExpression fieldPath) {
        ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
        renderDataColumn(ctx);
        ctx.sql(", '$.");
        ctx.sql(fieldPath.getPath());
        ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
      } else {
        ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
        renderArrayExpression(ctx, arr);
        ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
      }
      first = false;
    }
    ctx.sql("))");
  }

  /**
   * Renders an array expression for use in JSON_TABLE. Handles LiteralExpression with List values
   * by rendering them as proper JSON array strings.
   */
  @SuppressWarnings("unchecked")
  private void renderArrayExpression(SqlGenerationContext ctx, Expression arr) {
    if (arr instanceof LiteralExpression literal && literal.getValue() instanceof List) {
      // Render the list as a proper JSON array string
      ctx.sql("'");
      ctx.sql(toJsonArray((List<?>) literal.getValue()));
      ctx.sql("'");
    } else {
      ctx.visit(arr);
    }
  }

  /**
   * Renders $setDifference operator. MongoDB: {$setDifference: ["$arr1", "$arr2"]} Oracle: Uses
   * MINUS to find elements in first but not in second.
   */
  private void renderSetDifference(SqlGenerationContext ctx) {
    ctx.sql("(SELECT JSON_ARRAYAGG(val) FROM (");

    // First array
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath.getPath());
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    } else {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      ctx.visit(arrayExpression);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    }

    ctx.sql(" MINUS ");

    // Second array
    if (indexExpression instanceof FieldPathExpression fieldPath2) {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath2.getPath());
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    } else {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      ctx.visit(indexExpression);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    }

    ctx.sql("))");
  }

  /**
   * Renders $setEquals operator. MongoDB: {$setEquals: ["$arr1", "$arr2"]} Oracle: Checks if both
   * arrays have the same distinct elements.
   */
  private void renderSetEquals(SqlGenerationContext ctx) {
    if (additionalArgs == null || additionalArgs.size() < 2) {
      ctx.sql("1"); // Single or no array always equals itself
      return;
    }

    // Compare first two arrays (symmetric difference = 0)
    final Expression arr1 = additionalArgs.get(0);
    final Expression arr2 = additionalArgs.get(1);

    ctx.sql("CASE WHEN (");

    // Count of elements in arr1 not in arr2
    ctx.sql("SELECT COUNT(*) FROM (");
    renderArrayAsSelect(ctx, arr1);
    ctx.sql(" MINUS ");
    renderArrayAsSelect(ctx, arr2);
    ctx.sql(")");

    ctx.sql(") = 0 AND (");

    // Count of elements in arr2 not in arr1
    ctx.sql("SELECT COUNT(*) FROM (");
    renderArrayAsSelect(ctx, arr2);
    ctx.sql(" MINUS ");
    renderArrayAsSelect(ctx, arr1);
    ctx.sql(")");

    ctx.sql(") = 0 THEN 1 ELSE 0 END");
  }

  /**
   * Renders $setIsSubset operator. MongoDB: {$setIsSubset: ["$arr1", "$arr2"]} Oracle: Checks if
   * all elements of first array are in second.
   */
  private void renderSetIsSubset(SqlGenerationContext ctx) {
    // arr1 is subset of arr2 if (arr1 MINUS arr2) is empty
    ctx.sql("CASE WHEN (SELECT COUNT(*) FROM (");

    // Elements in arr1 not in arr2
    if (arrayExpression instanceof FieldPathExpression fieldPath) {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath.getPath());
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    } else {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      ctx.visit(arrayExpression);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    }

    ctx.sql(" MINUS ");

    if (indexExpression instanceof FieldPathExpression fieldPath2) {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath2.getPath());
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    } else {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      ctx.visit(indexExpression);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    }

    ctx.sql(")) = 0 THEN 1 ELSE 0 END");
  }

  /**
   * Renders $sum as an array expression operator. MongoDB: {$sum: "$orders.payment.amount"} - sums
   * all numeric values in the array path. Oracle: Uses JSON_TABLE with SUM aggregate.
   */
  private void renderSumArray(SqlGenerationContext ctx, String path) {
    // Normalize path by removing $ prefix if present
    String normalizedPath = path.startsWith("$") ? path.substring(1) : path;

    // Handle nested field paths like "orders.payment.amount"
    // We need to extract the array portion and the nested field path
    int firstDot = normalizedPath.indexOf('.');
    if (firstDot > 0) {
      // Nested path: split into array path and nested field
      final String arrayPath = normalizedPath.substring(0, firstDot);
      final String nestedPath = normalizedPath.substring(firstDot + 1);

      // Check if the array path is a pipeline lookup result
      String pipelineLookupAlias = ctx.getPipelineLookupAlias(arrayPath);
      if (pipelineLookupAlias != null) {
        // For pipeline lookup: sum from the JSON array column
        ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), 0) FROM JSON_TABLE(");
        ctx.sql(pipelineLookupAlias);
        ctx.sql(".");
        ctx.sql(arrayPath); // column name
        ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$.");
        ctx.sql(nestedPath);
        ctx.sql("')))");
        return;
      }

      // Check if the array path is from a simple (equality) $lookup
      SqlGenerationContext.LookupFieldInfo lookupInfo = ctx.getLookupFieldInfo(arrayPath);
      if (lookupInfo != null) {
        // For equality lookup: generate correlated subquery summing from foreign table
        // (SELECT NVL(SUM(subq.data."nested"."field"), 0) FROM foreignTable subq
        //  WHERE subq.data."foreignField" = base.data."localField")
        String subqAlias = "sum_" + arrayPath.substring(0, Math.min(3, arrayPath.length()));
        ctx.sql("(SELECT NVL(SUM(");
        ctx.sql(subqAlias);
        ctx.sql(".data.");
        ctx.sql(quotePath(nestedPath));
        ctx.sql("), 0) FROM ");
        ctx.tableName(lookupInfo.foreignTable());
        ctx.sql(" ");
        ctx.sql(subqAlias);
        ctx.sql(" WHERE ");
        ctx.sql(subqAlias);
        ctx.sql(".data.");
        ctx.sql(quotePath(lookupInfo.foreignField()));
        ctx.sql(" = ");
        String alias = ctx.getBaseTableAlias();
        if (alias != null && !alias.isEmpty()) {
          ctx.sql(alias);
          ctx.sql(".");
        }
        ctx.sql("data.");
        ctx.sql(quotePath(lookupInfo.localField()));
        ctx.sql(")");
        return;
      }

      ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), 0) FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(arrayPath);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$.");
      ctx.sql(nestedPath);
      ctx.sql("')))");
    } else {
      // Simple array path: sum all elements directly
      // Check if this is a pipeline lookup result
      String pipelineLookupAlias = ctx.getPipelineLookupAlias(normalizedPath);
      if (pipelineLookupAlias != null) {
        // For pipeline lookup: sum all elements from the JSON array column
        ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), 0) FROM JSON_TABLE(");
        ctx.sql(pipelineLookupAlias);
        ctx.sql(".");
        ctx.sql(normalizedPath); // column name
        ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
        return;
      }

      ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), 0) FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(normalizedPath);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
    }
  }

  /**
   * Renders $avg as an array expression operator. MongoDB: {$avg: "$scores"} - averages all numeric
   * values in the array. Oracle: Uses JSON_TABLE with AVG aggregate.
   */
  private void renderAvgArray(SqlGenerationContext ctx, String path) {
    // Normalize path by removing $ prefix if present
    String normalizedPath = path.startsWith("$") ? path.substring(1) : path;

    // Handle nested field paths similar to renderSumArray
    int firstDot = normalizedPath.indexOf('.');
    if (firstDot > 0) {
      final String arrayPath = normalizedPath.substring(0, firstDot);
      final String nestedPath = normalizedPath.substring(firstDot + 1);

      // Check if the array path is a pipeline lookup result (e.g., from $graphLookup)
      String pipelineLookupAlias = ctx.getPipelineLookupAlias(arrayPath);
      if (pipelineLookupAlias != null) {
        // For pipeline lookup: avg from the JSON array column in the CTE
        ctx.sql("(SELECT AVG(TO_NUMBER(val)) FROM JSON_TABLE(");
        ctx.sql(pipelineLookupAlias);
        ctx.sql(".");
        ctx.sql(arrayPath); // column name
        ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$.");
        ctx.sql(nestedPath);
        ctx.sql("')))");
        return;
      }

      // Check if the array path is from a simple (equality) $lookup
      SqlGenerationContext.LookupFieldInfo lookupInfo = ctx.getLookupFieldInfo(arrayPath);
      if (lookupInfo != null) {
        // For equality lookup: generate correlated subquery averaging from foreign table
        String subqAlias = "avg_" + arrayPath.substring(0, Math.min(3, arrayPath.length()));
        ctx.sql("(SELECT AVG(");
        ctx.sql(subqAlias);
        ctx.sql(".data.");
        ctx.sql(quotePath(nestedPath));
        ctx.sql(") FROM ");
        ctx.tableName(lookupInfo.foreignTable());
        ctx.sql(" ");
        ctx.sql(subqAlias);
        ctx.sql(" WHERE ");
        ctx.sql(subqAlias);
        ctx.sql(".data.");
        ctx.sql(quotePath(lookupInfo.foreignField()));
        ctx.sql(" = ");
        String alias = ctx.getBaseTableAlias();
        if (alias != null && !alias.isEmpty()) {
          ctx.sql(alias);
          ctx.sql(".");
        }
        ctx.sql("data.");
        ctx.sql(quotePath(lookupInfo.localField()));
        ctx.sql(")");
        return;
      }

      ctx.sql("(SELECT AVG(TO_NUMBER(val)) FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(arrayPath);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$.");
      ctx.sql(nestedPath);
      ctx.sql("')))");
    } else {
      // Simple array path: avg all elements directly
      // Check if this is a pipeline lookup result
      String pipelineLookupAlias = ctx.getPipelineLookupAlias(normalizedPath);
      if (pipelineLookupAlias != null) {
        // For pipeline lookup: avg all elements from the JSON array column
        ctx.sql("(SELECT AVG(TO_NUMBER(val)) FROM JSON_TABLE(");
        ctx.sql(pipelineLookupAlias);
        ctx.sql(".");
        ctx.sql(normalizedPath); // column name
        ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
        return;
      }

      ctx.sql("(SELECT AVG(TO_NUMBER(val)) FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(normalizedPath);
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
    }
  }

  /**
   * Renders $sum when the array is an expression (not a field path). Uses JSON_TABLE to iterate and
   * sum values.
   */
  private void renderSumArrayExpression(SqlGenerationContext ctx) {
    ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), 0) FROM JSON_TABLE(");
    ctx.visit(arrayExpression);
    ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
  }

  /**
   * Renders $avg when the array is an expression (not a field path). Uses JSON_TABLE to iterate and
   * average values.
   */
  private void renderAvgArrayExpression(SqlGenerationContext ctx) {
    ctx.sql("(SELECT AVG(TO_NUMBER(val)) FROM JSON_TABLE(");
    ctx.visit(arrayExpression);
    ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
  }

  /** Helper method to render an array expression as a SELECT statement. */
  private void renderArrayAsSelect(SqlGenerationContext ctx, Expression arr) {
    if (arr instanceof FieldPathExpression fieldPath) {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath.getPath());
      ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    } else {
      ctx.sql("SELECT DISTINCT val FROM JSON_TABLE(");
      ctx.visit(arr);
      ctx.sql(", '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))");
    }
  }

  /**
   * Quotes a field path for Oracle dot notation. Segments that start with underscore or digit need
   * quoting since Oracle identifiers must start with a letter when unquoted.
   */
  private static String quotePath(String path) {
    String[] segments = path.split("\\.");
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        result.append(".");
      }
      String segment = segments[i];
      if (!segment.isEmpty() && !Character.isLetter(segment.charAt(0))) {
        result.append("\"").append(segment).append("\"");
      } else {
        result.append(segment);
      }
    }
    return result.toString();
  }

  /**
   * Renders $range operator. MongoDB: {$range: [start, end, step?]} - generates array of integers.
   * Oracle: Uses CONNECT BY LEVEL to generate the sequence.
   */
  private void renderRange(SqlGenerationContext ctx) {
    // arrayExpression = start, indexExpression = end, additionalArgs[0] = step
    Expression startExpr = arrayExpression;
    Expression endExpr = indexExpression;
    final Expression stepExpr =
        (additionalArgs != null && !additionalArgs.isEmpty())
            ? additionalArgs.get(0)
            : LiteralExpression.of(1);

    // For step = 1 (or positive step):
    // SELECT JSON_ARRAYAGG(n ORDER BY n) FROM (
    //   SELECT start + LEVEL - 1 AS n FROM DUAL
    //   CONNECT BY start + LEVEL - 1 < end
    // )
    // For general step:
    // SELECT JSON_ARRAYAGG(n ORDER BY n) FROM (
    //   SELECT start + (LEVEL - 1) * step AS n FROM DUAL
    //   CONNECT BY start + (LEVEL - 1) * step < end (for positive step)
    //   CONNECT BY start + (LEVEL - 1) * step > end (for negative step)
    // )

    ctx.sql("(SELECT JSON_ARRAYAGG(n ORDER BY n) FROM (SELECT ");
    renderRangeExpression(ctx, startExpr);
    ctx.sql(" + (LEVEL - 1) * ");
    renderRangeExpression(ctx, stepExpr);
    ctx.sql(" AS n FROM DUAL CONNECT BY ");

    // Determine the direction check based on step
    if (stepExpr instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
      if (num.intValue() < 0) {
        // Negative step: start + (LEVEL - 1) * step > end
        renderRangeExpression(ctx, startExpr);
        ctx.sql(" + (LEVEL - 1) * ");
        renderRangeExpression(ctx, stepExpr);
        ctx.sql(" > ");
        renderRangeExpression(ctx, endExpr);
      } else {
        // Positive step: start + (LEVEL - 1) * step < end
        renderRangeExpression(ctx, startExpr);
        ctx.sql(" + (LEVEL - 1) * ");
        renderRangeExpression(ctx, stepExpr);
        ctx.sql(" < ");
        renderRangeExpression(ctx, endExpr);
      }
    } else {
      // Dynamic step - use SIGN function
      ctx.sql("SIGN(");
      renderRangeExpression(ctx, stepExpr);
      ctx.sql(") * (");
      renderRangeExpression(ctx, startExpr);
      ctx.sql(" + (LEVEL - 1) * ");
      renderRangeExpression(ctx, stepExpr);
      ctx.sql(") < SIGN(");
      renderRangeExpression(ctx, stepExpr);
      ctx.sql(") * ");
      renderRangeExpression(ctx, endExpr);
    }

    ctx.sql("))");
  }

  /** Helper to render a range expression (start, end, or step). */
  private void renderRangeExpression(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof LiteralExpression lit) {
      ctx.sql(String.valueOf(lit.getValue()));
    } else if (expr instanceof FieldPathExpression fieldPath) {
      ctx.sql("JSON_VALUE(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath.getPath());
      ctx.sql("' RETURNING NUMBER)");
    } else {
      ctx.visit(expr);
    }
  }

  /**
   * Renders $zip operator. MongoDB: {$zip: {inputs: [arr1, arr2], useLongestLength: boolean,
   * defaults: [d1, d2]}} Oracle: Uses JSON_TABLE with row numbers and joins to zip arrays together.
   */
  private void renderZip(SqlGenerationContext ctx) {
    // arrayExpression = useLongestLength (as LiteralExpression)
    // indexExpression = inputCount (as LiteralExpression)
    // additionalArgs = inputs followed by defaults (if any)

    boolean useLongestLength =
        arrayExpression instanceof LiteralExpression lit
            && lit.getValue() instanceof Boolean b
            && b;

    int inputCount =
        indexExpression instanceof LiteralExpression lit && lit.getValue() instanceof Number n
            ? n.intValue()
            : 2;

    final List<Expression> inputs = additionalArgs.subList(0, inputCount);
    final List<Expression> defaults =
        additionalArgs.size() > inputCount
            ? additionalArgs.subList(inputCount, additionalArgs.size())
            : Collections.emptyList();

    // Build the SQL using JSON_TABLE for each array
    // SELECT JSON_ARRAYAGG(JSON_ARRAY(t1.val, t2.val) ORDER BY rn) FROM (
    //   SELECT t1.rn, t1.val as v1, t2.val as v2 FROM
    //   JSON_TABLE(data, '$.arr1[*]' COLUMNS (rn FOR ORDINALITY, val PATH '$')) t1
    //   [INNER/FULL OUTER] JOIN
    //   JSON_TABLE(data, '$.arr2[*]' COLUMNS (rn FOR ORDINALITY, val PATH '$')) t2
    //   ON t1.rn = t2.rn
    // )

    ctx.sql("(SELECT JSON_ARRAYAGG(JSON_ARRAY(");

    // List columns for the JSON_ARRAY
    for (int i = 0; i < inputCount; i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      if (useLongestLength && i < defaults.size()) {
        ctx.sql("COALESCE(t");
        ctx.sql(String.valueOf(i + 1));
        ctx.sql(".val, ");
        renderZipDefault(ctx, defaults.get(i));
        ctx.sql(")");
      } else {
        ctx.sql("t");
        ctx.sql(String.valueOf(i + 1));
        ctx.sql(".val");
      }
    }

    ctx.sql(") ORDER BY ");
    // Order by first table's row number
    ctx.sql("t1.rn) FROM ");

    // First JSON_TABLE
    renderZipJsonTable(ctx, inputs.get(0), 1);

    // Join remaining tables
    String joinType = useLongestLength ? " FULL OUTER JOIN " : " INNER JOIN ";
    for (int i = 1; i < inputCount; i++) {
      ctx.sql(joinType);
      renderZipJsonTable(ctx, inputs.get(i), i + 1);
      ctx.sql(" ON t1.rn = t");
      ctx.sql(String.valueOf(i + 1));
      ctx.sql(".rn");
    }

    ctx.sql(")");
  }

  /** Helper to render JSON_TABLE for a zip input array. */
  private void renderZipJsonTable(SqlGenerationContext ctx, Expression arrayExpr, int tableNum) {
    ctx.sql("JSON_TABLE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");

    if (arrayExpr instanceof FieldPathExpression fieldPath) {
      ctx.sql(fieldPath.getPath());
    } else {
      // For non-field-path expressions, use a placeholder
      ctx.sql("array");
      ctx.sql(String.valueOf(tableNum));
    }

    ctx.sql("[*]' COLUMNS (rn FOR ORDINALITY, val PATH '$')) t");
    ctx.sql(String.valueOf(tableNum));
  }

  /** Helper to render a default value for zip. */
  private void renderZipDefault(SqlGenerationContext ctx, Expression defaultExpr) {
    if (defaultExpr instanceof LiteralExpression lit) {
      Object value = lit.getValue();
      if (value instanceof String) {
        ctx.sql("'");
        ctx.sql(value.toString().replace("'", "''"));
        ctx.sql("'");
      } else {
        ctx.sql(String.valueOf(value));
      }
    } else {
      ctx.visit(defaultExpr);
    }
  }

  /**
   * Collects all variable field references (e.g., $$item.product) from an InlineObjectExpression.
   * These need to be defined as columns in the JSON_TABLE.
   */
  private void collectVariableFieldsFromObject(
      InlineObjectExpression obj, java.util.Set<String> fields) {
    for (java.util.Map.Entry<String, Expression> entry : obj.getFields().entrySet()) {
      collectVariableFieldsFromExpression(entry.getValue(), fields);
    }
  }

  /**
   * Recursively collects variable field references from an expression. Field paths like
   * "$item.product" get added as "product".
   */
  private void collectVariableFieldsFromExpression(Expression expr, java.util.Set<String> fields) {
    if (expr instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      // Check for patterns like $item.field, $elem.field, $this.field
      if (path != null && path.startsWith("$") && path.contains(".")) {
        int dotIndex = path.indexOf('.');
        fields.add(path.substring(dotIndex + 1));
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      for (Expression operand : arith.getOperands()) {
        collectVariableFieldsFromExpression(operand, fields);
      }
    } else if (expr instanceof ComparisonExpression comp) {
      collectVariableFieldsFromExpression(comp.getLeft(), fields);
      collectVariableFieldsFromExpression(comp.getRight(), fields);
    } else if (expr instanceof InlineObjectExpression nested) {
      collectVariableFieldsFromObject(nested, fields);
    } else if (expr instanceof ConditionalExpression cond) {
      if (cond.getCondition() != null) {
        collectVariableFieldsFromExpression(cond.getCondition(), fields);
      }
      collectVariableFieldsFromExpression(cond.getThenExpr(), fields);
      collectVariableFieldsFromExpression(cond.getElseExpr(), fields);
    }
    // Add other expression types as needed
  }

  /**
   * Renders $map with object transformation. Creates JSON_TABLE with columns for each variable
   * field, then renders JSON_OBJECT with those columns.
   * Fields used in arithmetic are defined as NUMBER type for proper numeric handling.
   */
  private void renderMapWithObjectTransformation(
      SqlGenerationContext ctx,
      String arrayPath,
      InlineObjectExpression inlineObj,
      java.util.Set<String> varFields) {

    // Collect fields used in arithmetic expressions - these need NUMBER type
    java.util.Set<String> numericFields = new java.util.LinkedHashSet<>();
    collectArithmeticFields(inlineObj, numericFields);

    ctx.sql("(SELECT COALESCE(JSON_ARRAYAGG(");

    // Render JSON_OBJECT with the fields, substituting variable references with column names
    ctx.sql("JSON_OBJECT(");
    boolean first = true;
    for (java.util.Map.Entry<String, Expression> entry : inlineObj.getFields().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      first = false;

      ctx.sql("'");
      ctx.sql(entry.getKey());
      ctx.sql("' VALUE ");

      // Render the field value, substituting variable references
      renderMapFieldExpression(ctx, entry.getValue(), varFields);
    }
    ctx.sql(")");

    ctx.sql("), JSON_ARRAY()) FROM JSON_TABLE(");
    renderDataColumn(ctx);
    ctx.sql(", '$.");
    ctx.sql(arrayPath);
    ctx.sql("[*]' COLUMNS (");

    // Generate columns for each variable field
    // Fields used in arithmetic get NUMBER type, others get default VARCHAR2
    boolean firstCol = true;
    for (String field : varFields) {
      if (!firstCol) {
        ctx.sql(", ");
      }
      firstCol = false;
      ctx.sql(field);
      if (numericFields.contains(field)) {
        // Field is used in arithmetic - define as NUMBER for proper type handling
        ctx.sql(" NUMBER PATH '$.");
      } else {
        ctx.sql(" PATH '$.");
      }
      ctx.sql(field);
      ctx.sql("'");
    }
    ctx.sql(")))");
  }

  /**
   * Collects field names that are used in arithmetic expressions.
   * These fields should be defined as NUMBER in JSON_TABLE for correct numeric operations.
   */
  private void collectArithmeticFields(InlineObjectExpression obj, java.util.Set<String> fields) {
    for (java.util.Map.Entry<String, Expression> entry : obj.getFields().entrySet()) {
      if (entry.getValue() instanceof ArithmeticExpression arith) {
        collectArithmeticOperandFields(arith, fields);
      }
    }
  }

  /**
   * Recursively collects field names used as operands in arithmetic expressions.
   */
  private void collectArithmeticOperandFields(
      ArithmeticExpression arith, java.util.Set<String> fields) {
    for (Expression operand : arith.getOperands()) {
      if (operand instanceof FieldPathExpression fp) {
        String path = fp.getPath();
        if (path != null && path.startsWith("$") && path.contains(".")) {
          int dotIndex = path.indexOf('.');
          fields.add(path.substring(dotIndex + 1));
        }
      } else if (operand instanceof ArithmeticExpression nested) {
        collectArithmeticOperandFields(nested, fields);
      }
    }
  }

  /**
   * Renders an expression used in $map, substituting variable field references with JSON_TABLE
   * column names.
   */
  private void renderMapFieldExpression(
      SqlGenerationContext ctx, Expression expr, java.util.Set<String> varFields) {
    if (expr instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      // Check for patterns like $item.field, $elem.field, $this.field
      if (path != null && path.startsWith("$") && path.contains(".")) {
        int dotIndex = path.indexOf('.');
        String fieldName = path.substring(dotIndex + 1);
        // Output just the column name (defined in JSON_TABLE)
        ctx.sql(fieldName);
        return;
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      List<Expression> operands = arith.getOperands();
      String sqlOp = getSqlArithmeticOp(arith.getOp());

      ctx.sql("(");
      for (int i = 0; i < operands.size(); i++) {
        if (i > 0) {
          ctx.sql(" ");
          ctx.sql(sqlOp);
          ctx.sql(" ");
        }
        renderMapFieldExpression(ctx, operands.get(i), varFields);
      }
      ctx.sql(")");
      return;
    } else if (expr instanceof LiteralExpression) {
      ctx.visit(expr);
      return;
    }

    // Fallback: render expression as-is
    ctx.visit(expr);
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
