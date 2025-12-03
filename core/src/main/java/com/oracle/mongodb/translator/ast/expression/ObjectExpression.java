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
 * Represents an object expression. Translates MongoDB object operators to Oracle JSON functions.
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$mergeObjects: ["$obj1", "$obj2"]}} - merges multiple objects
 *   <li>{@code {$objectToArray: "$obj"}} - converts object to array of {k, v} pairs
 *   <li>{@code {$arrayToObject: "$arr"}} - converts array of {k, v} pairs to object
 * </ul>
 */
public final class ObjectExpression implements Expression {

  private final ObjectOp op;
  private final Expression inputExpression;
  private final List<Expression> additionalArgs;

  private ObjectExpression(ObjectOp op, Expression inputExpression, List<Expression> additionalArgs) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.inputExpression = inputExpression;
    this.additionalArgs = additionalArgs != null ? new ArrayList<>(additionalArgs) : null;
  }

  /**
   * Creates a $mergeObjects expression.
   *
   * @param objects the objects to merge
   */
  public static ObjectExpression mergeObjects(List<Expression> objects) {
    return new ObjectExpression(ObjectOp.MERGE_OBJECTS, null, objects);
  }

  /**
   * Creates a $objectToArray expression.
   *
   * @param object the object to convert to array
   */
  public static ObjectExpression objectToArray(Expression object) {
    return new ObjectExpression(ObjectOp.OBJECT_TO_ARRAY, object, null);
  }

  /**
   * Creates a $arrayToObject expression.
   *
   * @param array the array of {k, v} pairs to convert to object
   */
  public static ObjectExpression arrayToObject(Expression array) {
    return new ObjectExpression(ObjectOp.ARRAY_TO_OBJECT, array, null);
  }

  /** Returns the object operator. */
  public ObjectOp getOp() {
    return op;
  }

  /** Returns the input expression. */
  public Expression getInputExpression() {
    return inputExpression;
  }

  /** Returns additional arguments (may be null). */
  public List<Expression> getAdditionalArgs() {
    return additionalArgs != null ? Collections.unmodifiableList(additionalArgs) : null;
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    switch (op) {
      case MERGE_OBJECTS -> renderMergeObjects(ctx);
      case OBJECT_TO_ARRAY -> renderObjectToArray(ctx);
      case ARRAY_TO_OBJECT -> renderArrayToObject(ctx);
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

  /**
   * Renders $mergeObjects operator. MongoDB: {$mergeObjects: ["$obj1", "$obj2"]} Oracle: Uses
   * JSON_MERGEPATCH to combine objects (later objects override earlier).
   */
  private void renderMergeObjects(SqlGenerationContext ctx) {
    if (additionalArgs == null || additionalArgs.isEmpty()) {
      ctx.sql("JSON_OBJECT()");
      return;
    }

    if (additionalArgs.size() == 1) {
      // Single object - just return it
      Expression obj = additionalArgs.get(0);
      if (obj instanceof FieldPathExpression fieldPath) {
        ctx.sql("JSON_QUERY(");
        renderDataColumn(ctx);
        ctx.sql(", '$.");
        ctx.sql(fieldPath.getPath());
        ctx.sql("')");
      } else {
        ctx.visit(obj);
      }
      return;
    }

    // Multiple objects - chain JSON_MERGEPATCH calls
    // JSON_MERGEPATCH(JSON_MERGEPATCH(obj1, obj2), obj3)
    int depth = additionalArgs.size() - 1;

    // Open all the JSON_MERGEPATCH calls
    for (int i = 0; i < depth; i++) {
      ctx.sql("JSON_MERGEPATCH(");
    }

    // Render first object
    Expression first = additionalArgs.get(0);
    if (first instanceof FieldPathExpression fieldPath) {
      ctx.sql("JSON_QUERY(");
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath.getPath());
      ctx.sql("')");
    } else {
      ctx.visit(first);
    }

    // Render remaining objects with closing parentheses
    for (int i = 1; i < additionalArgs.size(); i++) {
      ctx.sql(", ");
      Expression obj = additionalArgs.get(i);
      if (obj instanceof FieldPathExpression fieldPath) {
        ctx.sql("JSON_QUERY(");
        renderDataColumn(ctx);
        ctx.sql(", '$.");
        ctx.sql(fieldPath.getPath());
        ctx.sql("')");
      } else {
        ctx.visit(obj);
      }
      ctx.sql(")");
    }
  }

  /**
   * Renders $objectToArray operator. MongoDB: {$objectToArray: "$obj"} Oracle: Converts object to
   * array of {k: key, v: value} pairs using JSON_TABLE.
   */
  private void renderObjectToArray(SqlGenerationContext ctx) {
    // Oracle 23c+ has JSON_KEYS and JSON_VALUE with path iteration
    // For compatibility, use a subquery approach
    ctx.sql("(SELECT JSON_ARRAYAGG(JSON_OBJECT('k' VALUE key_col, 'v' VALUE val_col)) ");
    ctx.sql("FROM JSON_TABLE(");

    if (inputExpression instanceof FieldPathExpression fieldPath) {
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath.getPath());
    } else {
      ctx.visit(inputExpression);
      ctx.sql(", '$");
    }

    ctx.sql(".*' COLUMNS (key_col VARCHAR2(4000) PATH '$.@key', ");
    ctx.sql("val_col VARCHAR2(4000) FORMAT JSON PATH '$')))");
  }

  /**
   * Renders $arrayToObject operator. MongoDB: {$arrayToObject: "$arr"} where arr contains [{k:
   * "key", v: "value"}, ...] Oracle: Converts array of {k, v} pairs to object.
   */
  private void renderArrayToObject(SqlGenerationContext ctx) {
    // Build object from array of key-value pairs
    ctx.sql("(SELECT JSON_OBJECTAGG(key_col VALUE val_col) ");
    ctx.sql("FROM JSON_TABLE(");

    if (inputExpression instanceof FieldPathExpression fieldPath) {
      renderDataColumn(ctx);
      ctx.sql(", '$.");
      ctx.sql(fieldPath.getPath());
    } else {
      ctx.visit(inputExpression);
      ctx.sql(", '$");
    }

    ctx.sql("[*]' COLUMNS (key_col VARCHAR2(4000) PATH '$.k', ");
    ctx.sql("val_col VARCHAR2(4000) FORMAT JSON PATH '$.v')))");
  }

  @Override
  public String toString() {
    if (additionalArgs != null) {
      return "Object(" + op.getMongoOperator() + ", " + additionalArgs + ")";
    }
    return "Object(" + op.getMongoOperator() + ", " + inputExpression + ")";
  }
}
