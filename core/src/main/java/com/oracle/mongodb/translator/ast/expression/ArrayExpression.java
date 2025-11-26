/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents an array expression.
 * Translates MongoDB array operators to Oracle JSON path expressions.
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code {$arrayElemAt: ["$items", 0]}} becomes
 *       {@code JSON_VALUE(data, '$.items[0]')}</li>
 *   <li>{@code {$size: "$items"}} becomes
 *       {@code JSON_VALUE(data, '$.items.size()')}</li>
 *   <li>{@code {$first: "$items"}} becomes
 *       {@code JSON_VALUE(data, '$.items[0]')}</li>
 *   <li>{@code {$last: "$items"}} becomes
 *       {@code JSON_VALUE(data, '$.items[last]')}</li>
 * </ul>
 */
public final class ArrayExpression implements Expression {

    private final ArrayOp op;
    private final Expression arrayExpression;
    private final Expression indexExpression;

    /**
     * Creates an array expression.
     *
     * @param op              the array operator
     * @param arrayExpression the array field expression
     * @param indexExpression the index expression (can be null for $size, $first, $last)
     */
    public ArrayExpression(ArrayOp op, Expression arrayExpression, Expression indexExpression) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.arrayExpression = Objects.requireNonNull(arrayExpression, "arrayExpression must not be null");
        this.indexExpression = indexExpression;
    }

    /**
     * Creates a $arrayElemAt expression.
     */
    public static ArrayExpression arrayElemAt(Expression array, Expression index) {
        return new ArrayExpression(ArrayOp.ARRAY_ELEM_AT, array, index);
    }

    /**
     * Creates a $size expression.
     */
    public static ArrayExpression size(Expression array) {
        return new ArrayExpression(ArrayOp.SIZE, array, null);
    }

    /**
     * Creates a $first expression.
     */
    public static ArrayExpression first(Expression array) {
        return new ArrayExpression(ArrayOp.FIRST, array, null);
    }

    /**
     * Creates a $last expression.
     */
    public static ArrayExpression last(Expression array) {
        return new ArrayExpression(ArrayOp.LAST, array, null);
    }

    /**
     * Returns the array operator.
     */
    public ArrayOp getOp() {
        return op;
    }

    /**
     * Returns the array expression.
     */
    public Expression getArrayExpression() {
        return arrayExpression;
    }

    /**
     * Returns the index expression (may be null).
     */
    public Expression getIndexExpression() {
        return indexExpression;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Get the field path from the array expression
        if (!(arrayExpression instanceof FieldPathExpression fieldPath)) {
            throw new IllegalStateException("Array expression must be a field path");
        }

        String path = fieldPath.getPath();

        switch (op) {
            case ARRAY_ELEM_AT -> renderArrayElemAt(ctx, path);
            case SIZE -> renderSize(ctx, path);
            case FIRST -> renderFirst(ctx, path);
            case LAST -> renderLast(ctx, path);
        }
    }

    private void renderArrayElemAt(SqlGenerationContext ctx, String path) {
        // JSON_VALUE(data, '$.items[index]')
        if (indexExpression instanceof LiteralExpression lit
            && lit.getValue() instanceof Number num) {
            ctx.sql("JSON_VALUE(data, '$.");
            ctx.sql(path);
            ctx.sql("[");
            ctx.sql(String.valueOf(num.intValue()));
            ctx.sql("]')");
        } else {
            throw new IllegalArgumentException("$arrayElemAt index must be a literal number");
        }
    }

    private void renderSize(SqlGenerationContext ctx, String path) {
        // JSON_VALUE(data, '$.items.size()')
        ctx.sql("JSON_VALUE(data, '$.");
        ctx.sql(path);
        ctx.sql(".size()')");
    }

    private void renderFirst(SqlGenerationContext ctx, String path) {
        // JSON_VALUE(data, '$.items[0]')
        ctx.sql("JSON_VALUE(data, '$.");
        ctx.sql(path);
        ctx.sql("[0]')");
    }

    private void renderLast(SqlGenerationContext ctx, String path) {
        // JSON_VALUE(data, '$.items[last]')
        ctx.sql("JSON_VALUE(data, '$.");
        ctx.sql(path);
        ctx.sql("[last]')");
    }

    @Override
    public String toString() {
        if (indexExpression != null) {
            return "Array(" + op.getMongoOperator() + ", " + arrayExpression + ", " + indexExpression + ")";
        }
        return "Array(" + op.getMongoOperator() + ", " + arrayExpression + ")";
    }
}
