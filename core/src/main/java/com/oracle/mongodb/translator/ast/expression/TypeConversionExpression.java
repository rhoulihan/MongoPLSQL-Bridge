/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a type conversion expression.
 * Translates to Oracle type conversion functions.
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code {$toInt: "$price"}} becomes {@code TO_NUMBER(JSON_VALUE(data, '$.price'))}</li>
 *   <li>{@code {$toString: "$count"}} becomes {@code TO_CHAR(JSON_VALUE(data, '$.count'))}</li>
 *   <li>{@code {$toBool: "$active"}} becomes {@code CASE WHEN ... THEN 1 ELSE 0 END}</li>
 *   <li>{@code {$toDate: "$timestamp"}} becomes {@code TO_TIMESTAMP_TZ(...)}</li>
 *   <li>{@code {$type: "$field"}} becomes {@code JSON_VALUE(data, '$.field.type()')}</li>
 * </ul>
 */
public final class TypeConversionExpression implements Expression {

    private final TypeConversionOp op;
    private final Expression argument;
    private final Expression onError;
    private final Expression onNull;

    /**
     * Creates a type conversion expression.
     *
     * @param op       the type conversion operator
     * @param argument the expression to convert
     */
    public TypeConversionExpression(TypeConversionOp op, Expression argument) {
        this(op, argument, null, null);
    }

    /**
     * Creates a type conversion expression with error handling.
     *
     * @param op       the type conversion operator
     * @param argument the expression to convert
     * @param onError  expression to return on conversion error (for $convert)
     * @param onNull   expression to return if input is null (for $convert)
     */
    public TypeConversionExpression(TypeConversionOp op, Expression argument,
                                     Expression onError, Expression onNull) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.argument = Objects.requireNonNull(argument, "argument must not be null");
        this.onError = onError;
        this.onNull = onNull;
    }

    /**
     * Creates a $toInt expression.
     */
    public static TypeConversionExpression toInt(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TO_INT, argument);
    }

    /**
     * Creates a $toLong expression.
     */
    public static TypeConversionExpression toLong(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TO_LONG, argument);
    }

    /**
     * Creates a $toDouble expression.
     */
    public static TypeConversionExpression toDouble(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TO_DOUBLE, argument);
    }

    /**
     * Creates a $toDecimal expression.
     */
    public static TypeConversionExpression toDecimal(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TO_DECIMAL, argument);
    }

    /**
     * Creates a $toString expression.
     */
    public static TypeConversionExpression toStringExpr(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TO_STRING, argument);
    }

    /**
     * Creates a $toBool expression.
     */
    public static TypeConversionExpression toBool(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TO_BOOL, argument);
    }

    /**
     * Creates a $toDate expression.
     */
    public static TypeConversionExpression toDate(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TO_DATE, argument);
    }

    /**
     * Creates a $toObjectId expression.
     */
    public static TypeConversionExpression toObjectId(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TO_OBJECT_ID, argument);
    }

    /**
     * Creates a $type expression.
     */
    public static TypeConversionExpression type(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.TYPE, argument);
    }

    /**
     * Creates an $isNumber expression.
     */
    public static TypeConversionExpression isNumber(Expression argument) {
        return new TypeConversionExpression(TypeConversionOp.IS_NUMBER, argument);
    }

    /**
     * Creates a $convert expression.
     */
    public static TypeConversionExpression convert(Expression argument, Expression onError, Expression onNull) {
        return new TypeConversionExpression(TypeConversionOp.CONVERT, argument, onError, onNull);
    }

    /**
     * Returns the type conversion operator.
     */
    public TypeConversionOp getOp() {
        return op;
    }

    /**
     * Returns the argument expression.
     */
    public Expression getArgument() {
        return argument;
    }

    /**
     * Returns the onError expression (for $convert), or null.
     */
    public Expression getOnError() {
        return onError;
    }

    /**
     * Returns the onNull expression (for $convert), or null.
     */
    public Expression getOnNull() {
        return onNull;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        switch (op) {
            case TO_INT, TO_LONG -> renderToNumber(ctx);
            case TO_DOUBLE -> renderToDouble(ctx);
            case TO_DECIMAL -> renderToDecimal(ctx);
            case TO_STRING -> renderToString(ctx);
            case TO_BOOL -> renderToBool(ctx);
            case TO_DATE -> renderToDate(ctx);
            case TO_OBJECT_ID -> renderToObjectId(ctx);
            case TYPE -> renderType(ctx);
            case IS_NUMBER -> renderIsNumber(ctx);
            case CONVERT -> renderConvert(ctx);
        }
    }

    private void renderToNumber(SqlGenerationContext ctx) {
        // TRUNC(TO_NUMBER(...)) for integer conversion
        ctx.sql("TRUNC(TO_NUMBER(");
        ctx.visit(argument);
        ctx.sql("))");
    }

    private void renderToDouble(SqlGenerationContext ctx) {
        ctx.sql("TO_BINARY_DOUBLE(");
        ctx.visit(argument);
        ctx.sql(")");
    }

    private void renderToDecimal(SqlGenerationContext ctx) {
        ctx.sql("TO_NUMBER(");
        ctx.visit(argument);
        ctx.sql(")");
    }

    private void renderToString(SqlGenerationContext ctx) {
        ctx.sql("TO_CHAR(");
        ctx.visit(argument);
        ctx.sql(")");
    }

    private void renderToBool(SqlGenerationContext ctx) {
        // MongoDB toBool: null/0/false -> false, everything else -> true
        // Oracle: CASE WHEN value IS NULL OR value = 0 OR value = 'false' THEN 0 ELSE 1 END
        ctx.sql("CASE WHEN ");
        ctx.visit(argument);
        ctx.sql(" IS NULL OR ");
        ctx.visit(argument);
        ctx.sql(" = 0 OR LOWER(TO_CHAR(");
        ctx.visit(argument);
        ctx.sql(")) = 'false' THEN 0 ELSE 1 END");
    }

    private void renderToDate(SqlGenerationContext ctx) {
        // Handle ISO 8601 date strings
        ctx.sql("TO_TIMESTAMP_TZ(");
        ctx.visit(argument);
        ctx.sql(", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3TZH:TZM')");
    }

    private void renderToObjectId(SqlGenerationContext ctx) {
        // ObjectId is stored as string in Oracle JSON, just return as-is
        ctx.visit(argument);
    }

    private void renderType(SqlGenerationContext ctx) {
        // Use JSON_VALUE with type() method to get BSON type
        // Returns: 'string', 'number', 'boolean', 'object', 'array', 'null'
        ctx.sql("CASE ");
        ctx.sql("WHEN ");
        ctx.visit(argument);
        ctx.sql(" IS NULL THEN 'null' ");
        ctx.sql("WHEN JSON_VALUE(TO_CHAR(");
        ctx.visit(argument);
        ctx.sql("), '$' RETURNING NUMBER ERROR ON ERROR) IS NOT NULL THEN 'number' ");
        ctx.sql("WHEN LOWER(TO_CHAR(");
        ctx.visit(argument);
        ctx.sql(")) IN ('true', 'false') THEN 'bool' ");
        ctx.sql("ELSE 'string' END");
    }

    private void renderIsNumber(SqlGenerationContext ctx) {
        // Check if expression evaluates to a number
        ctx.sql("CASE WHEN REGEXP_LIKE(TO_CHAR(");
        ctx.visit(argument);
        ctx.sql("), '^-?[0-9]+(\\.[0-9]+)?$') THEN 1 ELSE 0 END");
    }

    private void renderConvert(SqlGenerationContext ctx) {
        // $convert with onError and onNull handling
        // Use NVL for null handling and rely on Oracle's default conversion
        if (onNull != null) {
            ctx.sql("NVL(");
            ctx.visit(argument);
            ctx.sql(", ");
            ctx.visit(onNull);
            ctx.sql(")");
        } else {
            ctx.visit(argument);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TypeConversionExpression that = (TypeConversionExpression) obj;
        return op == that.op
            && Objects.equals(argument, that.argument)
            && Objects.equals(onError, that.onError)
            && Objects.equals(onNull, that.onNull);
    }

    @Override
    public int hashCode() {
        return Objects.hash(op, argument, onError, onNull);
    }

    @Override
    public String toString() {
        return "TypeConversion(" + op.getMongoOperator() + ", " + argument + ")";
    }
}
