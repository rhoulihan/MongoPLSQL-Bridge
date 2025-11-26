/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a date expression that extracts date components.
 * Translates MongoDB date operators to Oracle EXTRACT or TO_CHAR functions.
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code {$year: "$createdAt"}} becomes
 *       {@code EXTRACT(YEAR FROM TO_TIMESTAMP(JSON_VALUE(data, '$.createdAt'), ...))}</li>
 *   <li>{@code {$month: "$date"}} becomes
 *       {@code EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(data, '$.date'), ...))}</li>
 *   <li>{@code {$dayOfWeek: "$date"}} becomes
 *       {@code TO_NUMBER(TO_CHAR(TO_TIMESTAMP(...), 'D'))}</li>
 * </ul>
 *
 * <p>Note: MongoDB dates stored as ISODate are represented as strings in JSON.
 * Oracle needs TO_TIMESTAMP to convert the ISO 8601 string to a timestamp.
 */
public final class DateExpression implements Expression {

    /**
     * ISO 8601 format pattern for Oracle TO_TIMESTAMP.
     * Handles dates like: 2024-01-15T10:30:00.000Z
     */
    private static final String ISO_FORMAT = "YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"";

    private final DateOp op;
    private final Expression argument;

    /**
     * Creates a date expression.
     *
     * @param op       the date operator
     * @param argument the date expression to extract from
     */
    public DateExpression(DateOp op, Expression argument) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.argument = Objects.requireNonNull(argument, "argument must not be null");
    }

    /**
     * Creates a $year expression.
     */
    public static DateExpression year(Expression argument) {
        return new DateExpression(DateOp.YEAR, argument);
    }

    /**
     * Creates a $month expression.
     */
    public static DateExpression month(Expression argument) {
        return new DateExpression(DateOp.MONTH, argument);
    }

    /**
     * Creates a $dayOfMonth expression.
     */
    public static DateExpression dayOfMonth(Expression argument) {
        return new DateExpression(DateOp.DAY_OF_MONTH, argument);
    }

    /**
     * Creates a $hour expression.
     */
    public static DateExpression hour(Expression argument) {
        return new DateExpression(DateOp.HOUR, argument);
    }

    /**
     * Creates a $minute expression.
     */
    public static DateExpression minute(Expression argument) {
        return new DateExpression(DateOp.MINUTE, argument);
    }

    /**
     * Creates a $second expression.
     */
    public static DateExpression second(Expression argument) {
        return new DateExpression(DateOp.SECOND, argument);
    }

    /**
     * Creates a $dayOfWeek expression.
     */
    public static DateExpression dayOfWeek(Expression argument) {
        return new DateExpression(DateOp.DAY_OF_WEEK, argument);
    }

    /**
     * Creates a $dayOfYear expression.
     */
    public static DateExpression dayOfYear(Expression argument) {
        return new DateExpression(DateOp.DAY_OF_YEAR, argument);
    }

    /**
     * Returns the date operator.
     */
    public DateOp getOp() {
        return op;
    }

    /**
     * Returns the argument expression.
     */
    public Expression getArgument() {
        return argument;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Build the timestamp conversion
        StringBuilder timestampExpr = new StringBuilder();
        timestampExpr.append("TO_TIMESTAMP(");

        // Capture the inner expression
        var innerCtx = ctx.createNestedContext();
        innerCtx.visit(argument);
        timestampExpr.append(innerCtx.toSql());

        timestampExpr.append(", '");
        timestampExpr.append(ISO_FORMAT);
        timestampExpr.append("')");

        String timestampStr = timestampExpr.toString();

        if (op.isExtractBased()) {
            // EXTRACT(YEAR FROM timestamp)
            ctx.sql(String.format(op.getSqlTemplate(), timestampStr));
        } else {
            // TO_NUMBER(TO_CHAR(timestamp, 'D'))
            ctx.sql("TO_NUMBER(");
            ctx.sql(String.format(op.getSqlTemplate(), timestampStr));
            ctx.sql(")");
        }
    }

    @Override
    public String toString() {
        return "Date(" + op.getMongoOperator() + ", " + argument + ")";
    }
}
