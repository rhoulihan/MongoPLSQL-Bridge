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
 * Represents a string expression.
 * Translates to Oracle string functions (CONCAT, LOWER, UPPER, SUBSTR, etc.).
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code {$concat: ["Hello ", "$name"]}} becomes {@code 'Hello ' || name}</li>
 *   <li>{@code {$toLower: "$email"}} becomes {@code LOWER(email)}</li>
 *   <li>{@code {$toUpper: "$code"}} becomes {@code UPPER(code)}</li>
 *   <li>{@code {$substr: ["$text", 0, 5]}} becomes {@code SUBSTR(text, 1, 5)}</li>
 * </ul>
 */
public final class StringExpression implements Expression {

    private final StringOp op;
    private final List<Expression> arguments;

    /**
     * Creates a string expression.
     *
     * @param op        the string operator
     * @param arguments the argument expressions
     */
    public StringExpression(StringOp op, List<Expression> arguments) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.arguments = arguments != null ? new ArrayList<>(arguments) : new ArrayList<>();
    }

    /**
     * Creates a $concat expression.
     */
    public static StringExpression concat(List<Expression> parts) {
        return new StringExpression(StringOp.CONCAT, parts);
    }

    /**
     * Creates a $toLower expression.
     */
    public static StringExpression toLower(Expression argument) {
        return new StringExpression(StringOp.TO_LOWER, List.of(argument));
    }

    /**
     * Creates a $toUpper expression.
     */
    public static StringExpression toUpper(Expression argument) {
        return new StringExpression(StringOp.TO_UPPER, List.of(argument));
    }

    /**
     * Creates a $substr expression.
     *
     * @param string the source string
     * @param start  the starting index (0-based in MongoDB)
     * @param length the number of characters
     */
    public static StringExpression substr(Expression string, Expression start, Expression length) {
        return new StringExpression(StringOp.SUBSTR, List.of(string, start, length));
    }

    /**
     * Creates a $trim expression.
     */
    public static StringExpression trim(Expression argument) {
        return new StringExpression(StringOp.TRIM, List.of(argument));
    }

    /**
     * Creates a $strLenCP expression (string length in code points).
     */
    public static StringExpression strlen(Expression argument) {
        return new StringExpression(StringOp.STRLEN, List.of(argument));
    }

    /**
     * Returns the string operator.
     */
    public StringOp getOp() {
        return op;
    }

    /**
     * Returns the arguments as an unmodifiable list.
     */
    public List<Expression> getArguments() {
        return Collections.unmodifiableList(arguments);
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        switch (op) {
            case CONCAT -> renderConcat(ctx);
            case SUBSTR -> renderSubstr(ctx);
            case TRIM -> renderTrim(ctx);
            default -> renderSimpleFunction(ctx);
        }
    }

    private void renderConcat(SqlGenerationContext ctx) {
        // Use || for concatenation in Oracle
        ctx.sql("(");
        for (int i = 0; i < arguments.size(); i++) {
            if (i > 0) {
                ctx.sql(" || ");
            }
            ctx.visit(arguments.get(i));
        }
        ctx.sql(")");
    }

    private void renderSubstr(SqlGenerationContext ctx) {
        // MongoDB uses 0-based index, Oracle uses 1-based
        // So we need to add 1 to the start position
        ctx.sql("SUBSTR(");
        ctx.visit(arguments.get(0)); // string
        ctx.sql(", ");

        // Handle start index - need to add 1 for Oracle's 1-based indexing
        Expression startExpr = arguments.get(1);
        if (startExpr instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
            // If it's a literal number, add 1 directly
            ctx.bind(num.intValue() + 1);
        } else {
            // Otherwise, add 1 in SQL
            ctx.sql("(");
            ctx.visit(startExpr);
            ctx.sql(" + 1)");
        }

        ctx.sql(", ");
        ctx.visit(arguments.get(2)); // length
        ctx.sql(")");
    }

    private void renderTrim(SqlGenerationContext ctx) {
        ctx.sql("TRIM(");
        if (!arguments.isEmpty()) {
            ctx.visit(arguments.get(0));
        }
        ctx.sql(")");
    }

    private void renderSimpleFunction(SqlGenerationContext ctx) {
        ctx.sql(op.getSqlFunction());
        ctx.sql("(");
        for (int i = 0; i < arguments.size(); i++) {
            if (i > 0) {
                ctx.sql(", ");
            }
            ctx.visit(arguments.get(i));
        }
        ctx.sql(")");
    }

    @Override
    public String toString() {
        return "String(" + op.getMongoOperator() + ", " + arguments + ")";
    }
}
