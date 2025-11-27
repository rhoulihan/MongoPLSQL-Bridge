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
 * Represents an arithmetic expression with one or more operands.
 * Translates to Oracle arithmetic operations or functions.
 *
 * <p>Examples:
 * <ul>
 *   <li>{@code {$add: ["$price", "$tax"]}} becomes {@code price + tax}</li>
 *   <li>{@code {$multiply: ["$qty", "$price"]}} becomes {@code qty * price}</li>
 *   <li>{@code {$mod: ["$value", 10]}} becomes {@code MOD(value, 10)}</li>
 * </ul>
 */
public final class ArithmeticExpression implements Expression {

    private final ArithmeticOp op;
    private final List<Expression> operands;

    /**
     * Creates an arithmetic expression.
     *
     * @param op the arithmetic operator
     * @param operands the operand expressions (at least 1 for unary, 2 for binary operators)
     */
    public ArithmeticExpression(ArithmeticOp op, List<Expression> operands) {
        this.op = Objects.requireNonNull(op, "op must not be null");
        if (operands == null || operands.isEmpty()) {
            throw new IllegalArgumentException("Arithmetic expression requires at least 1 operand");
        }
        if (operands.size() < 2 && !op.allowsSingleOperand()) {
            throw new IllegalArgumentException(op.getMongoOperator() + " requires at least 2 operands");
        }
        this.operands = new ArrayList<>(operands);
    }

    /**
     * Creates an addition expression.
     */
    public static ArithmeticExpression add(Expression... operands) {
        return new ArithmeticExpression(ArithmeticOp.ADD, List.of(operands));
    }

    /**
     * Creates a subtraction expression.
     */
    public static ArithmeticExpression subtract(Expression left, Expression right) {
        return new ArithmeticExpression(ArithmeticOp.SUBTRACT, List.of(left, right));
    }

    /**
     * Creates a multiplication expression.
     */
    public static ArithmeticExpression multiply(Expression... operands) {
        return new ArithmeticExpression(ArithmeticOp.MULTIPLY, List.of(operands));
    }

    /**
     * Creates a division expression.
     */
    public static ArithmeticExpression divide(Expression left, Expression right) {
        return new ArithmeticExpression(ArithmeticOp.DIVIDE, List.of(left, right));
    }

    /**
     * Creates a modulo expression.
     */
    public static ArithmeticExpression mod(Expression left, Expression right) {
        return new ArithmeticExpression(ArithmeticOp.MOD, List.of(left, right));
    }

    /**
     * Returns the arithmetic operator.
     */
    public ArithmeticOp getOp() {
        return op;
    }

    /**
     * Returns the operands as an unmodifiable list.
     */
    public List<Expression> getOperands() {
        return Collections.unmodifiableList(operands);
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        if (op.requiresFunctionCall()) {
            renderAsFunction(ctx);
        } else {
            renderAsInfix(ctx);
        }
    }

    private void renderAsFunction(SqlGenerationContext ctx) {
        // MOD(a, b)
        ctx.sql(op.getSqlOperator());
        ctx.sql("(");
        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                ctx.sql(", ");
            }
            ctx.visit(operands.get(i));
        }
        ctx.sql(")");
    }

    private void renderAsInfix(SqlGenerationContext ctx) {
        // (a + b + c) or (a - b)
        ctx.sql("(");
        for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
                ctx.sql(" ");
                ctx.sql(op.getSqlOperator());
                ctx.sql(" ");
            }
            ctx.visit(operands.get(i));
        }
        ctx.sql(")");
    }

    @Override
    public String toString() {
        return "Arithmetic(" + op.getMongoOperator() + ", " + operands + ")";
    }
}
