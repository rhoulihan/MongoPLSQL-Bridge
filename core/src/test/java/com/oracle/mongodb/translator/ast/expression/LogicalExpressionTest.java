/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogicalExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderAndExpression() {
        var expr = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("status"), LiteralExpression.of("active")),
                new ComparisonExpression(ComparisonOp.GT,
                    FieldPathExpression.of("age", JsonReturnType.NUMBER), LiteralExpression.of(21))
            )
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(
            "(JSON_VALUE(data, '$.status') = :1) AND (JSON_VALUE(data, '$.age' RETURNING NUMBER) > :2)");
    }

    @Test
    void shouldRenderOrExpression() {
        var expr = new LogicalExpression(
            LogicalOp.OR,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("type"), LiteralExpression.of("A")),
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("type"), LiteralExpression.of("B"))
            )
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(
            "(JSON_VALUE(data, '$.type') = :1) OR (JSON_VALUE(data, '$.type') = :2)");
    }

    @Test
    void shouldRenderNotExpression() {
        var expr = new LogicalExpression(
            LogicalOp.NOT,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("deleted"), LiteralExpression.of(true))
            )
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(
            "NOT (JSON_VALUE(data, '$.deleted') = :1)");
    }

    @Test
    void shouldRenderNestedLogicalExpression() {
        var expr = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("active"), LiteralExpression.of(true)),
                new LogicalExpression(
                    LogicalOp.OR,
                    List.of(
                        new ComparisonExpression(ComparisonOp.EQ,
                            FieldPathExpression.of("role"), LiteralExpression.of("admin")),
                        new ComparisonExpression(ComparisonOp.EQ,
                            FieldPathExpression.of("role"), LiteralExpression.of("moderator"))
                    )
                )
            )
        );

        expr.render(context);

        assertThat(context.toSql()).contains("AND");
        assertThat(context.toSql()).contains("OR");
    }

    @Test
    void shouldHandleSingleOperandAnd() {
        var expr = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("status"), LiteralExpression.of("active"))
            )
        );

        expr.render(context);

        // Single operand should render without AND
        assertThat(context.toSql()).doesNotContain("AND");
        assertThat(context.toSql()).isEqualTo("(JSON_VALUE(data, '$.status') = :1)");
    }

    @Test
    void shouldRenderNorExpression() {
        var expr = new LogicalExpression(
            LogicalOp.NOR,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("status"), LiteralExpression.of("deleted")),
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("status"), LiteralExpression.of("archived"))
            )
        );

        expr.render(context);

        assertThat(context.toSql()).startsWith("NOT (");
        assertThat(context.toSql()).contains("OR");
    }

    @Test
    void shouldRejectEmptyOperands() {
        assertThatThrownBy(() -> new LogicalExpression(LogicalOp.AND, List.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one operand");
    }

    @Test
    void shouldRejectNotWithMultipleOperands() {
        assertThatThrownBy(() -> new LogicalExpression(
            LogicalOp.NOT,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("a"), LiteralExpression.of(1)),
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("b"), LiteralExpression.of(2))
            )
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exactly one operand");
    }

    @Test
    void shouldReturnOperator() {
        var expr = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("status"), LiteralExpression.of("active"))
            )
        );

        assertThat(expr.getOp()).isEqualTo(LogicalOp.AND);
    }

    @Test
    void shouldReturnOperands() {
        var operand = new ComparisonExpression(ComparisonOp.EQ,
            FieldPathExpression.of("status"), LiteralExpression.of("active"));
        var expr = new LogicalExpression(LogicalOp.AND, List.of(operand));

        assertThat(expr.getOperands()).containsExactly(operand);
    }

    @Test
    void shouldRenderThreeOperandAnd() {
        var expr = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("a"), LiteralExpression.of(1)),
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("b"), LiteralExpression.of(2)),
                new ComparisonExpression(ComparisonOp.EQ,
                    FieldPathExpression.of("c"), LiteralExpression.of(3))
            )
        );

        expr.render(context);

        // Should have two ANDs
        String sql = context.toSql();
        int andCount = sql.split("AND").length - 1;
        assertThat(andCount).isEqualTo(2);
    }
}
