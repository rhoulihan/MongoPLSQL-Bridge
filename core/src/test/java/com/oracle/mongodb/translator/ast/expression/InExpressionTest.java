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

class InExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderInExpression() {
        var expr = InExpression.in(
            FieldPathExpression.of("status"),
            List.of("active", "pending")
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.status') IN (:1, :2)");
        assertThat(context.getBindVariables()).containsExactly("active", "pending");
    }

    @Test
    void shouldRenderNotInExpression() {
        var expr = InExpression.notIn(
            FieldPathExpression.of("status"),
            List.of("deleted", "archived")
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.status') NOT IN (:1, :2)");
    }

    @Test
    void shouldRenderInWithSingleValue() {
        var expr = InExpression.in(
            FieldPathExpression.of("type"),
            List.of("premium")
        );

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.type') IN (:1)");
    }

    @Test
    void shouldRenderInWithNumericValues() {
        var expr = InExpression.in(
            FieldPathExpression.of("priority", JsonReturnType.NUMBER),
            List.of(1, 2, 3)
        );

        expr.render(context);

        assertThat(context.toSql())
            .contains("RETURNING NUMBER")
            .contains("IN (:1, :2, :3)");
        assertThat(context.getBindVariables()).containsExactly(1, 2, 3);
    }

    @Test
    void shouldRejectEmptyValues() {
        assertThatThrownBy(() -> InExpression.in(
            FieldPathExpression.of("status"),
            List.of()
        ))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one value");
    }

    @Test
    void shouldReturnField() {
        var field = FieldPathExpression.of("status");
        var expr = InExpression.in(field, List.of("a"));

        assertThat(expr.getField()).isEqualTo(field);
    }

    @Test
    void shouldReturnValues() {
        var values = List.<Object>of("a", "b");
        var expr = InExpression.in(FieldPathExpression.of("status"), values);

        assertThat(expr.getValues()).containsExactly("a", "b");
    }

    @Test
    void shouldReturnNegatedStatus() {
        assertThat(InExpression.in(FieldPathExpression.of("s"), List.of("a")).isNegated())
            .isFalse();
        assertThat(InExpression.notIn(FieldPathExpression.of("s"), List.of("a")).isNegated())
            .isTrue();
    }

    @Test
    void shouldProvideReadableToString() {
        var expr = InExpression.in(FieldPathExpression.of("status"), List.of("a"));

        assertThat(expr.toString()).contains("In(");
    }

    @Test
    void shouldProvideReadableToStringForNotIn() {
        var expr = InExpression.notIn(FieldPathExpression.of("status"), List.of("a"));

        assertThat(expr.toString()).contains("NotIn(");
    }

    @Test
    void shouldThrowOnNullField() {
        assertThatThrownBy(() -> new InExpression(null, List.of("a"), false))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("field");
    }

    @Test
    void shouldThrowOnNullValues() {
        assertThatThrownBy(() -> new InExpression(FieldPathExpression.of("status"), null, false))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("values");
    }

    @Test
    void shouldImplementEquals() {
        var expr1 = InExpression.in(FieldPathExpression.of("status"), List.of("a", "b"));
        var expr2 = InExpression.in(FieldPathExpression.of("status"), List.of("a", "b"));
        var expr3 = InExpression.notIn(FieldPathExpression.of("status"), List.of("a", "b"));
        var expr4 = InExpression.in(FieldPathExpression.of("other"), List.of("a", "b"));
        var expr5 = InExpression.in(FieldPathExpression.of("status"), List.of("c"));

        assertThat(expr1).isEqualTo(expr2);
        assertThat(expr1).isNotEqualTo(expr3); // different negated
        assertThat(expr1).isNotEqualTo(expr4); // different field
        assertThat(expr1).isNotEqualTo(expr5); // different values
        assertThat(expr1).isNotEqualTo(null);
        assertThat(expr1).isNotEqualTo("string");
        assertThat(expr1).isEqualTo(expr1); // same instance
    }

    @Test
    void shouldImplementHashCode() {
        var expr1 = InExpression.in(FieldPathExpression.of("status"), List.of("a", "b"));
        var expr2 = InExpression.in(FieldPathExpression.of("status"), List.of("a", "b"));

        assertThat(expr1.hashCode()).isEqualTo(expr2.hashCode());
    }
}
