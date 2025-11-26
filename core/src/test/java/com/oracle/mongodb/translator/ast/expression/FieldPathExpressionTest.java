/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FieldPathExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderSimpleFieldPath() {
        var expr = FieldPathExpression.of("status");

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.status')");
    }

    @Test
    void shouldRenderNestedFieldPath() {
        var expr = FieldPathExpression.of("customer.address.city");

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.customer.address.city')");
    }

    @Test
    void shouldHandleDollarPrefixedPath() {
        var expr = FieldPathExpression.of("$status");

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.status')");
    }

    @ParameterizedTest
    @CsvSource({
        "name, $.name",
        "$name, $.name",
        "user.email, $.user.email",
        "$user.email, $.user.email"
    })
    void shouldConvertToJsonPath(String input, String expectedPath) {
        var expr = FieldPathExpression.of(input);

        assertThat(expr.getJsonPath()).isEqualTo(expectedPath);
    }

    @Test
    void shouldRenderWithReturningClauseForNumbers() {
        var expr = FieldPathExpression.of("amount", JsonReturnType.NUMBER);

        expr.render(context);

        assertThat(context.toSql())
            .isEqualTo("JSON_VALUE(data, '$.amount' RETURNING NUMBER)");
    }

    @Test
    void shouldRenderWithCustomDataColumn() {
        var expr = FieldPathExpression.of("name", null, "doc");

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(doc, '$.name')");
    }

    @Test
    void shouldSupportEquality() {
        var expr1 = FieldPathExpression.of("status");
        var expr2 = FieldPathExpression.of("status");
        var expr3 = FieldPathExpression.of("other");

        assertThat(expr1).isEqualTo(expr2);
        assertThat(expr1).isNotEqualTo(expr3);
    }
}
