/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderConcat() {
        var expr = StringExpression.concat(List.of(
            LiteralExpression.of("Hello "),
            FieldPathExpression.of("name")
        ));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("(:1 || JSON_VALUE(data, '$.name'))");
    }

    @Test
    void shouldRenderConcatWithMultipleParts() {
        var expr = StringExpression.concat(List.of(
            FieldPathExpression.of("firstName"),
            LiteralExpression.of(" "),
            FieldPathExpression.of("lastName")
        ));

        expr.render(context);

        assertThat(context.toSql())
            .contains("JSON_VALUE(data, '$.firstName')")
            .contains(" || ")
            .contains("JSON_VALUE(data, '$.lastName')");
    }

    @Test
    void shouldRenderToLower() {
        var expr = StringExpression.toLower(FieldPathExpression.of("email"));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("LOWER(JSON_VALUE(data, '$.email'))");
    }

    @Test
    void shouldRenderToUpper() {
        var expr = StringExpression.toUpper(FieldPathExpression.of("code"));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("UPPER(JSON_VALUE(data, '$.code'))");
    }

    @Test
    void shouldRenderSubstr() {
        // MongoDB: {$substr: ["$text", 0, 5]} - 0-based index
        // Oracle: SUBSTR(text, 1, 5) - 1-based index
        var expr = StringExpression.substr(
            FieldPathExpression.of("text"),
            LiteralExpression.of(0),
            LiteralExpression.of(5)
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("SUBSTR(JSON_VALUE(data, '$.text'), :1, :2)");
        // Start index should be 1 (0 + 1 for Oracle's 1-based indexing)
        assertThat(context.getBindVariables()).containsExactly(1, 5);
    }

    @Test
    void shouldRenderSubstrWithNonZeroStart() {
        // MongoDB: {$substr: ["$text", 3, 10]} - start at index 3
        // Oracle: SUBSTR(text, 4, 10) - start at position 4
        var expr = StringExpression.substr(
            FieldPathExpression.of("text"),
            LiteralExpression.of(3),
            LiteralExpression.of(10)
        );

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("SUBSTR(JSON_VALUE(data, '$.text'), :1, :2)");
        assertThat(context.getBindVariables()).containsExactly(4, 10);
    }

    @Test
    void shouldRenderTrim() {
        var expr = StringExpression.trim(FieldPathExpression.of("input"));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("TRIM(JSON_VALUE(data, '$.input'))");
    }

    @Test
    void shouldRenderStrlen() {
        var expr = StringExpression.strlen(FieldPathExpression.of("name"));

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("LENGTH(JSON_VALUE(data, '$.name'))");
    }

    @Test
    void shouldReturnOp() {
        var expr = StringExpression.toLower(FieldPathExpression.of("x"));
        assertThat(expr.getOp()).isEqualTo(StringOp.TO_LOWER);
    }

    @Test
    void shouldReturnArguments() {
        var field = FieldPathExpression.of("x");
        var expr = StringExpression.toLower(field);
        assertThat(expr.getArguments()).containsExactly(field);
    }

    @Test
    void shouldProvideReadableToString() {
        var expr = StringExpression.toLower(FieldPathExpression.of("name"));
        assertThat(expr.toString()).contains("$toLower");
    }

    @Test
    void shouldRenderNestedStringOperations() {
        // {$toUpper: {$trim: "$name"}}
        var inner = StringExpression.trim(FieldPathExpression.of("name"));
        var outer = StringExpression.toUpper(inner);

        outer.render(context);

        assertThat(context.toSql()).isEqualTo("UPPER(TRIM(JSON_VALUE(data, '$.name')))");
    }
}
