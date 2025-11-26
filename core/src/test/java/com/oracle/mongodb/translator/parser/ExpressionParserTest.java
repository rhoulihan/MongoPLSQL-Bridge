/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ExpressionParserTest {

    private ExpressionParser parser;
    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        parser = new ExpressionParser();
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldParseSimpleEquality() {
        var doc = Document.parse("{\"status\": \"active\"}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.status') = :1");
        assertThat(context.getBindVariables()).containsExactly("active");
    }

    @Test
    void shouldParseComparisonOperatorGt() {
        var doc = Document.parse("{\"age\": {\"$gt\": 21}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("JSON_VALUE(data, '$.age'");
        assertThat(context.toSql()).contains("> :1");
    }

    @Test
    void shouldParseComparisonOperatorLte() {
        var doc = Document.parse("{\"price\": {\"$lte\": 99.99}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("<= :1");
    }

    @Test
    void shouldParseMultipleFieldsAsAnd() {
        var doc = Document.parse("{\"status\": \"active\", \"age\": {\"$gte\": 18}}");

        var expr = parser.parse(doc);

        assertThat(expr).isInstanceOf(LogicalExpression.class);
        var logical = (LogicalExpression) expr;
        assertThat(logical.getOp()).isEqualTo(LogicalOp.AND);
    }

    @Test
    void shouldParseExplicitAnd() {
        var doc = Document.parse("{\"$and\": [{\"status\": \"active\"}, {\"age\": {\"$gt\": 21}}]}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("AND");
    }

    @Test
    void shouldParseOr() {
        var doc = Document.parse("{\"$or\": [{\"type\": \"A\"}, {\"type\": \"B\"}]}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("OR");
    }

    @Test
    void shouldParseNestedPath() {
        var doc = Document.parse("{\"customer.address.city\": \"New York\"}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("$.customer.address.city");
    }

    @Test
    void shouldParseFieldLevelNot() {
        var doc = Document.parse("{\"status\": {\"$not\": {\"$eq\": \"deleted\"}}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("NOT");
    }

    @Test
    void shouldParseIn() {
        var doc = Document.parse("{\"status\": {\"$in\": [\"active\", \"pending\"]}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("IN");
        assertThat(context.toSql()).contains(":1");
        assertThat(context.toSql()).contains(":2");
    }

    @Test
    void shouldParseNotIn() {
        var doc = Document.parse("{\"status\": {\"$nin\": [\"deleted\", \"archived\"]}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("NOT IN");
    }

    @Test
    void shouldParseNe() {
        var doc = Document.parse("{\"status\": {\"$ne\": \"deleted\"}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("<>");
    }

    @Test
    void shouldParseNumericEquality() {
        var doc = Document.parse("{\"count\": 42}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("= :1");
        assertThat(context.getBindVariables()).containsExactly(42);
    }

    @Test
    void shouldParseBooleanEquality() {
        var doc = Document.parse("{\"active\": true}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("= :1");
        assertThat(context.getBindVariables()).containsExactly(true);
    }

    @Test
    void shouldParseNullEquality() {
        var doc = Document.parse("{\"deletedAt\": null}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("IS NULL");
    }

    @Test
    void shouldParseTopLevelNor() {
        var doc = Document.parse("{\"$nor\": [{\"status\": \"deleted\"}, {\"status\": \"archived\"}]}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).startsWith("NOT (");
        assertThat(context.toSql()).contains("OR");
    }

    @Test
    void shouldThrowOnUnsupportedOperator() {
        var doc = Document.parse("{\"field\": {\"$unsupported\": 1}}");

        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(UnsupportedOperatorException.class)
            .hasMessageContaining("$unsupported");
    }

    @Test
    void shouldParseMultipleComparisonOnSameField() {
        var doc = Document.parse("{\"age\": {\"$gte\": 18, \"$lte\": 65}}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains(">=");
        assertThat(context.toSql()).contains("<=");
        assertThat(context.toSql()).contains("AND");
    }

    @Test
    void shouldParseNestedAndOr() {
        var doc = Document.parse(
            "{\"$and\": [{\"active\": true}, {\"$or\": [{\"role\": \"admin\"}, {\"role\": \"manager\"}]}]}");

        var expr = parser.parse(doc);
        expr.render(context);

        assertThat(context.toSql()).contains("AND");
        assertThat(context.toSql()).contains("OR");
    }
}
