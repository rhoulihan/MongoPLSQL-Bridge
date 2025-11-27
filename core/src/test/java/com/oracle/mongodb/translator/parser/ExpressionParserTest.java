/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;

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

    // Additional tests for parseValue method

    @Test
    void shouldParseValueNull() {
        Expression expr = parser.parseValue(null);
        assertThat(expr).isInstanceOf(LiteralExpression.class);
    }

    @Test
    void shouldParseValueFieldReference() {
        Expression expr = parser.parseValue("$fieldName");
        assertThat(expr).isInstanceOf(FieldPathExpression.class);
    }

    @Test
    void shouldParseValueNestedFieldReference() {
        Expression expr = parser.parseValue("$nested.field.path");
        assertThat(expr).isInstanceOf(FieldPathExpression.class);
    }

    @Test
    void shouldParseValueString() {
        Expression expr = parser.parseValue("plain string");
        assertThat(expr).isInstanceOf(LiteralExpression.class);
    }

    @Test
    void shouldParseValueNumber() {
        Expression expr = parser.parseValue(42);
        assertThat(expr).isInstanceOf(LiteralExpression.class);
    }

    @Test
    void shouldParseValueBoolean() {
        Expression expr = parser.parseValue(true);
        assertThat(expr).isInstanceOf(LiteralExpression.class);
    }

    @Test
    void shouldParseValueArray() {
        Expression expr = parser.parseValue(List.of(1, 2, 3));
        assertThat(expr).isInstanceOf(LiteralExpression.class);
    }

    @Test
    void shouldThrowOnUnsupportedValueType() {
        assertThatThrownBy(() -> parser.parseValue(new Object()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Unsupported value type");
    }

    @Test
    void shouldThrowOnEmptyExpressionDocument() {
        var doc = new Document();
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Empty expression document");
    }

    // Arithmetic expression tests

    @Test
    void shouldParseAddExpression() {
        var doc = Document.parse("{\"$add\": [\"$price\", \"$tax\"]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("+");
    }

    @Test
    void shouldParseSubtractExpression() {
        var doc = Document.parse("{\"$subtract\": [\"$total\", \"$discount\"]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("-");
    }

    @Test
    void shouldParseMultiplyExpression() {
        var doc = Document.parse("{\"$multiply\": [\"$quantity\", \"$price\"]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("*");
    }

    @Test
    void shouldParseDivideExpression() {
        var doc = Document.parse("{\"$divide\": [\"$total\", 2]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("/");
    }

    @Test
    void shouldParseModExpression() {
        var doc = Document.parse("{\"$mod\": [\"$value\", 10]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("MOD");
    }

    @Test
    void shouldThrowOnArithmeticNonArray() {
        var doc = Document.parse("{\"$add\": \"not an array\"}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires an array");
    }

    @Test
    void shouldThrowOnArithmeticLessThanTwoOperands() {
        var doc = Document.parse("{\"$add\": [1]}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires at least 2 operands");
    }

    // String expression tests

    @Test
    void shouldParseToLowerExpression() {
        var doc = Document.parse("{\"$toLower\": \"$name\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("LOWER");
    }

    @Test
    void shouldParseToUpperExpression() {
        var doc = Document.parse("{\"$toUpper\": \"$name\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("UPPER");
    }

    @Test
    void shouldParseTrimExpression() {
        var doc = Document.parse("{\"$trim\": \"$name\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("TRIM");
    }

    @Test
    void shouldParseLtrimExpression() {
        var doc = Document.parse("{\"$ltrim\": \"$name\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("LTRIM");
    }

    @Test
    void shouldParseRtrimExpression() {
        var doc = Document.parse("{\"$rtrim\": \"$name\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("RTRIM");
    }

    @Test
    void shouldParseStrLenCPExpression() {
        var doc = Document.parse("{\"$strLenCP\": \"$name\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("LENGTH");
    }

    @Test
    void shouldParseConcatExpression() {
        var doc = Document.parse("{\"$concat\": [\"$firstName\", \" \", \"$lastName\"]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("||");
    }

    @Test
    void shouldParseSubstrExpression() {
        var doc = Document.parse("{\"$substr\": [\"$name\", 0, 5]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("SUBSTR");
    }

    @Test
    void shouldThrowOnConcatNonArray() {
        var doc = Document.parse("{\"$concat\": \"not an array\"}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires an array");
    }

    // Conditional expression tests

    @Test
    void shouldParseCondArrayForm() {
        var doc = Document.parse("{\"$cond\": [true, \"in stock\", \"out of stock\"]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("CASE");
    }

    @Test
    void shouldParseCondDocumentForm() {
        var doc = Document.parse("""
            {
                "$cond": {
                    "if": true,
                    "then": "in stock",
                    "else": "out of stock"
                }
            }
            """);
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("CASE");
    }

    @Test
    void shouldThrowOnCondArrayWrongSize() {
        var doc = Document.parse("{\"$cond\": [\"$condition\", \"then\"]}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("3 elements");
    }

    @Test
    void shouldThrowOnCondDocumentMissingFields() {
        var doc = Document.parse("{\"$cond\": {\"if\": true}}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("'if', 'then', and 'else'");
    }

    @Test
    void shouldThrowOnCondInvalidType() {
        var doc = Document.parse("{\"$cond\": \"invalid\"}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("array or document");
    }

    @Test
    void shouldParseIfNullExpression() {
        var doc = Document.parse("{\"$ifNull\": [\"$description\", \"No description\"]}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("NVL");
    }

    @Test
    void shouldThrowOnIfNullNonArray() {
        var doc = Document.parse("{\"$ifNull\": \"invalid\"}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires an array");
    }

    @Test
    void shouldThrowOnIfNullWrongSize() {
        var doc = Document.parse("{\"$ifNull\": [\"$field\"]}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exactly 2 elements");
    }

    // Date expression tests

    @Test
    void shouldParseYearExpression() {
        var doc = Document.parse("{\"$year\": \"$createdAt\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("EXTRACT(YEAR");
    }

    @Test
    void shouldParseMonthExpression() {
        var doc = Document.parse("{\"$month\": \"$createdAt\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("EXTRACT(MONTH");
    }

    @Test
    void shouldParseDayOfMonthExpression() {
        var doc = Document.parse("{\"$dayOfMonth\": \"$createdAt\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("EXTRACT(DAY");
    }

    @Test
    void shouldParseHourExpression() {
        var doc = Document.parse("{\"$hour\": \"$createdAt\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("EXTRACT(HOUR");
    }

    @Test
    void shouldParseMinuteExpression() {
        var doc = Document.parse("{\"$minute\": \"$createdAt\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("EXTRACT(MINUTE");
    }

    @Test
    void shouldParseSecondExpression() {
        var doc = Document.parse("{\"$second\": \"$createdAt\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("EXTRACT(SECOND");
    }

    @Test
    void shouldParseDayOfWeekExpression() {
        var doc = Document.parse("{\"$dayOfWeek\": \"$createdAt\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("TO_NUMBER(TO_CHAR");
    }

    @Test
    void shouldParseDayOfYearExpression() {
        var doc = Document.parse("{\"$dayOfYear\": \"$createdAt\"}");
        Expression expr = parser.parseValue(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("DDD");
    }

    // Array expression tests

    @Test
    void shouldParseArrayElemAtExpression() {
        var doc = Document.parse("{\"$arrayElemAt\": [\"$items\", 0]}");
        Expression expr = parser.parseValue(doc);
        assertThat(expr).isNotNull();
    }

    @Test
    void shouldParseSizeExpression() {
        var doc = Document.parse("{\"$size\": \"$items\"}");
        Expression expr = parser.parseValue(doc);
        assertThat(expr).isNotNull();
    }

    @Test
    void shouldParseFirstExpression() {
        var doc = Document.parse("{\"$first\": \"$items\"}");
        Expression expr = parser.parseValue(doc);
        assertThat(expr).isNotNull();
    }

    @Test
    void shouldParseLastExpression() {
        var doc = Document.parse("{\"$last\": \"$items\"}");
        Expression expr = parser.parseValue(doc);
        assertThat(expr).isNotNull();
    }

    @Test
    void shouldThrowOnArrayElemAtNonArray() {
        var doc = Document.parse("{\"$arrayElemAt\": \"invalid\"}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires an array");
    }

    @Test
    void shouldThrowOnArrayElemAtWrongSize() {
        var doc = Document.parse("{\"$arrayElemAt\": [\"$items\"]}");
        assertThatThrownBy(() -> parser.parseValue(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exactly 2 arguments");
    }

    // Logical operator error cases

    @Test
    void shouldThrowOnTopLevelNotNonDocument() {
        var doc = Document.parse("{\"$not\": \"invalid\"}");
        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("$not requires a document");
    }

    @Test
    void shouldThrowOnAndNonArray() {
        var doc = Document.parse("{\"$and\": \"invalid\"}");
        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires an array");
    }

    @Test
    void shouldThrowOnFieldNotNonDocument() {
        var doc = Document.parse("{\"field\": {\"$not\": \"invalid\"}}");
        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("$not requires a document");
    }

    @Test
    void shouldThrowOnEmptyFilter() {
        var doc = new Document();
        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Empty filter document");
    }

    @Test
    void shouldThrowOnInNonArray() {
        var doc = Document.parse("{\"status\": {\"$in\": \"invalid\"}}");
        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("requires an array");
    }

    @Test
    void shouldThrowOnInWithEmptyArray() {
        var doc = Document.parse("{\"status\": {\"$in\": []}}");
        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one value");
    }

    @Test
    void shouldParseComparisonLt() {
        var doc = Document.parse("{\"count\": {\"$lt\": 100}}");
        var expr = parser.parse(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("<");
        assertThat(context.toSql()).doesNotContain("<=");
    }

    @Test
    void shouldParseExplicitEq() {
        var doc = Document.parse("{\"status\": {\"$eq\": \"active\"}}");
        var expr = parser.parse(doc);
        expr.render(context);
        assertThat(context.toSql()).contains("=");
    }
}
