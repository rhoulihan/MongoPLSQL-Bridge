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
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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

    assertThat(context.toSql()).isEqualTo("data.status = :1");
    assertThat(context.getBindVariables()).containsExactly("active");
  }

  @Test
  void shouldParseComparisonOperatorGt() {
    var doc = Document.parse("{\"age\": {\"$gt\": 21}}");

    var expr = parser.parse(doc);
    expr.render(context);

    assertThat(context.toSql()).contains("data.age");
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

    assertThat(context.toSql()).contains("data.customer.address.city");
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

    // Uses dot notation with IS NULL for null comparison
    assertThat(context.toSql()).contains("data.deletedAt IS NULL");
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
    var doc =
        Document.parse(
            "{\"$and\": [{\"active\": true}, {\"$or\": [{\"role\": \"admin\"}, {\"role\":"
                + " \"manager\"}]}]}");

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
    // $ltrim renders as TRIM() - uses Oracle standard TRIM function
    assertThat(context.toSql()).contains("TRIM(");
  }

  @Test
  void shouldParseRtrimExpression() {
    var doc = Document.parse("{\"$rtrim\": \"$name\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    // $rtrim renders as TRIM() - uses Oracle standard TRIM function
    assertThat(context.toSql()).contains("TRIM(");
  }

  @Test
  void shouldParseStrLenCpExpression() {
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
    var doc =
        Document.parse(
            """
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

  // Type conversion expression tests

  @Test
  void shouldParseToIntExpression() {
    var doc = Document.parse("{\"$toInt\": \"$price\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("TRUNC(TO_NUMBER(");
  }

  @Test
  void shouldParseToLongExpression() {
    var doc = Document.parse("{\"$toLong\": \"$count\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("TRUNC(TO_NUMBER(");
  }

  @Test
  void shouldParseToDoubleExpression() {
    var doc = Document.parse("{\"$toDouble\": \"$amount\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("TO_BINARY_DOUBLE(");
  }

  @Test
  void shouldParseToDecimalExpression() {
    var doc = Document.parse("{\"$toDecimal\": \"$total\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("TO_NUMBER(");
  }

  @Test
  void shouldParseToStringExpression() {
    var doc = Document.parse("{\"$toString\": \"$code\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("TO_CHAR(");
  }

  @Test
  void shouldParseToBoolExpression() {
    var doc = Document.parse("{\"$toBool\": \"$active\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("CASE WHEN");
  }

  @Test
  void shouldParseToDateExpression() {
    var doc = Document.parse("{\"$toDate\": \"$timestamp\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("TO_TIMESTAMP_TZ(");
  }

  @Test
  void shouldParseTypeExpression() {
    var doc = Document.parse("{\"$type\": \"$field\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("CASE");
  }

  @Test
  void shouldParseIsNumberExpression() {
    var doc = Document.parse("{\"$isNumber\": \"$value\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("REGEXP_LIKE");
  }

  @Test
  void shouldParseConvertExpression() {
    var doc =
        Document.parse("{\"$convert\": {\"input\": \"$value\", \"to\": \"int\", \"onNull\": 0}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("NVL(");
  }

  @Test
  void shouldParseConvertWithoutOnNull() {
    var doc = Document.parse("{\"$convert\": {\"input\": \"$value\", \"to\": \"int\"}}");
    Expression expr = parser.parseValue(doc);
    assertThat(expr).isNotNull();
  }

  // Additional String Operators Tests

  @Test
  void shouldParseSplitExpression() {
    var doc = Document.parse("{\"$split\": [\"$text\", \",\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("REGEXP_SUBSTR");
  }

  @Test
  void shouldParseIndexOfCpExpression() {
    var doc = Document.parse("{\"$indexOfCP\": [\"$text\", \"abc\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("INSTR");
  }

  @Test
  void shouldParseIndexOfCpWithStartExpression() {
    var doc = Document.parse("{\"$indexOfCP\": [\"$text\", \"abc\", 5, 20]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("INSTR");
  }

  @Test
  void shouldParseRegexMatchExpression() {
    var doc =
        Document.parse("{\"$regexMatch\": {\"input\": \"$description\", \"regex\": \"pattern\"}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("REGEXP_LIKE");
  }

  @Test
  void shouldParseRegexMatchWithOptionsExpression() {
    var doc =
        Document.parse(
            "{\"$regexMatch\": {\"input\": \"$description\", \"regex\": \"pattern\", \"options\":"
                + " \"i\"}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("REGEXP_LIKE");
    // Options is passed as a bind variable
    assertThat(context.getBindVariables()).contains("i");
  }

  @Test
  void shouldParseRegexFindExpression() {
    var doc = Document.parse("{\"$regexFind\": {\"input\": \"$text\", \"regex\": \"\\\\d+\"}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("REGEXP_INSTR");
  }

  @Test
  void shouldParseReplaceOneExpression() {
    var doc =
        Document.parse(
            "{\"$replaceOne\": {\"input\": \"$text\", \"find\": \"foo\", \"replacement\":"
                + " \"bar\"}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("REGEXP_REPLACE");
  }

  @Test
  void shouldParseReplaceAllExpression() {
    var doc =
        Document.parse(
            "{\"$replaceAll\": {\"input\": \"$text\", \"find\": \"foo\", \"replacement\":"
                + " \"bar\"}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("REGEXP_REPLACE");
  }

  @Test
  void shouldThrowForInvalidRegexMatchDocument() {
    var doc = Document.parse("{\"$regexMatch\": {\"input\": \"$text\"}}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("regex");
  }

  @Test
  void shouldThrowForInvalidReplaceOneDocument() {
    var doc = Document.parse("{\"$replaceOne\": {\"input\": \"$text\", \"find\": \"foo\"}}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("replacement");
  }

  // Additional Array Operators Tests

  @Test
  void shouldParseConcatArraysExpression() {
    var doc = Document.parse("{\"$concatArrays\": [\"$arr1\", \"$arr2\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldParseSliceExpression() {
    var doc = Document.parse("{\"$slice\": [\"$items\", 3]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_QUERY");
  }

  @Test
  void shouldParseSliceWithSkipExpression() {
    var doc = Document.parse("{\"$slice\": [\"$items\", 2, 5]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_QUERY");
  }

  @Test
  void shouldParseFilterExpression() {
    var doc =
        Document.parse("{\"$filter\": {\"input\": \"$items\", \"as\": \"item\", \"cond\": true}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldParseMapExpression() {
    var doc =
        Document.parse("{\"$map\": {\"input\": \"$items\", \"as\": \"item\", \"in\": \"$item\"}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldParseReduceExpression() {
    var doc =
        Document.parse("{\"$reduce\": {\"input\": \"$items\", \"initialValue\": 0, \"in\": 1}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    // $reduce is complex and renders a placeholder
    assertThat(context.toSql()).contains("NULL");
  }

  @Test
  void shouldThrowForInvalidSliceArgs() {
    var doc = Document.parse("{\"$slice\": [\"$items\"]}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2 or 3 arguments");
  }

  @Test
  void shouldThrowForInvalidFilterDocument() {
    var doc = Document.parse("{\"$filter\": {\"input\": \"$items\"}}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("cond");
  }

  // $literal expression tests

  @Test
  void shouldParseLiteralStringExpression() {
    var doc = Document.parse("{\"$literal\": \"employee\"}");
    Expression expr = parser.parseValue(doc);
    assertThat(expr).isInstanceOf(LiteralExpression.class);
    expr.render(context);
    assertThat(context.toSql()).isEqualTo(":1");
    assertThat(context.getBindVariables()).containsExactly("employee");
  }

  @Test
  void shouldParseLiteralNumberExpression() {
    var doc = Document.parse("{\"$literal\": 42}");
    Expression expr = parser.parseValue(doc);
    assertThat(expr).isInstanceOf(LiteralExpression.class);
    expr.render(context);
    assertThat(context.toSql()).isEqualTo(":1");
    assertThat(context.getBindVariables()).containsExactly(42);
  }

  @Test
  void shouldParseLiteralFieldPathAsLiteral() {
    // $literal prevents field path interpretation - "$field" is treated as literal string
    var doc = Document.parse("{\"$literal\": \"$notAFieldPath\"}");
    Expression expr = parser.parseValue(doc);
    assertThat(expr).isInstanceOf(LiteralExpression.class);
    expr.render(context);
    assertThat(context.toSql()).isEqualTo(":1");
    assertThat(context.getBindVariables()).containsExactly("$notAFieldPath");
  }

  // $switch expression tests

  @Test
  void shouldParseSwitchExpression() {
    var doc =
        Document.parse(
            "{\"$switch\": {"
                + "\"branches\": ["
                + "  {\"case\": {\"$eq\": [\"$status\", \"A\"]}, \"then\": \"Active\"},"
                + "  {\"case\": {\"$eq\": [\"$status\", \"B\"]}, \"then\": \"Blocked\"}"
                + "],"
                + "\"default\": \"Unknown\""
                + "}}");
    Expression expr = parser.parseValue(doc);
    assertThat(expr)
        .isInstanceOf(com.oracle.mongodb.translator.ast.expression.SwitchExpression.class);
    expr.render(context);
    assertThat(context.toSql())
        .contains("CASE")
        .contains("WHEN")
        .contains("THEN")
        .contains("ELSE")
        .contains("END");
  }

  @Test
  void shouldParseSwitchWithoutDefault() {
    var doc =
        Document.parse(
            "{\"$switch\": {"
                + "\"branches\": ["
                + "  {\"case\": {\"$gt\": [\"$score\", 90]}, \"then\": \"A\"}"
                + "]"
                + "}}");
    Expression expr = parser.parseValue(doc);
    assertThat(expr)
        .isInstanceOf(com.oracle.mongodb.translator.ast.expression.SwitchExpression.class);
    expr.render(context);
    assertThat(context.toSql()).contains("CASE").contains("WHEN").contains("THEN").contains("END");
    assertThat(context.toSql()).doesNotContain("ELSE");
  }

  @Test
  void shouldThrowForSwitchWithoutBranches() {
    var doc = Document.parse("{\"$switch\": {\"default\": \"Unknown\"}}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("branches");
  }

  // New array operator tests: $reverseArray, $sortArray, $in, $isArray, $indexOfArray

  @Test
  void shouldParseReverseArrayExpression() {
    var doc = Document.parse("{\"$reverseArray\": \"$items\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_ARRAYAGG").contains("ORDER BY").contains("DESC");
  }

  @Test
  void shouldParseSortArrayExpression() {
    var doc = Document.parse("{\"$sortArray\": {\"input\": \"$scores\", \"sortBy\": 1}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_ARRAYAGG").contains("ORDER BY");
  }

  @Test
  void shouldParseSortArrayDescendingExpression() {
    var doc = Document.parse("{\"$sortArray\": {\"input\": \"$scores\", \"sortBy\": -1}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_ARRAYAGG").contains("ORDER BY").contains("DESC");
  }

  @Test
  void shouldParseInArrayExpression() {
    var doc = Document.parse("{\"$in\": [\"apple\", \"$fruits\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_EXISTS");
  }

  @Test
  void shouldParseIsArrayExpression() {
    var doc = Document.parse("{\"$isArray\": \"$items\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("CASE WHEN").contains("JSON_EXISTS");
  }

  @Test
  void shouldParseIndexOfArrayExpression() {
    var doc = Document.parse("{\"$indexOfArray\": [\"$items\", \"needle\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_TABLE");
  }

  @Test
  void shouldParseIndexOfArrayWithRangeExpression() {
    var doc = Document.parse("{\"$indexOfArray\": [\"$items\", \"needle\", 2, 5]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_TABLE");
  }

  @Test
  void shouldThrowForInvalidSortArrayDocument() {
    var doc = Document.parse("{\"$sortArray\": {\"input\": \"$scores\"}}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sortBy");
  }

  @Test
  void shouldThrowForInvalidInArrayArgs() {
    var doc = Document.parse("{\"$in\": [\"apple\"]}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2 arguments");
  }

  @Test
  void shouldThrowForInvalidIndexOfArrayArgs() {
    var doc = Document.parse("{\"$indexOfArray\": [\"$items\"]}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2");
  }

  // Set operator tests: $setUnion, $setIntersection, $setDifference, $setEquals, $setIsSubset

  @Test
  void shouldParseSetUnionExpression() {
    var doc = Document.parse("{\"$setUnion\": [\"$arr1\", \"$arr2\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("UNION").contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldParseSetIntersectionExpression() {
    var doc = Document.parse("{\"$setIntersection\": [\"$arr1\", \"$arr2\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("INTERSECT").contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldParseSetDifferenceExpression() {
    var doc = Document.parse("{\"$setDifference\": [\"$arr1\", \"$arr2\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("MINUS").contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldParseSetEqualsExpression() {
    var doc = Document.parse("{\"$setEquals\": [\"$arr1\", \"$arr2\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("CASE");
  }

  @Test
  void shouldParseSetIsSubsetExpression() {
    var doc = Document.parse("{\"$setIsSubset\": [\"$arr1\", \"$arr2\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("CASE").contains("MINUS");
  }

  @Test
  void shouldThrowForInvalidSetDifferenceArgs() {
    var doc = Document.parse("{\"$setDifference\": [\"$arr1\"]}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2 arguments");
  }

  @Test
  void shouldThrowForInvalidSetIsSubsetArgs() {
    var doc = Document.parse("{\"$setIsSubset\": [\"$arr1\"]}");
    assertThatThrownBy(() -> parser.parseValue(doc))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("2 arguments");
  }

  // Object operator tests: $mergeObjects, $objectToArray, $arrayToObject

  @Test
  void shouldParseMergeObjectsExpression() {
    var doc = Document.parse("{\"$mergeObjects\": [\"$obj1\", \"$obj2\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_MERGEPATCH");
  }

  @Test
  void shouldParseMergeObjectsSingleObject() {
    var doc = Document.parse("{\"$mergeObjects\": \"$obj\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_QUERY");
  }

  @Test
  void shouldParseObjectToArrayExpression() {
    var doc = Document.parse("{\"$objectToArray\": \"$obj\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldParseArrayToObjectExpression() {
    var doc = Document.parse("{\"$arrayToObject\": \"$arr\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("JSON_OBJECTAGG");
  }

  // $isString tests

  @Test
  void shouldParseIsStringExpression() {
    var doc = Document.parse("{\"$isString\": \"$name\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    // Should generate CASE WHEN with type check
    assertThat(context.toSql()).contains("CASE");
  }

  @Test
  void shouldParseIsStringWithNestedField() {
    var doc = Document.parse("{\"$isString\": \"$user.name\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("user.name");
  }

  // NULL and Edge Case Handling Tests

  @Test
  void shouldHandleNullInComparison() {
    var doc = Document.parse("{\"status\": null}");
    var expr = parser.parse(doc);
    expr.render(context);
    // Uses dot notation with IS NULL for null comparison
    assertThat(context.toSql()).contains("data.status IS NULL");
  }

  @Test
  void shouldHandleNotEqualNull() {
    var doc = Document.parse("{\"status\": {\"$ne\": null}}");
    var expr = parser.parse(doc);
    expr.render(context);
    // Uses dot notation with IS NOT NULL for not-null comparison
    assertThat(context.toSql()).contains("data.status IS NOT NULL");
  }

  @Test
  void shouldHandleIfNullExpression() {
    var doc = Document.parse("{\"$ifNull\": [\"$name\", \"Unknown\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    assertThat(context.toSql()).containsAnyOf("COALESCE", "NVL");
  }

  @Test
  void shouldHandleEmptyArrayLiteral() {
    Expression expr = parser.parseValue(List.of());
    assertThat(expr).isInstanceOf(LiteralExpression.class);
    expr.render(context);
    // Empty array should render properly
    assertThat(context.toSql()).isNotEmpty();
  }

  @Test
  void shouldHandleEmptyStringLiteral() {
    Expression expr = parser.parseValue("");
    assertThat(expr).isInstanceOf(LiteralExpression.class);
    expr.render(context);
    assertThat(context.toSql()).contains(":1");
    assertThat(context.getBindVariables()).contains("");
  }

  @Test
  void shouldHandleZeroValue() {
    var doc = Document.parse("{\"count\": 0}");
    var expr = parser.parse(doc);
    expr.render(context);
    assertThat(context.toSql()).contains(":1");
    assertThat(context.getBindVariables()).contains(0);
  }

  @Test
  void shouldHandleNegativeNumber() {
    var doc = Document.parse("{\"balance\": {\"$lt\": -100}}");
    var expr = parser.parse(doc);
    expr.render(context);
    assertThat(context.toSql()).contains("<");
    assertThat(context.getBindVariables()).contains(-100);
  }

  @Test
  void shouldHandleDecimalNumber() {
    var doc = Document.parse("{\"price\": 19.99}");
    var expr = parser.parse(doc);
    expr.render(context);
    assertThat(context.getBindVariables()).contains(19.99);
  }

  @Test
  void shouldHandleBooleanTrue() {
    var doc = Document.parse("{\"active\": true}");
    var expr = parser.parse(doc);
    expr.render(context);
    assertThat(context.getBindVariables()).contains(true);
  }

  @Test
  void shouldHandleBooleanFalse() {
    var doc = Document.parse("{\"deleted\": false}");
    var expr = parser.parse(doc);
    expr.render(context);
    assertThat(context.getBindVariables()).contains(false);
  }

  @Test
  void shouldHandleFieldPathWithRootPrefix() {
    // MongoDB uses $$ROOT for root document reference
    // The $$ROOT prefix should be handled - either stripped or preserved
    var doc = Document.parse("{\"$eq\": [\"$$ROOT.status\", \"active\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    // Should either contain ROOT or just the field status
    assertThat(context.toSql()).containsAnyOf("ROOT", "status");
  }

  @Test
  void shouldHandleNestedArrayAccess() {
    var doc = Document.parse("{\"$arrayElemAt\": [\"$items\", 0]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    // Should access first element of array
    assertThat(context.toSql()).containsAnyOf("$[0]", "[0]", "ARRAY");
  }

  @Test
  void shouldHandleNegativeArrayIndex() {
    var doc = Document.parse("{\"$arrayElemAt\": [\"$items\", -1]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    // Negative index means from end
    assertThat(context.toSql()).isNotEmpty();
  }

  // ==================== Newly Added Operators Tests ====================

  @Test
  void shouldRenderSwitchWithNestedConditions() {
    // Nested $switch: outer switch contains inner switch in then clause
    var doc =
        Document.parse(
            "{\"$switch\": {"
                + "\"branches\": ["
                + "  {\"case\": {\"$gt\": [\"$score\", 90]}, "
                + "   \"then\": {\"$switch\": {"
                + "     \"branches\": "
                + "[{\"case\": {\"$eq\": [\"$bonus\", true]}, \"then\": \"A+\"}],"
                + "     \"default\": \"A\""
                + "   }}},"
                + "  {\"case\": {\"$gt\": [\"$score\", 80]}, \"then\": \"B\"}"
                + "],"
                + "\"default\": \"C\""
                + "}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should have nested CASE statements
    assertThat(sql).contains("CASE");
    // Count occurrences of CASE - should be at least 2
    int caseCount = sql.split("CASE").length - 1;
    assertThat(caseCount).isGreaterThanOrEqualTo(2);
  }

  @Test
  void shouldRenderReverseArrayComplex() {
    // $reverseArray on a computed array from $concatArrays
    var doc =
        Document.parse(
            "{\"$reverseArray\": {\"$concatArrays\": [\"$arr1\", \"$arr2\"]}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should produce array reversal SQL
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).containsIgnoringCase("DESC");
  }

  @Test
  void shouldRenderSortArrayMultipleFields() {
    // $sortArray with object containing multiple sort fields
    var doc =
        Document.parse(
            "{\"$sortArray\": "
                + "{\"input\": \"$employees\", \"sortBy\": {\"department\": 1, \"salary\": -1}}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should have ORDER BY with multiple fields
    assertThat(sql).contains("ORDER BY");
    assertThat(sql).contains("JSON_ARRAYAGG");
  }

  @Test
  void shouldRenderInWithLargeArray() {
    // $in with a large literal array (100+ elements)
    StringBuilder arrayBuilder = new StringBuilder("[");
    for (int i = 0; i < 100; i++) {
      if (i > 0) {
        arrayBuilder.append(",");
      }
      arrayBuilder.append(i);
    }
    arrayBuilder.append("]");
    var doc =
        Document.parse(
            "{\"$in\": [\"$status\", " + arrayBuilder.toString() + "]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should check if value is in array
    assertThat(sql).containsAnyOf("IN", "JSON_EXISTS", "OR");
  }

  @Test
  void shouldRenderSetUnionMultiple() {
    // $setUnion with 3+ arrays
    var doc =
        Document.parse(
            "{\"$setUnion\": [\"$tags1\", \"$tags2\", \"$tags3\", \"$tags4\"]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should use UNION for set operations
    assertThat(sql).contains("UNION");
    // Should reference all arrays
    assertThat(sql).contains("tags1");
    assertThat(sql).contains("tags4");
  }

  @Test
  void shouldRenderSetOperationsNested() {
    // Nested set operations: $setDifference of $setUnion and $setIntersection
    var doc =
        Document.parse(
            "{\"$setDifference\": ["
                + "{\"$setUnion\": [\"$a\", \"$b\"]},"
                + "{\"$setIntersection\": [\"$c\", \"$d\"]}"
                + "]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should have nested set operations
    assertThat(sql).containsAnyOf("UNION", "INTERSECT", "MINUS", "EXCEPT");
  }

  @Test
  void shouldRenderMergeObjectsDeep() {
    // $mergeObjects with nested object fields
    var doc =
        Document.parse(
            "{\"$mergeObjects\": ["
                + "\"$profile\","
                + "\"$settings\","
                + "{\"lastUpdated\": \"$$NOW\"}"
                + "]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should use JSON merge functionality
    assertThat(sql).containsAnyOf("JSON_MERGEPATCH", "JSON_OBJECT");
  }

  @Test
  void shouldRenderObjectToArrayNested() {
    // $objectToArray on a nested field path
    var doc = Document.parse("{\"$objectToArray\": \"$user.preferences.display\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should access nested path and convert to array
    assertThat(sql).contains("user");
    assertThat(sql).contains("preferences");
  }

  @Test
  void shouldRenderIsStringWithExpressions() {
    // $isString with computed expression input
    var doc =
        Document.parse(
            "{\"$isString\": {\"$concat\": [\"$firstName\", \" \", \"$lastName\"]}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should check if result is string - always true for $concat result
    assertThat(sql).isNotEmpty();
  }

  @Test
  void shouldRenderTypeWithAllTypes() {
    // $type returns type name as string
    var doc = Document.parse("{\"$type\": \"$value\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should use CASE to determine type
    assertThat(sql).contains("CASE");
    // Should check for multiple types
    assertThat(sql).containsAnyOf("'string'", "'int'", "'null'", "'object'");
  }

  @Test
  void shouldRenderArrayToObjectFromPairs() {
    // $arrayToObject from array of [key, value] pairs
    var doc = Document.parse("{\"$arrayToObject\": \"$keyValuePairs\"}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should convert array to object
    assertThat(sql).containsAnyOf("JSON_OBJECT", "JSON_ARRAYAGG");
  }

  @Test
  void shouldRenderIndexOfArrayWithRange() {
    // $indexOfArray with start and end indices
    var doc =
        Document.parse(
            "{\"$indexOfArray\": [\"$items\", \"target\", 2, 10]}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should search within range
    assertThat(sql).contains("JSON_TABLE");
  }

  @Test
  void shouldCombineNewOperatorsInPipeline() {
    // Test parsing multiple new operators together in expressions
    var doc =
        Document.parse(
            "{\"$cond\": {"
                + "\"if\": {\"$isNumber\": \"$value\"},"
                + "\"then\": {\"$type\": \"$value\"},"
                + "\"else\": {\"$toString\": \"$value\"}"
                + "}}");
    Expression expr = parser.parseValue(doc);
    expr.render(context);
    String sql = context.toSql();
    // Should combine conditional with type operations
    assertThat(sql).contains("CASE");
    assertThat(sql).isNotEmpty();
  }
}
