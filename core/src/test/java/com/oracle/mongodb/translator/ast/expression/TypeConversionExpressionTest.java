/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TypeConversionExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext(true, null);
    }

    // Factory method tests

    @Test
    void shouldCreateToIntExpression() {
        var expr = TypeConversionExpression.toInt(FieldPathExpression.of("price"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TO_INT);
        assertThat(expr.getArgument()).isInstanceOf(FieldPathExpression.class);
    }

    @Test
    void shouldCreateToLongExpression() {
        var expr = TypeConversionExpression.toLong(FieldPathExpression.of("count"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TO_LONG);
    }

    @Test
    void shouldCreateToDoubleExpression() {
        var expr = TypeConversionExpression.toDouble(FieldPathExpression.of("amount"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TO_DOUBLE);
    }

    @Test
    void shouldCreateToDecimalExpression() {
        var expr = TypeConversionExpression.toDecimal(FieldPathExpression.of("total"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TO_DECIMAL);
    }

    @Test
    void shouldCreateToStringExpression() {
        var expr = TypeConversionExpression.toStringExpr(FieldPathExpression.of("code"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TO_STRING);
    }

    @Test
    void shouldCreateToBoolExpression() {
        var expr = TypeConversionExpression.toBool(FieldPathExpression.of("active"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TO_BOOL);
    }

    @Test
    void shouldCreateToDateExpression() {
        var expr = TypeConversionExpression.toDate(FieldPathExpression.of("timestamp"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TO_DATE);
    }

    @Test
    void shouldCreateToObjectIdExpression() {
        var expr = TypeConversionExpression.toObjectId(FieldPathExpression.of("_id"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TO_OBJECT_ID);
    }

    @Test
    void shouldCreateTypeExpression() {
        var expr = TypeConversionExpression.type(FieldPathExpression.of("field"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.TYPE);
    }

    @Test
    void shouldCreateIsNumberExpression() {
        var expr = TypeConversionExpression.isNumber(FieldPathExpression.of("value"));

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.IS_NUMBER);
    }

    @Test
    void shouldCreateConvertExpression() {
        var expr = TypeConversionExpression.convert(
            FieldPathExpression.of("value"),
            LiteralExpression.of(0),
            LiteralExpression.ofNull()
        );

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.CONVERT);
        assertThat(expr.getOnError()).isNotNull();
        assertThat(expr.getOnNull()).isNotNull();
    }

    // Render tests

    @Test
    void shouldRenderToInt() {
        var expr = TypeConversionExpression.toInt(LiteralExpression.of("123"));
        expr.render(context);

        assertThat(context.toSql()).contains("TRUNC(TO_NUMBER(");
    }

    @Test
    void shouldRenderToLong() {
        var expr = TypeConversionExpression.toLong(LiteralExpression.of("456"));
        expr.render(context);

        assertThat(context.toSql()).contains("TRUNC(TO_NUMBER(");
    }

    @Test
    void shouldRenderToDouble() {
        var expr = TypeConversionExpression.toDouble(LiteralExpression.of("3.14"));
        expr.render(context);

        assertThat(context.toSql()).contains("TO_BINARY_DOUBLE(");
    }

    @Test
    void shouldRenderToDecimal() {
        var expr = TypeConversionExpression.toDecimal(LiteralExpression.of("99.99"));
        expr.render(context);

        assertThat(context.toSql()).contains("TO_NUMBER(");
    }

    @Test
    void shouldRenderToString() {
        var expr = TypeConversionExpression.toStringExpr(LiteralExpression.of(42));
        expr.render(context);

        assertThat(context.toSql()).contains("TO_CHAR(");
    }

    @Test
    void shouldRenderToBool() {
        var expr = TypeConversionExpression.toBool(LiteralExpression.of(1));
        expr.render(context);

        assertThat(context.toSql()).contains("CASE WHEN");
        assertThat(context.toSql()).contains("THEN 'false' ELSE 'true' END");
    }

    @Test
    void shouldRenderToDate() {
        var expr = TypeConversionExpression.toDate(LiteralExpression.of("2024-01-15T10:30:00.000Z"));
        expr.render(context);

        assertThat(context.toSql()).contains("TO_TIMESTAMP_TZ(");
    }

    @Test
    void shouldRenderToObjectId() {
        var expr = TypeConversionExpression.toObjectId(LiteralExpression.of("507f1f77bcf86cd799439011"));
        expr.render(context);

        // ObjectId is just passed through
        assertThat(context.toSql()).contains("507f1f77bcf86cd799439011");
    }

    @Test
    void shouldRenderType() {
        var expr = TypeConversionExpression.type(LiteralExpression.of("test"));
        expr.render(context);

        assertThat(context.toSql()).contains("CASE");
        assertThat(context.toSql()).contains("'null'");
        assertThat(context.toSql()).contains("'int'");
        assertThat(context.toSql()).contains("'string'");
    }

    @Test
    void shouldRenderIsNumber() {
        var expr = TypeConversionExpression.isNumber(LiteralExpression.of("123"));
        expr.render(context);

        assertThat(context.toSql()).contains("REGEXP_LIKE");
        assertThat(context.toSql()).contains("CASE WHEN");
    }

    @Test
    void shouldRenderConvertWithOnNull() {
        var expr = TypeConversionExpression.convert(
            FieldPathExpression.of("value"),
            null,
            LiteralExpression.of(0)
        );
        expr.render(context);

        assertThat(context.toSql()).contains("NVL(");
    }

    @Test
    void shouldRenderConvertWithoutOnNull() {
        var expr = TypeConversionExpression.convert(
            FieldPathExpression.of("value"),
            null,
            null
        );
        expr.render(context);

        assertThat(context.toSql()).doesNotContain("NVL(");
    }

    // Null validation tests

    @Test
    void shouldThrowOnNullOp() {
        assertThatNullPointerException()
            .isThrownBy(() -> new TypeConversionExpression(null, LiteralExpression.of(1)))
            .withMessageContaining("op");
    }

    @Test
    void shouldThrowOnNullArgument() {
        assertThatNullPointerException()
            .isThrownBy(() -> new TypeConversionExpression(TypeConversionOp.TO_INT, null))
            .withMessageContaining("argument");
    }

    // Equality tests

    @Test
    void shouldBeEqualWhenSame() {
        var expr1 = TypeConversionExpression.toInt(LiteralExpression.of("123"));
        var expr2 = TypeConversionExpression.toInt(LiteralExpression.of("123"));

        assertThat(expr1).isEqualTo(expr2);
        assertThat(expr1.hashCode()).isEqualTo(expr2.hashCode());
    }

    @Test
    void shouldNotBeEqualWhenDifferentOp() {
        var expr1 = TypeConversionExpression.toInt(LiteralExpression.of("123"));
        var expr2 = TypeConversionExpression.toLong(LiteralExpression.of("123"));

        assertThat(expr1).isNotEqualTo(expr2);
    }

    @Test
    void shouldNotBeEqualWhenDifferentArgument() {
        var expr1 = TypeConversionExpression.toInt(LiteralExpression.of("123"));
        var expr2 = TypeConversionExpression.toInt(LiteralExpression.of("456"));

        assertThat(expr1).isNotEqualTo(expr2);
    }

    @Test
    void shouldNotBeEqualToNull() {
        var expr = TypeConversionExpression.toInt(LiteralExpression.of("123"));

        assertThat(expr).isNotEqualTo(null);
    }

    @Test
    void shouldNotBeEqualToDifferentType() {
        var expr = TypeConversionExpression.toInt(LiteralExpression.of("123"));

        assertThat(expr).isNotEqualTo("not an expression");
    }

    // toString test

    @Test
    void shouldProvideReadableToString() {
        var expr = TypeConversionExpression.toInt(FieldPathExpression.of("price"));

        assertThat(expr.toString())
            .contains("TypeConversion")
            .contains("$toInt")
            .contains("price");
    }

    // Getters test

    @Test
    void shouldReturnCorrectGetters() {
        var argument = FieldPathExpression.of("value");
        var onError = LiteralExpression.of(0);
        var onNull = LiteralExpression.ofNull();

        var expr = new TypeConversionExpression(TypeConversionOp.CONVERT, argument, onError, onNull);

        assertThat(expr.getOp()).isEqualTo(TypeConversionOp.CONVERT);
        assertThat(expr.getArgument()).isEqualTo(argument);
        assertThat(expr.getOnError()).isEqualTo(onError);
        assertThat(expr.getOnNull()).isEqualTo(onNull);
    }
}
