/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticOp;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AddFieldsStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldCreateAddFieldsWithFields() {
        Map<String, Expression> fields = new LinkedHashMap<>();
        fields.put("total", LiteralExpression.of(100));

        var stage = new AddFieldsStage(fields);

        assertThat(stage.getFields()).hasSize(1);
        assertThat(stage.getFields()).containsKey("total");
    }

    @Test
    void shouldReturnOperatorNameAddFields() {
        var stage = new AddFieldsStage(Map.of("x", LiteralExpression.of(1)));

        assertThat(stage.getOperatorName()).isEqualTo("$addFields");
    }

    @Test
    void shouldRenderComputedField() {
        Map<String, Expression> fields = new LinkedHashMap<>();
        fields.put("doubleAmount", new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            List.of(FieldPathExpression.of("amount"), LiteralExpression.of(2))
        ));

        var stage = new AddFieldsStage(fields);
        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("doubleAmount");
        assertThat(sql).contains("$.amount");
    }

    @Test
    void shouldRenderMultipleFields() {
        Map<String, Expression> fields = new LinkedHashMap<>();
        fields.put("field1", LiteralExpression.of("value1"));
        fields.put("field2", LiteralExpression.of(42));

        var stage = new AddFieldsStage(fields);
        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("field1");
        assertThat(sql).contains("field2");
    }

    @Test
    void shouldRenderLiteralValue() {
        Map<String, Expression> fields = new LinkedHashMap<>();
        fields.put("status", LiteralExpression.of("active"));

        var stage = new AddFieldsStage(fields);
        stage.render(context);

        assertThat(context.toSql()).contains("status");
    }

    @Test
    void shouldRenderFieldReference() {
        Map<String, Expression> fields = new LinkedHashMap<>();
        fields.put("copyOfName", FieldPathExpression.of("name"));

        var stage = new AddFieldsStage(fields);
        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("copyOfName");
        assertThat(sql).contains("$.name");
    }

    @Test
    void shouldThrowOnNullFields() {
        assertThatNullPointerException()
            .isThrownBy(() -> new AddFieldsStage(null))
            .withMessageContaining("fields");
    }

    @Test
    void shouldProvideReadableToString() {
        Map<String, Expression> fields = new LinkedHashMap<>();
        fields.put("total", LiteralExpression.of(100));

        var stage = new AddFieldsStage(fields);

        assertThat(stage.toString())
            .contains("AddFieldsStage")
            .contains("total");
    }
}
