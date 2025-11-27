/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;

class CountStageTest {

    @Test
    void shouldRenderCountClause() {
        var stage = new CountStage("totalCount");
        var context = new DefaultSqlGenerationContext();

        stage.render(context);

        assertThat(context.toSql()).contains("COUNT(*)");
        assertThat(context.toSql()).contains("totalCount");
    }

    @Test
    void shouldRenderAsJsonObject() {
        var stage = new CountStage("myCount");
        var context = new DefaultSqlGenerationContext();

        stage.render(context);

        // The output should create a JSON object with the field name as key
        assertThat(context.toSql()).contains("JSON_OBJECT");
        assertThat(context.toSql()).contains("myCount");
    }

    @Test
    void shouldRejectNullFieldName() {
        assertThatThrownBy(() -> new CountStage(null))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectEmptyFieldName() {
        assertThatThrownBy(() -> new CountStage(""))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldRejectFieldNameStartingWithDollar() {
        assertThatThrownBy(() -> new CountStage("$count"))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldReturnFieldName() {
        var stage = new CountStage("documentCount");

        assertThat(stage.getFieldName()).isEqualTo("documentCount");
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new CountStage("count");

        assertThat(stage.getOperatorName()).isEqualTo("$count");
    }

    @Test
    void shouldProvideToString() {
        var stage = new CountStage("results");

        assertThat(stage.toString()).isEqualTo("CountStage(results)");
    }
}
