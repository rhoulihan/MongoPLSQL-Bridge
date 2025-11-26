/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnwindStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldCreateUnwindWithPath() {
        var stage = new UnwindStage("items");

        assertThat(stage.getPath()).isEqualTo("items");
        assertThat(stage.getIncludeArrayIndex()).isNull();
        assertThat(stage.isPreserveNullAndEmptyArrays()).isFalse();
    }

    @Test
    void shouldCreateUnwindWithAllOptions() {
        var stage = new UnwindStage("items", "itemIndex", true);

        assertThat(stage.getPath()).isEqualTo("items");
        assertThat(stage.getIncludeArrayIndex()).isEqualTo("itemIndex");
        assertThat(stage.isPreserveNullAndEmptyArrays()).isTrue();
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new UnwindStage("items");

        assertThat(stage.getOperatorName()).isEqualTo("$unwind");
    }

    @Test
    void shouldRenderJsonTable() {
        var stage = new UnwindStage("items");

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("JSON_TABLE");
        assertThat(sql).contains("$.items[*]");
        assertThat(sql).contains("COLUMNS");
    }

    @Test
    void shouldRenderNestedPath() {
        var stage = new UnwindStage("order.items");

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("$.order.items[*]");
    }

    @Test
    void shouldRenderWithArrayIndex() {
        var stage = new UnwindStage("items", "idx", false);

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("FOR ORDINALITY");
        assertThat(sql).contains("idx");
    }

    @Test
    void shouldRenderWithPreserveNullAndEmptyArrays() {
        var stage = new UnwindStage("items", null, true);

        stage.render(context);

        String sql = context.toSql();
        // Oracle uses OUTER for preserving nulls
        assertThat(sql).containsIgnoringCase("ERROR ON ERROR");
    }

    @Test
    void shouldThrowOnNullPath() {
        assertThatNullPointerException()
            .isThrownBy(() -> new UnwindStage(null))
            .withMessageContaining("path");
    }

    @Test
    void shouldProvideReadableToString() {
        var stage = new UnwindStage("items", "idx", true);

        assertThat(stage.toString())
            .contains("UnwindStage")
            .contains("items")
            .contains("idx")
            .contains("true");
    }
}
