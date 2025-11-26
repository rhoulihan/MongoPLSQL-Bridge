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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SkipStageTest {

    @Test
    void shouldRenderOffsetClause() {
        var stage = new SkipStage(20);
        var context = new DefaultSqlGenerationContext();

        stage.render(context);

        assertThat(context.toSql()).isEqualTo("OFFSET 20 ROWS");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, -100})
    void shouldRejectNegativeSkip(int skip) {
        assertThatThrownBy(() -> new SkipStage(skip))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldAllowZeroSkip() {
        var stage = new SkipStage(0);
        var context = new DefaultSqlGenerationContext();

        stage.render(context);

        assertThat(context.toSql()).isEqualTo("OFFSET 0 ROWS");
    }

    @Test
    void shouldReturnSkipValue() {
        var stage = new SkipStage(50);

        assertThat(stage.getSkip()).isEqualTo(50);
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new SkipStage(10);

        assertThat(stage.getOperatorName()).isEqualTo("$skip");
    }
}
