/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;

class PipelineParserTest {

    private PipelineParser parser;

    @BeforeEach
    void setUp() {
        parser = new PipelineParser();
    }

    @Test
    void shouldParseLimitStage() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 10}")
        );

        var result = parser.parse("orders", pipeline);

        assertThat(result.getStages()).hasSize(1);
        assertThat(result.getStages().get(0)).isInstanceOf(LimitStage.class);
        assertThat(((LimitStage) result.getStages().get(0)).getLimit()).isEqualTo(10);
    }

    @Test
    void shouldParseSkipStage() {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 5}")
        );

        var result = parser.parse("orders", pipeline);

        assertThat(result.getStages()).hasSize(1);
        assertThat(result.getStages().get(0)).isInstanceOf(SkipStage.class);
        assertThat(((SkipStage) result.getStages().get(0)).getSkip()).isEqualTo(5);
    }

    @Test
    void shouldParseMultipleStages() {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 10}"),
            Document.parse("{\"$limit\": 5}")
        );

        var result = parser.parse("orders", pipeline);

        assertThat(result.getStages()).hasSize(2);
        assertThat(result.getStages().get(0)).isInstanceOf(SkipStage.class);
        assertThat(result.getStages().get(1)).isInstanceOf(LimitStage.class);
    }

    @Test
    void shouldThrowOnUnknownStage() {
        var pipeline = List.of(
            Document.parse("{\"$unknownOperator\": {}}")
        );

        assertThatThrownBy(() -> parser.parse("orders", pipeline))
            .isInstanceOf(UnsupportedOperatorException.class)
            .hasMessageContaining("$unknownOperator");
    }

    @Test
    void shouldSetCollectionName() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 1}")
        );

        var result = parser.parse("customers", pipeline);

        assertThat(result.getCollectionName()).isEqualTo("customers");
    }

    @Test
    void shouldParseEmptyPipeline() {
        var result = parser.parse("orders", List.of());

        assertThat(result.getStages()).isEmpty();
        assertThat(result.getCollectionName()).isEqualTo("orders");
    }

    @Test
    void shouldRejectStageWithMultipleKeys() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 10, \"$skip\": 5}")
        );

        assertThatThrownBy(() -> parser.parse("orders", pipeline))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("exactly one key");
    }
}
