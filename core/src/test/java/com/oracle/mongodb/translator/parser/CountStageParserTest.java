/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.CountStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CountStageParserTest {

    private CountStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new CountStageParser();
    }

    @Test
    void shouldParseCountWithFieldName() {
        CountStage stage = parser.parse("totalDocuments");

        assertThat(stage).isNotNull();
        assertThat(stage.getFieldName()).isEqualTo("totalDocuments");
    }

    @Test
    void shouldParseCountWithSimpleName() {
        CountStage stage = parser.parse("count");

        assertThat(stage.getFieldName()).isEqualTo("count");
    }

    @Test
    void shouldReturnCorrectOperatorName() {
        CountStage stage = parser.parse("myCount");

        assertThat(stage.getOperatorName()).isEqualTo("$count");
    }

    @Test
    void shouldThrowOnNullValue() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(null))
            .withMessageContaining("$count");
    }

    @Test
    void shouldThrowOnEmptyString() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(""))
            .withMessageContaining("$count");
    }

    @Test
    void shouldThrowOnNonStringValue() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(123))
            .withMessageContaining("$count");
    }

    @Test
    void shouldThrowOnDocumentValue() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(new org.bson.Document("key", "value")))
            .withMessageContaining("$count");
    }

    @Test
    void shouldThrowOnFieldNameStartingWithDollar() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse("$invalid"))
            .withMessageContaining("$");
    }
}
