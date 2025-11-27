/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.RedactStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RedactStageParserTest {

    private RedactStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new RedactStageParser();
    }

    @Test
    void shouldParseConditionalRedact() {
        var doc = Document.parse("""
            {
                "$cond": {
                    "if": {"$eq": ["$level", 5]},
                    "then": "$$PRUNE",
                    "else": "$$DESCEND"
                }
            }
            """);

        RedactStage stage = parser.parse(doc);

        assertThat(stage).isNotNull();
        assertThat(stage.getExpression()).isNotNull();
    }

    @Test
    void shouldParseDirectSystemVariable() {
        RedactStage stage = parser.parse("$$KEEP");

        assertThat(stage).isNotNull();
        assertThat(stage.getExpression()).isNotNull();
    }

    @Test
    void shouldReturnCorrectOperatorName() {
        var doc = Document.parse("""
            {
                "$cond": {
                    "if": {"$eq": ["$status", "public"]},
                    "then": "$$KEEP",
                    "else": "$$PRUNE"
                }
            }
            """);

        RedactStage stage = parser.parse(doc);

        assertThat(stage.getOperatorName()).isEqualTo("$redact");
    }

    @Test
    void shouldThrowOnInvalidValue() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(123))
            .withMessageContaining("$redact");
    }

    @Test
    void shouldThrowOnNullValue() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(null))
            .withMessageContaining("$redact");
    }
}
