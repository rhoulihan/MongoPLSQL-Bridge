/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.OutStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutStageParserTest {

    private OutStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new OutStageParser();
    }

    @Test
    void shouldParseSimpleStringForm() {
        OutStage stage = parser.parse("outputCollection");

        assertThat(stage.getTargetCollection()).isEqualTo("outputCollection");
        assertThat(stage.hasTargetDatabase()).isFalse();
    }

    @Test
    void shouldParseDocumentFormWithCollOnly() {
        var doc = Document.parse("""
            {
                "coll": "targetCollection"
            }
            """);

        OutStage stage = parser.parse(doc);

        assertThat(stage.getTargetCollection()).isEqualTo("targetCollection");
        assertThat(stage.hasTargetDatabase()).isFalse();
    }

    @Test
    void shouldParseDocumentFormWithDbAndColl() {
        var doc = Document.parse("""
            {
                "db": "myDatabase",
                "coll": "targetCollection"
            }
            """);

        OutStage stage = parser.parse(doc);

        assertThat(stage.getTargetCollection()).isEqualTo("targetCollection");
        assertThat(stage.hasTargetDatabase()).isTrue();
        assertThat(stage.getTargetDatabase()).isEqualTo("myDatabase");
    }

    @Test
    void shouldThrowOnMissingColl() {
        var doc = Document.parse("""
            {
                "db": "myDatabase"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("coll");
    }

    @Test
    void shouldThrowOnNonStringColl() {
        var doc = Document.parse("""
            {
                "coll": 123
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("coll")
            .withMessageContaining("string");
    }

    @Test
    void shouldThrowOnNullValue() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(null))
            .withMessageContaining("string or document");
    }

    @Test
    void shouldThrowOnInvalidType() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(123))
            .withMessageContaining("string or document");
    }
}
