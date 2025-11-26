/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnwindStageParserTest {

    private UnwindStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new UnwindStageParser();
    }

    @Test
    void shouldParseSimpleStringPath() {
        // { $unwind: "$items" } - value is just a string
        UnwindStage stage = parser.parse("$items");

        assertThat(stage.getPath()).isEqualTo("items");
        assertThat(stage.getIncludeArrayIndex()).isNull();
        assertThat(stage.isPreserveNullAndEmptyArrays()).isFalse();
    }

    @Test
    void shouldParsePathWithoutDollarSign() {
        UnwindStage stage = parser.parse("items");

        assertThat(stage.getPath()).isEqualTo("items");
    }

    @Test
    void shouldParseNestedPath() {
        UnwindStage stage = parser.parse("$order.items");

        assertThat(stage.getPath()).isEqualTo("order.items");
    }

    @Test
    void shouldParseDocumentWithPathOnly() {
        var doc = Document.parse("""
            {
                "path": "$items"
            }
            """);

        UnwindStage stage = parser.parse(doc);

        assertThat(stage.getPath()).isEqualTo("items");
        assertThat(stage.getIncludeArrayIndex()).isNull();
        assertThat(stage.isPreserveNullAndEmptyArrays()).isFalse();
    }

    @Test
    void shouldParseDocumentWithAllOptions() {
        var doc = Document.parse("""
            {
                "path": "$items",
                "includeArrayIndex": "itemIndex",
                "preserveNullAndEmptyArrays": true
            }
            """);

        UnwindStage stage = parser.parse(doc);

        assertThat(stage.getPath()).isEqualTo("items");
        assertThat(stage.getIncludeArrayIndex()).isEqualTo("itemIndex");
        assertThat(stage.isPreserveNullAndEmptyArrays()).isTrue();
    }

    @Test
    void shouldParseDocumentWithPreserveNullFalse() {
        var doc = Document.parse("""
            {
                "path": "$items",
                "preserveNullAndEmptyArrays": false
            }
            """);

        UnwindStage stage = parser.parse(doc);

        assertThat(stage.isPreserveNullAndEmptyArrays()).isFalse();
    }

    @Test
    void shouldThrowOnMissingPath() {
        var doc = Document.parse("""
            {
                "includeArrayIndex": "idx"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("path");
    }

    @Test
    void shouldThrowOnInvalidPathType() {
        var doc = Document.parse("""
            {
                "path": 123
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("path")
            .withMessageContaining("string");
    }

    @Test
    void shouldThrowOnEmptyPath() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(""))
            .withMessageContaining("path");
    }

    @Test
    void shouldThrowOnDollarOnlyPath() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse("$"))
            .withMessageContaining("path");
    }
}
