/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.LookupStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LookupStageParserTest {

    private LookupStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new LookupStageParser();
    }

    @Test
    void shouldParseBasicLookup() {
        var doc = Document.parse("""
            {
                "from": "inventory",
                "localField": "item",
                "foreignField": "sku",
                "as": "inventory_docs"
            }
            """);

        LookupStage stage = parser.parse(doc);

        assertThat(stage.getFrom()).isEqualTo("inventory");
        assertThat(stage.getLocalField()).isEqualTo("item");
        assertThat(stage.getForeignField()).isEqualTo("sku");
        assertThat(stage.getAs()).isEqualTo("inventory_docs");
    }

    @Test
    void shouldParseWithNestedFields() {
        var doc = Document.parse("""
            {
                "from": "users",
                "localField": "metadata.authorId",
                "foreignField": "profile.userId",
                "as": "author"
            }
            """);

        LookupStage stage = parser.parse(doc);

        assertThat(stage.getLocalField()).isEqualTo("metadata.authorId");
        assertThat(stage.getForeignField()).isEqualTo("profile.userId");
    }

    @Test
    void shouldThrowOnMissingFrom() {
        var doc = Document.parse("""
            {
                "localField": "item",
                "foreignField": "sku",
                "as": "docs"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("from");
    }

    @Test
    void shouldThrowOnMissingLocalField() {
        var doc = Document.parse("""
            {
                "from": "inventory",
                "foreignField": "sku",
                "as": "docs"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("localField");
    }

    @Test
    void shouldThrowOnMissingForeignField() {
        var doc = Document.parse("""
            {
                "from": "inventory",
                "localField": "item",
                "as": "docs"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("foreignField");
    }

    @Test
    void shouldThrowOnMissingAs() {
        var doc = Document.parse("""
            {
                "from": "inventory",
                "localField": "item",
                "foreignField": "sku"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("as");
    }

    @Test
    void shouldThrowOnNonStringFrom() {
        var doc = Document.parse("""
            {
                "from": 123,
                "localField": "item",
                "foreignField": "sku",
                "as": "docs"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("from")
            .withMessageContaining("string");
    }
}
