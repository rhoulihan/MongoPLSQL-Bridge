/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.BucketStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BucketStageParserTest {

    private BucketStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new BucketStageParser();
    }

    @Test
    void shouldParseBasicBucket() {
        var doc = Document.parse("""
            {
                "groupBy": "$price",
                "boundaries": [0, 100, 200, 300]
            }
            """);

        BucketStage stage = parser.parse(doc);

        assertThat(stage.getBoundaries()).containsExactly(0, 100, 200, 300);
        assertThat(stage.hasDefault()).isFalse();
        assertThat(stage.getOutput()).isEmpty();
    }

    @Test
    void shouldParseWithDefault() {
        var doc = Document.parse("""
            {
                "groupBy": "$age",
                "boundaries": [0, 18, 65, 100],
                "default": "Other"
            }
            """);

        BucketStage stage = parser.parse(doc);

        assertThat(stage.hasDefault()).isTrue();
        assertThat(stage.getDefaultBucket()).isEqualTo("Other");
    }

    @Test
    void shouldParseWithNumericDefault() {
        var doc = Document.parse("""
            {
                "groupBy": "$score",
                "boundaries": [0, 50, 100],
                "default": -1
            }
            """);

        BucketStage stage = parser.parse(doc);

        assertThat(stage.getDefaultBucket()).isEqualTo(-1);
    }

    @Test
    void shouldParseWithOutput() {
        var doc = Document.parse("""
            {
                "groupBy": "$price",
                "boundaries": [0, 100, 200],
                "output": {
                    "count": { "$sum": 1 },
                    "total": { "$sum": "$amount" }
                }
            }
            """);

        BucketStage stage = parser.parse(doc);

        assertThat(stage.getOutput()).hasSize(2);
        assertThat(stage.getOutput()).containsKey("count");
        assertThat(stage.getOutput()).containsKey("total");
    }

    @Test
    void shouldParseComplexGroupBy() {
        var doc = Document.parse("""
            {
                "groupBy": { "$multiply": ["$price", 1.1] },
                "boundaries": [0, 100, 200]
            }
            """);

        BucketStage stage = parser.parse(doc);

        assertThat(stage.getGroupBy()).isNotNull();
    }

    @Test
    void shouldThrowOnMissingGroupBy() {
        var doc = Document.parse("""
            {
                "boundaries": [0, 100]
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("groupBy");
    }

    @Test
    void shouldThrowOnMissingBoundaries() {
        var doc = Document.parse("""
            {
                "groupBy": "$price"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("boundaries");
    }

    @Test
    void shouldThrowOnNonArrayBoundaries() {
        var doc = Document.parse("""
            {
                "groupBy": "$price",
                "boundaries": "invalid"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("boundaries")
            .withMessageContaining("array");
    }

    @Test
    void shouldThrowOnTooFewBoundaries() {
        var doc = Document.parse("""
            {
                "groupBy": "$price",
                "boundaries": [0]
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("at least 2");
    }

    @Test
    void shouldThrowOnNonDocumentOutput() {
        var doc = Document.parse("""
            {
                "groupBy": "$price",
                "boundaries": [0, 100],
                "output": "invalid"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("output")
            .withMessageContaining("document");
    }
}
