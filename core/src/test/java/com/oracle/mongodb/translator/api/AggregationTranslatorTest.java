/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;

class AggregationTranslatorTest {

    private AggregationTranslator translator;

    @BeforeEach
    void setUp() {
        translator = AggregationTranslator.create(
            OracleConfiguration.builder()
                .collectionName("orders")
                .build()
        );
    }

    @Test
    void shouldCreateTranslatorInstance() {
        assertThat(translator).isNotNull();
    }

    @Test
    void shouldTranslateLimitPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 10}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("SELECT")
            .contains("orders")
            .contains("FETCH FIRST 10 ROWS ONLY");
    }

    @Test
    void shouldTranslateSkipPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 20}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("SELECT")
            .contains("orders")
            .contains("OFFSET 20 ROWS");
    }

    @Test
    void shouldTranslateSkipAndLimitPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 20}"),
            Document.parse("{\"$limit\": 10}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("SELECT")
            .contains("orders")
            .contains("OFFSET 20 ROWS")
            .contains("FETCH FIRST 10 ROWS ONLY");
    }

    @Test
    void shouldUseCustomDataColumn() {
        var customTranslator = AggregationTranslator.create(
            OracleConfiguration.builder()
                .collectionName("orders")
                .dataColumnName("json_doc")
                .build()
        );

        var pipeline = List.of(
            Document.parse("{\"$limit\": 5}")
        );

        var result = customTranslator.translate(pipeline);

        assertThat(result.sql()).contains("json_doc");
    }

    @Test
    void shouldUseSchemaQualifiedTableName() {
        var schemaTranslator = AggregationTranslator.create(
            OracleConfiguration.builder()
                .collectionName("orders")
                .schemaName("sales")
                .build()
        );

        var pipeline = List.of(
            Document.parse("{\"$limit\": 5}")
        );

        var result = schemaTranslator.translate(pipeline);

        assertThat(result.sql()).contains("sales.orders");
    }

    @Test
    void shouldReturnEmptyBindVariables() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 10}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.bindVariables()).isEmpty();
    }

    @Test
    void shouldReturnFullSupportCapability() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 10}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.capability()).isEqualTo(TranslationCapability.FULL_SUPPORT);
    }
}
