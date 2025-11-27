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

    @Test
    void shouldCreateTranslatorWithOptions() {
        var options = TranslationOptions.builder()
            .prettyPrint(true)
            .strictMode(true)
            .build();

        var translatorWithOptions = AggregationTranslator.create(
            OracleConfiguration.builder()
                .collectionName("orders")
                .build(),
            options
        );

        assertThat(translatorWithOptions).isNotNull();
        assertThat(translatorWithOptions.getOptions()).isEqualTo(options);
    }

    @Test
    void shouldReturnConfiguration() {
        var config = OracleConfiguration.builder()
            .collectionName("products")
            .schemaName("inventory")
            .build();

        var customTranslator = AggregationTranslator.create(config);

        assertThat(customTranslator.getConfiguration()).isEqualTo(config);
        assertThat(customTranslator.getConfiguration().collectionName()).isEqualTo("products");
        assertThat(customTranslator.getConfiguration().schemaName()).isEqualTo("inventory");
    }

    @Test
    void shouldReturnDefaultOptions() {
        assertThat(translator.getOptions()).isNotNull();
    }

    @Test
    void shouldTranslateMatchPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\"}}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("WHERE")
            .contains("status");
        assertThat(result.bindVariables()).isNotEmpty();
    }

    @Test
    void shouldTranslateGroupPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$group\": {\"_id\": \"$category\", \"total\": {\"$sum\": \"$amount\"}}}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("GROUP BY")
            .contains("SUM");
    }

    @Test
    void shouldTranslateProjectPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$project\": {\"name\": 1, \"price\": 1}}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("AS name")
            .contains("AS price");
    }

    @Test
    void shouldTranslateSortPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$sort\": {\"price\": -1}}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("ORDER BY")
            .contains("DESC");
    }

    @Test
    void shouldTranslateComplexPipeline() {
        var pipeline = List.of(
            Document.parse("{\"$match\": {\"active\": true}}"),
            Document.parse("{\"$group\": {\"_id\": \"$category\", \"count\": {\"$count\": {}}}}"),
            Document.parse("{\"$sort\": {\"count\": -1}}"),
            Document.parse("{\"$limit\": 10}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("WHERE")
            .contains("GROUP BY")
            .contains("ORDER BY")
            .contains("FETCH FIRST");
    }

    @Test
    void shouldTranslateEmptyPipeline() {
        var pipeline = List.<Document>of();

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("SELECT")
            .contains("orders");
    }

    @Test
    void shouldReturnWarningsWhenApplicable() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 10}")
        );

        var result = translator.translate(pipeline);

        // Warnings list should exist (may be empty)
        assertThat(result.warnings()).isNotNull();
    }
}
