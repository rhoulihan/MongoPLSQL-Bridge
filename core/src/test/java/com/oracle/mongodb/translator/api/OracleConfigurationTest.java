/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class OracleConfigurationTest {

    @Test
    void shouldCreateConfigurationWithCollectionName() {
        var config = OracleConfiguration.builder()
            .collectionName("orders")
            .build();

        assertThat(config.collectionName()).isEqualTo("orders");
    }

    @Test
    void shouldRequireCollectionName() {
        assertThatThrownBy(() -> OracleConfiguration.builder().build())
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("collectionName");
    }

    @Test
    void shouldDefaultDataColumnToData() {
        var config = OracleConfiguration.builder()
            .collectionName("orders")
            .build();

        assertThat(config.dataColumnName()).isEqualTo("data");
    }

    @Test
    void shouldAllowCustomDataColumnName() {
        var config = OracleConfiguration.builder()
            .collectionName("orders")
            .dataColumnName("json_doc")
            .build();

        assertThat(config.dataColumnName()).isEqualTo("json_doc");
    }

    @Test
    void shouldDefaultSchemaToNull() {
        var config = OracleConfiguration.builder()
            .collectionName("orders")
            .build();

        assertThat(config.schemaName()).isNull();
    }

    @Test
    void shouldAllowCustomSchema() {
        var config = OracleConfiguration.builder()
            .collectionName("orders")
            .schemaName("myschema")
            .build();

        assertThat(config.schemaName()).isEqualTo("myschema");
    }

    @Test
    void shouldReturnQualifiedTableName() {
        var config = OracleConfiguration.builder()
            .collectionName("orders")
            .schemaName("sales")
            .build();

        assertThat(config.qualifiedTableName()).isEqualTo("sales.orders");
    }

    @Test
    void shouldReturnSimpleTableNameWhenNoSchema() {
        var config = OracleConfiguration.builder()
            .collectionName("orders")
            .build();

        assertThat(config.qualifiedTableName()).isEqualTo("orders");
    }
}
