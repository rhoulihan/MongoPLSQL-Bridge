/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LookupStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldCreateLookupWithAllFields() {
        var stage = new LookupStage("inventory", "item", "sku", "inventory_docs");

        assertThat(stage.getFrom()).isEqualTo("inventory");
        assertThat(stage.getLocalField()).isEqualTo("item");
        assertThat(stage.getForeignField()).isEqualTo("sku");
        assertThat(stage.getAs()).isEqualTo("inventory_docs");
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new LookupStage("inventory", "item", "sku", "inventory_docs");

        assertThat(stage.getOperatorName()).isEqualTo("$lookup");
    }

    @Test
    void shouldRenderLeftOuterJoin() {
        var stage = new LookupStage("inventory", "item", "sku", "inventory_docs");

        stage.render(context);

        assertThat(context.toSql())
            .contains("LEFT OUTER JOIN")
            .contains("inventory")
            .contains("$.item")
            .contains("$.sku");
    }

    @Test
    void shouldRenderJoinConditionWithJsonValue() {
        var stage = new LookupStage("products", "productId", "_id", "product_info");

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("JSON_VALUE");
        assertThat(sql).contains("$.productId");
        assertThat(sql).contains("$._id");
    }

    @Test
    void shouldUseTableAlias() {
        var stage = new LookupStage("categories", "categoryId", "id", "category");

        stage.render(context);

        // The joined table should have an alias for clarity
        assertThat(context.toSql()).containsPattern("categories\\s+\\w+");
    }

    @Test
    void shouldHandleNestedFieldPath() {
        var stage = new LookupStage("users", "metadata.userId", "profile.id", "user_data");

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("$.metadata.userId");
        assertThat(sql).contains("$.profile.id");
    }

    @Test
    void shouldThrowOnNullFrom() {
        assertThatNullPointerException()
            .isThrownBy(() -> new LookupStage(null, "item", "sku", "docs"))
            .withMessageContaining("from");
    }

    @Test
    void shouldThrowOnNullLocalField() {
        assertThatNullPointerException()
            .isThrownBy(() -> new LookupStage("inventory", null, "sku", "docs"))
            .withMessageContaining("localField");
    }

    @Test
    void shouldThrowOnNullForeignField() {
        assertThatNullPointerException()
            .isThrownBy(() -> new LookupStage("inventory", "item", null, "docs"))
            .withMessageContaining("foreignField");
    }

    @Test
    void shouldThrowOnNullAs() {
        assertThatNullPointerException()
            .isThrownBy(() -> new LookupStage("inventory", "item", "sku", null))
            .withMessageContaining("as");
    }

    @Test
    void shouldProvideReadableToString() {
        var stage = new LookupStage("inventory", "item", "sku", "inventory_docs");

        assertThat(stage.toString())
            .contains("LookupStage")
            .contains("inventory")
            .contains("item")
            .contains("sku")
            .contains("inventory_docs");
    }
}
