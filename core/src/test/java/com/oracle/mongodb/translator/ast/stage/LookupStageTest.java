/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import java.util.Map;
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
        var stage = LookupStage.equality("inventory", "item", "sku", "inventory_docs");

        assertThat(stage.getFrom()).isEqualTo("inventory");
        assertThat(stage.getLocalField()).isEqualTo("item");
        assertThat(stage.getForeignField()).isEqualTo("sku");
        assertThat(stage.getAs()).isEqualTo("inventory_docs");
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = LookupStage.equality("inventory", "item", "sku", "inventory_docs");

        assertThat(stage.getOperatorName()).isEqualTo("$lookup");
    }

    @Test
    void shouldRenderLeftOuterJoin() {
        var stage = LookupStage.equality("inventory", "item", "sku", "inventory_docs");

        stage.render(context);

        assertThat(context.toSql())
            .contains("LEFT OUTER JOIN")
            .contains("inventory")
            .contains("$.item")
            .contains("$.sku");
    }

    @Test
    void shouldRenderJoinConditionWithJsonValue() {
        var stage = LookupStage.equality("products", "productId", "_id", "product_info");

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("JSON_VALUE");
        assertThat(sql).contains("$.productId");
        assertThat(sql).contains("$._id");
    }

    @Test
    void shouldUseTableAlias() {
        var stage = LookupStage.equality("categories", "categoryId", "id", "category");

        stage.render(context);

        // The joined table should have an alias for clarity
        assertThat(context.toSql()).containsPattern("categories\\s+\\w+");
    }

    @Test
    void shouldHandleNestedFieldPath() {
        var stage = LookupStage.equality("users", "metadata.userId", "profile.id", "user_data");

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("$.metadata.userId");
        assertThat(sql).contains("$.profile.id");
    }

    @Test
    void shouldThrowOnNullFrom() {
        assertThatNullPointerException()
            .isThrownBy(() -> LookupStage.equality(null, "item", "sku", "docs"))
            .withMessageContaining("from");
    }

    @Test
    void shouldThrowOnNullLocalField() {
        assertThatNullPointerException()
            .isThrownBy(() -> LookupStage.equality("inventory", null, "sku", "docs"))
            .withMessageContaining("localField");
    }

    @Test
    void shouldThrowOnNullForeignField() {
        assertThatNullPointerException()
            .isThrownBy(() -> LookupStage.equality("inventory", "item", null, "docs"))
            .withMessageContaining("foreignField");
    }

    @Test
    void shouldThrowOnNullAs() {
        assertThatNullPointerException()
            .isThrownBy(() -> LookupStage.equality("inventory", "item", "sku", null))
            .withMessageContaining("as");
    }

    @Test
    void shouldProvideReadableToString() {
        var stage = LookupStage.equality("inventory", "item", "sku", "inventory_docs");

        assertThat(stage.toString())
            .contains("LookupStage")
            .contains("inventory")
            .contains("item")
            .contains("sku")
            .contains("inventory_docs");
    }

    @Test
    void shouldCreatePipelineFormLookup() {
        var letVars = Map.of("orderId", "id", "customerId", "customer_id");
        var dummyFilter = new ComparisonExpression(ComparisonOp.EQ,
            FieldPathExpression.of("status"), LiteralExpression.of("active"));
        var pipeline = List.<Stage>of(new MatchStage(dummyFilter));

        var stage = LookupStage.withPipeline("orders", letVars, pipeline, "order_details");

        assertThat(stage.getFrom()).isEqualTo("orders");
        assertThat(stage.getAs()).isEqualTo("order_details");
        assertThat(stage.getLetVariables()).hasSize(2);
        assertThat(stage.getPipeline()).hasSize(1);
        assertThat(stage.getLocalField()).isNull();
        assertThat(stage.getForeignField()).isNull();
    }

    @Test
    void shouldIdentifyPipelineForm() {
        var equalityStage = LookupStage.equality("inv", "a", "b", "c");
        var pipelineStage = LookupStage.withPipeline("inv", Map.of(), List.of(), "c");

        assertThat(equalityStage.isPipelineForm()).isFalse();
        assertThat(pipelineStage.isPipelineForm()).isTrue();
    }

    @Test
    void shouldHandleNullLetVariables() {
        var stage = LookupStage.withPipeline("orders", null, List.of(), "result");

        assertThat(stage.getLetVariables()).isEmpty();
    }

    @Test
    void shouldHandleNullPipeline() {
        var stage = LookupStage.withPipeline("orders", Map.of(), null, "result");

        assertThat(stage.getPipeline()).isEmpty();
    }

    @Test
    void shouldThrowOnRenderPipelineForm() {
        var stage = LookupStage.withPipeline("orders", Map.of("x", "y"), List.of(), "result");

        assertThatThrownBy(() -> stage.render(context))
            .isInstanceOf(UnsupportedOperatorException.class)
            .hasMessageContaining("$lookup")
            .hasMessageContaining("pipeline");
    }

    @Test
    void shouldProvideReadableToStringForPipelineForm() {
        var dummyFilter = new ComparisonExpression(ComparisonOp.EQ,
            FieldPathExpression.of("status"), LiteralExpression.of("active"));
        var stage = LookupStage.withPipeline("orders",
            Map.of("orderId", "id"),
            List.of(new MatchStage(dummyFilter)),
            "order_details");

        assertThat(stage.toString())
            .contains("LookupStage")
            .contains("orders")
            .contains("let=")
            .contains("pipeline=1 stages")
            .contains("order_details");
    }

    @Test
    void shouldReturnPipelineStages() {
        var dummyFilter = new ComparisonExpression(ComparisonOp.EQ,
            FieldPathExpression.of("status"), LiteralExpression.of("active"));
        var matchStage = new MatchStage(dummyFilter);
        var stage = LookupStage.withPipeline("orders", Map.of(), List.of(matchStage), "result");

        assertThat(stage.getPipeline()).containsExactly(matchStage);
    }
}
