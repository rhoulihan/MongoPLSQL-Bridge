/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.AccumulatorOp;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BucketStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldCreateWithRequiredFields() {
        var stage = new BucketStage(
            FieldPathExpression.of("price"),
            List.of(0, 100, 200, 300),
            null,
            Map.of()
        );

        assertThat(stage.getGroupBy()).isEqualTo(FieldPathExpression.of("price"));
        assertThat(stage.getBoundaries()).containsExactly(0, 100, 200, 300);
        assertThat(stage.hasDefault()).isFalse();
        assertThat(stage.getOutput()).isEmpty();
    }

    @Test
    void shouldCreateWithDefaultBucket() {
        var stage = new BucketStage(
            FieldPathExpression.of("age"),
            List.of(0, 18, 65),
            "Other",
            Map.of()
        );

        assertThat(stage.hasDefault()).isTrue();
        assertThat(stage.getDefaultBucket()).isEqualTo("Other");
    }

    @Test
    void shouldCreateWithOutputAccumulators() {
        var count = new AccumulatorExpression(AccumulatorOp.SUM, LiteralExpression.of(1));
        var stage = new BucketStage(
            FieldPathExpression.of("price"),
            List.of(0, 50, 100),
            null,
            Map.of("count", count)
        );

        assertThat(stage.getOutput()).hasSize(1);
        assertThat(stage.getOutput()).containsKey("count");
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new BucketStage(
            FieldPathExpression.of("price"),
            List.of(0, 100),
            null,
            Map.of()
        );

        assertThat(stage.getOperatorName()).isEqualTo("$bucket");
    }

    @Test
    void shouldRenderCaseExpression() {
        var stage = new BucketStage(
            FieldPathExpression.of("price"),
            List.of(0, 100, 200),
            "Other",
            Map.of()
        );

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("CASE");
        assertThat(sql).contains("WHEN");
        assertThat(sql).contains(">= 0");
        assertThat(sql).contains("< 100");
        assertThat(sql).contains("THEN 0");
        assertThat(sql).contains(">= 100");
        assertThat(sql).contains("< 200");
        assertThat(sql).contains("THEN 100");
        assertThat(sql).contains("ELSE 'Other'");
        assertThat(sql).contains("END AS _id");
    }

    @Test
    void shouldRenderWithoutDefaultBucket() {
        var stage = new BucketStage(
            FieldPathExpression.of("score"),
            List.of(0, 50, 100),
            null,
            Map.of()
        );

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("CASE");
        assertThat(sql).doesNotContain("ELSE");
        assertThat(sql).contains("END AS _id");
    }

    @Test
    void shouldRenderWithAccumulators() {
        var count = new AccumulatorExpression(AccumulatorOp.SUM, LiteralExpression.of(1));
        var total = new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("amount"));
        var stage = new BucketStage(
            FieldPathExpression.of("price"),
            List.of(0, 100),
            null,
            Map.of("count", count, "total", total)
        );

        stage.render(context);

        String sql = context.toSql();
        // SUM with literal uses bind variable (:1)
        assertThat(sql).contains("SUM(");
        assertThat(sql).contains("AS count");
        assertThat(sql).contains("AS total");
    }

    @Test
    void shouldThrowOnNullGroupBy() {
        assertThatNullPointerException()
            .isThrownBy(() -> new BucketStage(null, List.of(0, 100), null, Map.of()))
            .withMessageContaining("groupBy");
    }

    @Test
    void shouldThrowOnNullBoundaries() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new BucketStage(FieldPathExpression.of("x"), null, null, Map.of()))
            .withMessageContaining("boundaries");
    }

    @Test
    void shouldThrowOnTooFewBoundaries() {
        assertThatIllegalArgumentException()
            .isThrownBy(() -> new BucketStage(FieldPathExpression.of("x"), List.of(0), null, Map.of()))
            .withMessageContaining("at least 2");
    }

    @Test
    void shouldProvideReadableToString() {
        var stage = new BucketStage(
            FieldPathExpression.of("price"),
            List.of(0, 100, 200),
            "Other",
            Map.of()
        );

        assertThat(stage.toString())
            .contains("BucketStage")
            .contains("price")
            .contains("boundaries")
            .contains("Other");
    }
}
