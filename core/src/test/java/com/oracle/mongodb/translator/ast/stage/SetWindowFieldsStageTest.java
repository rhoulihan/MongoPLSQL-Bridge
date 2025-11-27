/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage.WindowField;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage.WindowSpec;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SetWindowFieldsStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldCreateWithMinimalFields() {
        var windowField = new WindowField("$sum", "$quantity", null);
        var stage = new SetWindowFieldsStage(
            null,
            null,
            Map.of("total", windowField)
        );

        assertThat(stage.getPartitionBy()).isNull();
        assertThat(stage.getSortBy()).isEmpty();
        assertThat(stage.getOutput()).hasSize(1);
    }

    @Test
    void shouldCreateWithAllOptions() {
        var windowSpec = new WindowSpec("documents", List.of("unbounded", "current"));
        var windowField = new WindowField("$sum", "$quantity", windowSpec);
        var stage = new SetWindowFieldsStage(
            "$state",
            Map.of("orderDate", 1),
            Map.of("cumulativeSum", windowField)
        );

        assertThat(stage.getPartitionBy()).isEqualTo("$state");
        assertThat(stage.getSortBy()).containsEntry("orderDate", 1);
        assertThat(stage.getOutput()).containsKey("cumulativeSum");
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new SetWindowFieldsStage(null, null, Map.of());

        assertThat(stage.getOperatorName()).isEqualTo("$setWindowFields");
    }

    @Test
    void shouldRenderRankWindowFunction() {
        var windowField = new WindowField("$rank", null, null);
        var stage = new SetWindowFieldsStage(
            "$state",
            Map.of("date", 1),
            Map.of("rank", windowField)
        );

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("RANK()");
        assertThat(sql).contains("OVER");
        assertThat(sql).contains("PARTITION BY");
        assertThat(sql).contains("ORDER BY");
        assertThat(sql).contains("AS rank");
    }

    @Test
    void shouldRenderSumWithWindow() {
        var windowSpec = new WindowSpec("documents", List.of("unbounded", "current"));
        var windowField = new WindowField("$sum", "$quantity", windowSpec);
        var stage = new SetWindowFieldsStage(
            "$state",
            Map.of("orderDate", 1),
            Map.of("cumulativeSum", windowField)
        );

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("SUM(");
        assertThat(sql).contains("OVER");
        assertThat(sql).contains("ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW");
        assertThat(sql).contains("AS cumulativeSum");
    }

    @Test
    void shouldRenderRowNumber() {
        var windowField = new WindowField("$rowNumber", null, null);
        var stage = new SetWindowFieldsStage(
            null,
            Map.of("date", 1),
            Map.of("rowNum", windowField)
        );

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("ROW_NUMBER()");
        assertThat(sql).contains("AS rowNum");
    }

    @Test
    void shouldRenderDenseRank() {
        var windowField = new WindowField("$denseRank", null, null);
        var stage = new SetWindowFieldsStage(
            "$department",
            Map.of("salary", -1),
            Map.of("denseRank", windowField)
        );

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("DENSE_RANK()");
        assertThat(sql).contains("DESC");
        assertThat(sql).contains("AS denseRank");
    }

    @Test
    void shouldThrowOnNullOutput() {
        assertThatNullPointerException()
            .isThrownBy(() -> new SetWindowFieldsStage(null, null, null))
            .withMessageContaining("output");
    }

    @Test
    void shouldProvideReadableToString() {
        var windowField = new WindowField("$sum", "$qty", null);
        var stage = new SetWindowFieldsStage(
            "$region",
            Map.of("date", -1),
            Map.of("total", windowField)
        );

        assertThat(stage.toString())
            .contains("SetWindowFieldsStage")
            .contains("partitionBy=$region")
            .contains("sortBy=")
            .contains("total");
    }
}
