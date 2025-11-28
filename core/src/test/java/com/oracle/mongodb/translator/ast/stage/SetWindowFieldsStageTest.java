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
    var stage = new SetWindowFieldsStage(null, null, Map.of("total", windowField));

    assertThat(stage.getPartitionBy()).isNull();
    assertThat(stage.getSortBy()).isEmpty();
    assertThat(stage.getOutput()).hasSize(1);
  }

  @Test
  void shouldCreateWithAllOptions() {
    var windowSpec = new WindowSpec("documents", List.of("unbounded", "current"));
    var windowField = new WindowField("$sum", "$quantity", windowSpec);
    var stage =
        new SetWindowFieldsStage(
            "$state", Map.of("orderDate", 1), Map.of("cumulativeSum", windowField));

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
    var stage = new SetWindowFieldsStage("$state", Map.of("date", 1), Map.of("rank", windowField));

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
    var stage =
        new SetWindowFieldsStage(
            "$state", Map.of("orderDate", 1), Map.of("cumulativeSum", windowField));

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
    var stage = new SetWindowFieldsStage(null, Map.of("date", 1), Map.of("rowNum", windowField));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("ROW_NUMBER()");
    assertThat(sql).contains("AS rowNum");
  }

  @Test
  void shouldRenderDenseRank() {
    var windowField = new WindowField("$denseRank", null, null);
    var stage =
        new SetWindowFieldsStage(
            "$department", Map.of("salary", -1), Map.of("denseRank", windowField));

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
    var stage =
        new SetWindowFieldsStage("$region", Map.of("date", -1), Map.of("total", windowField));

    assertThat(stage.toString())
        .contains("SetWindowFieldsStage")
        .contains("partitionBy=$region")
        .contains("sortBy=")
        .contains("total");
  }

  @Test
  void shouldRenderAvgWindowFunction() {
    var windowField = new WindowField("$avg", "$price", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("avgPrice", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("AVG(").contains("AS avgPrice");
  }

  @Test
  void shouldRenderMinWindowFunction() {
    var windowField = new WindowField("$min", "$value", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("minVal", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("MIN(").contains("AS minVal");
  }

  @Test
  void shouldRenderMaxWindowFunction() {
    var windowField = new WindowField("$max", "$value", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("maxVal", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("MAX(").contains("AS maxVal");
  }

  @Test
  void shouldRenderCountWindowFunction() {
    var windowField = new WindowField("$count", null, null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("cnt", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("COUNT(*)").contains("AS cnt");
  }

  @Test
  void shouldRenderFirstValueWindowFunction() {
    var windowField = new WindowField("$first", "$name", null);
    var stage = new SetWindowFieldsStage(null, Map.of("id", 1), Map.of("firstName", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("FIRST_VALUE(").contains("AS firstName");
  }

  @Test
  void shouldRenderLastValueWindowFunction() {
    var windowField = new WindowField("$last", "$name", null);
    var stage = new SetWindowFieldsStage(null, Map.of("id", 1), Map.of("lastName", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("LAST_VALUE(").contains("AS lastName");
  }

  @Test
  void shouldRenderShiftWindowFunction() {
    var windowField = new WindowField("$shift", "$value", null);
    var stage = new SetWindowFieldsStage(null, Map.of("id", 1), Map.of("prevVal", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("LAG(").contains("AS prevVal");
  }

  @Test
  void shouldRenderStdDevPopWindowFunction() {
    var windowField = new WindowField("$stdDevPop", "$score", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("stddev", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("STDDEV_POP(").contains("AS stddev");
  }

  @Test
  void shouldRenderStdDevSampWindowFunction() {
    var windowField = new WindowField("$stdDevSamp", "$score", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("stddev", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("STDDEV_SAMP(").contains("AS stddev");
  }

  @Test
  void shouldRenderDocumentNumberWindowFunction() {
    var windowField = new WindowField("$documentNumber", null, null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("docNum", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("ROW_NUMBER()").contains("AS docNum");
  }

  @Test
  void shouldRenderUnsupportedOperatorAsComment() {
    var windowField = new WindowField("$unknownOp", "$field", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("result", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("/* unsupported: $unknownOp */").contains("NULL");
  }

  @Test
  void shouldRenderRangeWindowFrame() {
    var windowSpec = new WindowSpec("range", List.of("unbounded", "current"));
    var windowField = new WindowField("$sum", "$amount", windowSpec);
    var stage = new SetWindowFieldsStage(null, Map.of("date", 1), Map.of("total", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("RANGE BETWEEN");
  }

  @Test
  void shouldRenderNumericWindowBounds() {
    var windowSpec = new WindowSpec("documents", List.of("-2", "2"));
    var windowField = new WindowField("$avg", "$value", windowSpec);
    var stage = new SetWindowFieldsStage(null, Map.of("id", 1), Map.of("movingAvg", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("2 PRECEDING").contains("2 FOLLOWING");
  }

  @Test
  void shouldRenderZeroBoundAsCurrentRow() {
    var windowSpec = new WindowSpec("documents", List.of("0", "0"));
    var windowField = new WindowField("$sum", "$value", windowSpec);
    var stage = new SetWindowFieldsStage(null, Map.of("id", 1), Map.of("currentVal", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("CURRENT ROW AND CURRENT ROW");
  }

  @Test
  void shouldRenderMultipleOutputFields() {
    var field1 = new WindowField("$sum", "$qty", null);
    var field2 = new WindowField("$avg", "$price", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("total", field1, "avgPrice", field2));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SUM(").contains("AVG(").contains(", ");
  }

  @Test
  void shouldRenderMultipleSortFields() {
    var windowField = new WindowField("$rank", null, null);
    var stage =
        new SetWindowFieldsStage(null, Map.of("date", 1, "amount", -1), Map.of("rnk", windowField));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("ORDER BY").contains("DESC");
  }

  @Test
  void shouldRenderFieldPathWithoutDollarPrefix() {
    var windowField = new WindowField("$sum", "quantity", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("total", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("$.quantity");
  }

  @Test
  void shouldRenderPartitionByWithoutDollarPrefix() {
    var windowField = new WindowField("$rank", null, null);
    var stage = new SetWindowFieldsStage("state", Map.of("id", 1), Map.of("rnk", windowField));

    stage.render(context);

    assertThat(context.toSql()).contains("$.state");
  }

  @Test
  void shouldProvideToStringWithoutPartitionBy() {
    var windowField = new WindowField("$sum", "$qty", null);
    var stage = new SetWindowFieldsStage(null, null, Map.of("total", windowField));

    assertThat(stage.toString()).contains("SetWindowFieldsStage").doesNotContain("partitionBy=");
  }

  @Test
  void shouldProvideToStringWithoutSortBy() {
    var windowField = new WindowField("$sum", "$qty", null);
    var stage = new SetWindowFieldsStage("$region", null, Map.of("total", windowField));

    assertThat(stage.toString())
        .contains("SetWindowFieldsStage")
        .contains("partitionBy=")
        .doesNotContain("sortBy=");
  }
}
