/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for FillStage which fills null and missing values.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * {
 *   $fill: {
 *     partitionBy: "$state",
 *     sortBy: { "date": 1 },
 *     output: {
 *       "quantity": { method: "linear" },
 *       "price": { value: 0 }
 *     }
 *   }
 * }
 * </pre>
 */
class FillStageTest {

  @Test
  void shouldRenderFillWithLocfMethod() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec("locf", null));

    var stage = new FillStage(null, null, output);
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    // LOCF uses LAG window function
    assertThat(sql).containsIgnoringCase("LAG");
    assertThat(sql).containsIgnoringCase("quantity");
  }

  @Test
  void shouldRenderFillWithLinearMethod() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("price", new FillStage.FillSpec("linear", null));

    Map<String, Integer> sortBy = new LinkedHashMap<>();
    sortBy.put("date", 1);

    var stage = new FillStage(null, sortBy, output);
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    // Linear interpolation requires more complex SQL
    assertThat(sql).contains("SELECT");
  }

  @Test
  void shouldRenderFillWithConstantValue() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec(null, 0));

    var stage = new FillStage(null, null, output);
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    // Constant value uses COALESCE/NVL
    assertThat(sql).containsIgnoringCase("COALESCE");
    assertThat(sql).containsIgnoringCase("quantity");
  }

  @Test
  void shouldRenderFillWithPartition() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec("locf", null));

    var stage = new FillStage("region", null, output);
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).containsIgnoringCase("PARTITION BY");
    assertThat(sql).containsIgnoringCase("region");
  }

  @Test
  void shouldRenderFillWithSortBy() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec("locf", null));

    Map<String, Integer> sortBy = new LinkedHashMap<>();
    sortBy.put("date", 1);

    var stage = new FillStage(null, sortBy, output);
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).containsIgnoringCase("ORDER BY");
    assertThat(sql).containsIgnoringCase("date");
  }

  @Test
  void shouldRejectNullOutput() {
    assertThatThrownBy(() -> new FillStage(null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("output");
  }

  @Test
  void shouldRejectEmptyOutput() {
    assertThatThrownBy(() -> new FillStage(null, null, Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("output");
  }

  @Test
  void shouldReturnOperatorName() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec("locf", null));

    var stage = new FillStage(null, null, output);

    assertThat(stage.getOperatorName()).isEqualTo("$fill");
  }

  @Test
  void shouldReturnPartitionBy() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec("locf", null));

    var stage = new FillStage("region", null, output);

    assertThat(stage.getPartitionBy()).isEqualTo("region");
  }

  @Test
  void shouldReturnSortBy() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec("locf", null));

    Map<String, Integer> sortBy = new LinkedHashMap<>();
    sortBy.put("date", 1);
    sortBy.put("time", -1);

    var stage = new FillStage(null, sortBy, output);

    assertThat(stage.getSortBy()).containsExactly(
        Map.entry("date", 1),
        Map.entry("time", -1));
  }

  @Test
  void shouldReturnOutput() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec("locf", null));
    output.put("price", new FillStage.FillSpec(null, 0));

    var stage = new FillStage(null, null, output);

    assertThat(stage.getOutput()).hasSize(2);
    assertThat(stage.getOutput().get("quantity").method()).isEqualTo("locf");
    assertThat(stage.getOutput().get("price").value()).isEqualTo(0);
  }

  @Test
  void shouldProvideToString() {
    Map<String, FillStage.FillSpec> output = new LinkedHashMap<>();
    output.put("quantity", new FillStage.FillSpec("locf", null));

    var stage = new FillStage("region", null, output);

    assertThat(stage.toString()).contains("FillStage");
    assertThat(stage.toString()).contains("region");
  }

  @Test
  void shouldReturnFillSpecProperties() {
    var specWithMethod = new FillStage.FillSpec("locf", null);
    var specWithValue = new FillStage.FillSpec(null, 42);

    assertThat(specWithMethod.method()).isEqualTo("locf");
    assertThat(specWithMethod.value()).isNull();
    assertThat(specWithValue.method()).isNull();
    assertThat(specWithValue.value()).isEqualTo(42);
  }
}
