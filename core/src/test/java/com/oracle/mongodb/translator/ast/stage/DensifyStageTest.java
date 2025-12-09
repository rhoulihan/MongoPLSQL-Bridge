/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for DensifyStage which fills gaps in sequences.
 *
 * <p>MongoDB syntax:
 *
 * <pre>
 * {
 *   $densify: {
 *     field: "timestamp",
 *     partitionByFields: ["region"],
 *     range: {
 *       step: 1,
 *       unit: "hour",
 *       bounds: "full"
 *     }
 *   }
 * }
 * </pre>
 */
class DensifyStageTest {

  @Test
  void shouldRenderDensifyWithNumericRange() {
    var stage =
        new DensifyStage(
            "sequenceNumber",
            null,
            new DensifyStage.RangeSpec(1, null, "full"));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    // Should generate sequence values using recursive CTE or CONNECT BY
    assertThat(sql).contains("SELECT");
    assertThat(sql).containsIgnoringCase("sequenceNumber");
  }

  @Test
  void shouldRenderDensifyWithDateRange() {
    var stage =
        new DensifyStage(
            "timestamp",
            null,
            new DensifyStage.RangeSpec(1, "hour", "full"));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).containsIgnoringCase("timestamp");
  }

  @Test
  void shouldRenderDensifyWithPartition() {
    var stage =
        new DensifyStage(
            "timestamp",
            List.of("region", "category"),
            new DensifyStage.RangeSpec(1, "day", "partition"));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).containsIgnoringCase("region");
    assertThat(sql).containsIgnoringCase("category");
  }

  @Test
  void shouldRenderDensifyWithExplicitBounds() {
    var stage =
        new DensifyStage(
            "value",
            null,
            new DensifyStage.RangeSpec(5, null, List.of(0, 100)));
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
  }

  @Test
  void shouldRejectNullField() {
    assertThatThrownBy(
            () -> new DensifyStage(null, null, new DensifyStage.RangeSpec(1, null, "full")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("field");
  }

  @Test
  void shouldRejectNullRange() {
    assertThatThrownBy(() -> new DensifyStage("timestamp", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("range");
  }

  @Test
  void shouldRejectNonPositiveStep() {
    assertThatThrownBy(() -> new DensifyStage.RangeSpec(0, null, "full"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("step");

    assertThatThrownBy(() -> new DensifyStage.RangeSpec(-1, "hour", "full"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("step");
  }

  @Test
  void shouldReturnOperatorName() {
    var stage =
        new DensifyStage(
            "timestamp",
            null,
            new DensifyStage.RangeSpec(1, null, "full"));

    assertThat(stage.getOperatorName()).isEqualTo("$densify");
  }

  @Test
  void shouldReturnField() {
    var stage =
        new DensifyStage(
            "myField",
            null,
            new DensifyStage.RangeSpec(1, null, "full"));

    assertThat(stage.getField()).isEqualTo("myField");
  }

  @Test
  void shouldReturnPartitionByFields() {
    var stage =
        new DensifyStage(
            "timestamp",
            List.of("region", "category"),
            new DensifyStage.RangeSpec(1, null, "full"));

    assertThat(stage.getPartitionByFields()).containsExactly("region", "category");
  }

  @Test
  void shouldReturnEmptyPartitionByFieldsWhenNull() {
    var stage =
        new DensifyStage(
            "timestamp",
            null,
            new DensifyStage.RangeSpec(1, null, "full"));

    assertThat(stage.getPartitionByFields()).isEmpty();
  }

  @Test
  void shouldReturnRange() {
    var range = new DensifyStage.RangeSpec(5, "minute", "partition");
    var stage = new DensifyStage("timestamp", null, range);

    assertThat(stage.getRange()).isEqualTo(range);
  }

  @Test
  void shouldProvideToString() {
    var stage =
        new DensifyStage(
            "timestamp",
            List.of("region"),
            new DensifyStage.RangeSpec(1, "hour", "full"));

    assertThat(stage.toString()).contains("DensifyStage");
    assertThat(stage.toString()).contains("timestamp");
  }

  @Test
  void shouldReturnRangeSpecProperties() {
    var range = new DensifyStage.RangeSpec(10, "day", "partition");

    assertThat(range.step()).isEqualTo(10);
    assertThat(range.unit()).isEqualTo("day");
    assertThat(range.bounds()).isEqualTo("partition");
    assertThat(range.explicitBounds()).isNull();
  }

  @Test
  void shouldReturnRangeSpecWithExplicitBounds() {
    var range = new DensifyStage.RangeSpec(5, null, List.of(10, 50));

    assertThat(range.step()).isEqualTo(5);
    assertThat(range.unit()).isNull();
    assertThat(range.bounds()).isNull();
    assertThat(range.explicitBounds()).containsExactly(10, 50);
  }
}
