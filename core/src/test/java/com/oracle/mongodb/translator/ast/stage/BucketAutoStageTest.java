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
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BucketAutoStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldCreateWithRequiredFields() {
    var stage = new BucketAutoStage(FieldPathExpression.of("price"), 5, Map.of(), null);

    assertThat(stage.getGroupBy()).isEqualTo(FieldPathExpression.of("price"));
    assertThat(stage.getBuckets()).isEqualTo(5);
    assertThat(stage.getOutput()).isEmpty();
    assertThat(stage.hasGranularity()).isFalse();
  }

  @Test
  void shouldCreateWithOutputAccumulators() {
    var count = new AccumulatorExpression(AccumulatorOp.SUM, LiteralExpression.of(1));
    var stage =
        new BucketAutoStage(FieldPathExpression.of("price"), 10, Map.of("count", count), null);

    assertThat(stage.getOutput()).hasSize(1);
    assertThat(stage.getOutput()).containsKey("count");
  }

  @Test
  void shouldCreateWithGranularity() {
    var stage = new BucketAutoStage(FieldPathExpression.of("price"), 5, Map.of(), "R5");

    assertThat(stage.hasGranularity()).isTrue();
    assertThat(stage.getGranularity()).isEqualTo("R5");
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new BucketAutoStage(FieldPathExpression.of("price"), 5, Map.of(), null);

    assertThat(stage.getOperatorName()).isEqualTo("$bucketAuto");
  }

  @Test
  void shouldRenderNtileExpression() {
    var stage = new BucketAutoStage(FieldPathExpression.of("price"), 4, Map.of(), null);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("NTILE(4)");
    assertThat(sql).contains("OVER (ORDER BY");
    assertThat(sql).contains("AS \"_id\"");
  }

  @Test
  void shouldRenderWithAccumulators() {
    var count = new AccumulatorExpression(AccumulatorOp.COUNT, null);
    var stage =
        new BucketAutoStage(FieldPathExpression.of("price"), 3, Map.of("total", count), null);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("COUNT(*)");
    assertThat(sql).contains("AS total");
  }

  @Test
  void shouldRenderWithGranularityComment() {
    var stage = new BucketAutoStage(FieldPathExpression.of("price"), 5, Map.of(), "E6");

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("/* granularity 'E6' not supported */");
  }

  @Test
  void shouldThrowOnNullGroupBy() {
    assertThatNullPointerException()
        .isThrownBy(() -> new BucketAutoStage(null, 5, Map.of(), null))
        .withMessageContaining("groupBy");
  }

  @Test
  void shouldThrowOnZeroBuckets() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new BucketAutoStage(FieldPathExpression.of("x"), 0, Map.of(), null))
        .withMessageContaining("positive");
  }

  @Test
  void shouldThrowOnNegativeBuckets() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new BucketAutoStage(FieldPathExpression.of("x"), -1, Map.of(), null))
        .withMessageContaining("positive");
  }

  @Test
  void shouldProvideReadableToString() {
    var stage = new BucketAutoStage(FieldPathExpression.of("price"), 5, Map.of(), "R10");

    assertThat(stage.toString())
        .contains("BucketAutoStage")
        .contains("price")
        .contains("5")
        .contains("R10");
  }

  @Test
  void shouldHandleNullOutput() {
    var stage = new BucketAutoStage(FieldPathExpression.of("price"), 5, null, null);

    assertThat(stage.getOutput()).isEmpty();
  }

  @Test
  void shouldHandleEmptyGranularity() {
    var stage = new BucketAutoStage(FieldPathExpression.of("price"), 5, Map.of(), "");

    assertThat(stage.hasGranularity()).isFalse();
  }
}
