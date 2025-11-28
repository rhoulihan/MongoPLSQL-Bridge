/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;

class PipelineTest {

  @Test
  void shouldCreateEmptyPipeline() {
    var pipeline = Pipeline.of();

    assertThat(pipeline.getStages()).isEmpty();
  }

  @Test
  void shouldCreatePipelineWithStages() {
    var limit = new LimitStage(10);
    var skip = new SkipStage(5);

    var pipeline = Pipeline.of(skip, limit);

    assertThat(pipeline.getStages()).containsExactly(skip, limit);
  }

  @Test
  void shouldAddStage() {
    var pipeline = Pipeline.of();
    var limit = new LimitStage(10);

    var newPipeline = pipeline.addStage(limit);

    assertThat(newPipeline.getStages()).containsExactly(limit);
    assertThat(pipeline.getStages()).isEmpty(); // Original unchanged
  }

  @Test
  void shouldRejectNullStage() {
    var pipeline = Pipeline.of();

    assertThatThrownBy(() -> pipeline.addStage(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void shouldRenderSkipAndLimitClauses() {
    var skip = new SkipStage(20);
    var limit = new LimitStage(10);
    var pipeline = Pipeline.of(skip, limit);
    var context = new DefaultSqlGenerationContext();

    pipeline.render(context);

    assertThat(context.toSql()).isEqualTo("OFFSET 20 ROWS FETCH FIRST 10 ROWS ONLY");
  }

  @Test
  void shouldRenderEmptyPipeline() {
    var pipeline = Pipeline.of();
    var context = new DefaultSqlGenerationContext();

    pipeline.render(context);

    assertThat(context.toSql()).isEmpty();
  }

  @Test
  void shouldReturnStageCount() {
    var pipeline = Pipeline.of(new LimitStage(10), new SkipStage(5));

    assertThat(pipeline.size()).isEqualTo(2);
  }

  @Test
  void shouldCheckIfEmpty() {
    assertThat(Pipeline.of().isEmpty()).isTrue();
    assertThat(Pipeline.of(new LimitStage(10)).isEmpty()).isFalse();
  }

  @Test
  void shouldReturnCollectionName() {
    var pipeline = Pipeline.of("orders");

    assertThat(pipeline.getCollectionName()).isEqualTo("orders");
  }

  @Test
  void shouldDefaultCollectionNameToNull() {
    var pipeline = Pipeline.of();

    assertThat(pipeline.getCollectionName()).isNull();
  }

  @Test
  void shouldCreatePipelineWithCollectionAndStages() {
    var pipeline = Pipeline.of("customers", new LimitStage(5));

    assertThat(pipeline.getCollectionName()).isEqualTo("customers");
    assertThat(pipeline.getStages()).hasSize(1);
  }
}
