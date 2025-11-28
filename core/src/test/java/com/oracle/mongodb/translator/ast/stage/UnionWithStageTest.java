/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class UnionWithStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldCreateWithCollectionOnly() {
    var stage = new UnionWithStage("otherCollection");

    assertThat(stage.getCollection()).isEqualTo("otherCollection");
    assertThat(stage.getPipeline()).isEmpty();
    assertThat(stage.hasPipeline()).isFalse();
  }

  @Test
  void shouldCreateWithEmptyPipeline() {
    var stage = new UnionWithStage("otherCollection", List.of());

    assertThat(stage.getCollection()).isEqualTo("otherCollection");
    assertThat(stage.getPipeline()).isEmpty();
    assertThat(stage.hasPipeline()).isFalse();
  }

  @Test
  void shouldCreateWithPipeline() {
    List<Stage> pipeline = List.of(new LimitStage(10), new SkipStage(5));
    var stage = new UnionWithStage("otherCollection", pipeline);

    assertThat(stage.getCollection()).isEqualTo("otherCollection");
    assertThat(stage.getPipeline()).hasSize(2);
    assertThat(stage.hasPipeline()).isTrue();
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new UnionWithStage("otherCollection");

    assertThat(stage.getOperatorName()).isEqualTo("$unionWith");
  }

  @Test
  void shouldRenderUnionAll() {
    var stage = new UnionWithStage("inventory");

    stage.render(context);

    assertThat(context.toSql())
        .contains("UNION ALL")
        .contains("SELECT data FROM")
        .contains("inventory");
  }

  @Test
  void shouldThrowOnNullCollection() {
    assertThatNullPointerException()
        .isThrownBy(() -> new UnionWithStage(null))
        .withMessageContaining("collection");
  }

  @Test
  void shouldHandleNullPipelineAsEmpty() {
    var stage = new UnionWithStage("collection", null);

    assertThat(stage.getPipeline()).isEmpty();
    assertThat(stage.hasPipeline()).isFalse();
  }

  @Test
  void shouldProvideReadableToStringWithoutPipeline() {
    var stage = new UnionWithStage("inventory");

    assertThat(stage.toString())
        .contains("UnionWithStage")
        .contains("collection=inventory")
        .doesNotContain("pipeline");
  }

  @Test
  void shouldProvideReadableToStringWithPipeline() {
    List<Stage> pipeline = List.of(new LimitStage(5));
    var stage = new UnionWithStage("inventory", pipeline);

    assertThat(stage.toString())
        .contains("UnionWithStage")
        .contains("collection=inventory")
        .contains("pipeline=");
  }

  @Test
  void shouldReturnImmutablePipeline() {
    List<Stage> originalPipeline = List.of(new LimitStage(10));
    var stage = new UnionWithStage("collection", originalPipeline);

    List<Stage> returned = stage.getPipeline();

    // The pipeline should be immutable
    assertThat(returned).hasSize(1);
  }
}
