/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.ast.stage.MergeStage.WhenMatched;
import com.oracle.mongodb.translator.ast.stage.MergeStage.WhenNotMatched;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MergeStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldCreateWithDefaultOptions() {
    var stage = new MergeStage("targetCollection");

    assertThat(stage.getTargetCollection()).isEqualTo("targetCollection");
    assertThat(stage.getOnFields()).containsExactly("_id");
    assertThat(stage.getWhenMatched()).isEqualTo(WhenMatched.MERGE);
    assertThat(stage.getWhenNotMatched()).isEqualTo(WhenNotMatched.INSERT);
  }

  @Test
  void shouldCreateWithAllOptions() {
    var stage =
        new MergeStage(
            "output",
            List.of("orderId", "customerId"),
            WhenMatched.REPLACE,
            WhenNotMatched.DISCARD);

    assertThat(stage.getTargetCollection()).isEqualTo("output");
    assertThat(stage.getOnFields()).containsExactly("orderId", "customerId");
    assertThat(stage.getWhenMatched()).isEqualTo(WhenMatched.REPLACE);
    assertThat(stage.getWhenNotMatched()).isEqualTo(WhenNotMatched.DISCARD);
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new MergeStage("target");

    assertThat(stage.getOperatorName()).isEqualTo("$merge");
  }

  @Test
  void shouldRenderMergeComment() {
    var stage =
        new MergeStage(
            "outputCollection", List.of("_id", "type"), WhenMatched.REPLACE, WhenNotMatched.INSERT);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("MERGE INTO");
    assertThat(sql).contains("outputCollection");
    assertThat(sql).contains("_id, type");
  }

  @Test
  void shouldThrowOnNullTarget() {
    assertThatNullPointerException()
        .isThrownBy(() -> new MergeStage(null))
        .withMessageContaining("targetCollection");
  }

  @Test
  void shouldProvideReadableToString() {
    var stage = new MergeStage("output", List.of("_id"), WhenMatched.MERGE, WhenNotMatched.INSERT);

    assertThat(stage.toString())
        .contains("MergeStage")
        .contains("output")
        .contains("MERGE")
        .contains("INSERT");
  }
}
