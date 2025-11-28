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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SampleStageTest {

  @Test
  void shouldRenderSampleClause() {
    var stage = new SampleStage(10);
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    assertThat(context.toSql()).contains("ORDER BY DBMS_RANDOM.VALUE");
    assertThat(context.toSql()).contains("FETCH FIRST 10 ROWS ONLY");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, -100})
  void shouldRejectInvalidSize(int size) {
    assertThatThrownBy(() -> new SampleStage(size)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldReturnSize() {
    var stage = new SampleStage(25);

    assertThat(stage.getSize()).isEqualTo(25);
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new SampleStage(5);

    assertThat(stage.getOperatorName()).isEqualTo("$sample");
  }

  @Test
  void shouldProvideToString() {
    var stage = new SampleStage(100);

    assertThat(stage.toString()).isEqualTo("SampleStage(100)");
  }
}
