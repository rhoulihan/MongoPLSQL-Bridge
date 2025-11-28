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

class LimitStageTest {

  @Test
  void shouldRenderLimitClause() {
    var stage = new LimitStage(10);
    var context = new DefaultSqlGenerationContext();

    stage.render(context);

    assertThat(context.toSql()).isEqualTo("FETCH FIRST 10 ROWS ONLY");
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, -100})
  void shouldRejectInvalidLimit(int limit) {
    assertThatThrownBy(() -> new LimitStage(limit)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void shouldReturnLimit() {
    var stage = new LimitStage(25);

    assertThat(stage.getLimit()).isEqualTo(25);
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new LimitStage(5);

    assertThat(stage.getOperatorName()).isEqualTo("$limit");
  }
}
