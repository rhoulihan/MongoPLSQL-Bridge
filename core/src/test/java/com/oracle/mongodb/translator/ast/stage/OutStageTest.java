/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OutStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldCreateWithCollectionOnly() {
    var stage = new OutStage("outputCollection");

    assertThat(stage.getTargetCollection()).isEqualTo("outputCollection");
    assertThat(stage.hasTargetDatabase()).isFalse();
    assertThat(stage.getTargetDatabase()).isNull();
  }

  @Test
  void shouldCreateWithDatabaseAndCollection() {
    var stage = new OutStage("outputCollection", "myDatabase");

    assertThat(stage.getTargetCollection()).isEqualTo("outputCollection");
    assertThat(stage.hasTargetDatabase()).isTrue();
    assertThat(stage.getTargetDatabase()).isEqualTo("myDatabase");
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new OutStage("target");

    assertThat(stage.getOperatorName()).isEqualTo("$out");
  }

  @Test
  void shouldRenderInsertComment() {
    var stage = new OutStage("outputCollection");

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("INSERT INTO");
    assertThat(sql).contains("outputCollection");
  }

  @Test
  void shouldRenderWithDatabase() {
    var stage = new OutStage("myCollection", "myDb");

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("myDb.myCollection");
  }

  @Test
  void shouldThrowOnNullCollection() {
    assertThatNullPointerException()
        .isThrownBy(() -> new OutStage(null))
        .withMessageContaining("targetCollection");
  }

  @Test
  void shouldProvideReadableToStringWithoutDatabase() {
    var stage = new OutStage("output");

    assertThat(stage.toString()).contains("OutStage").contains("output").doesNotContain("database");
  }

  @Test
  void shouldProvideReadableToStringWithDatabase() {
    var stage = new OutStage("output", "myDb");

    assertThat(stage.toString())
        .contains("OutStage")
        .contains("database=myDb")
        .contains("collection=output");
  }
}
