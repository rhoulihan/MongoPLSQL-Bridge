/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.stage.UnsetStage;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for UnsetStageParser. */
class UnsetStageParserTest {

  private UnsetStageParser parser;
  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    parser = new UnsetStageParser();
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldParseSingleFieldString() {
    UnsetStage stage = parser.parse("fieldToRemove");

    assertThat(stage.getFields()).containsExactly("fieldToRemove");
    stage.render(context);
    assertThat(context.toSql()).contains("fieldToRemove");
  }

  @Test
  void shouldParseMultipleFieldsArray() {
    UnsetStage stage = parser.parse(List.of("field1", "field2", "field3"));

    assertThat(stage.getFields()).containsExactly("field1", "field2", "field3");
    stage.render(context);
    assertThat(context.toSql()).contains("field1");
    assertThat(context.toSql()).contains("field2");
    assertThat(context.toSql()).contains("field3");
  }

  @Test
  void shouldParseNestedField() {
    UnsetStage stage = parser.parse("parent.child");

    assertThat(stage.getFields()).containsExactly("parent.child");
    stage.render(context);
    assertThat(context.toSql()).contains("parent.child");
  }

  @Test
  void shouldThrowOnEmptyArray() {
    assertThatThrownBy(() -> parser.parse(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at least one field");
  }

  @Test
  void shouldThrowOnNullValue() {
    assertThatThrownBy(() -> parser.parse(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("string or array");
  }

  @Test
  void shouldThrowOnNonStringInArray() {
    assertThatThrownBy(() -> parser.parse(List.of("field1", 123)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("strings");
  }

  @Test
  void shouldThrowOnInvalidType() {
    assertThatThrownBy(() -> parser.parse(123))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("string or array");
  }
}
