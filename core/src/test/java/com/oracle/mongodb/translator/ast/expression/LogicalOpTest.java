/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class LogicalOpTest {

  @ParameterizedTest
  @CsvSource({"$and, AND", "$or, OR", "$not, NOT", "$nor, NOR"})
  void shouldParseMongoOperator(String mongoOp, LogicalOp expected) {
    assertThat(LogicalOp.fromMongo(mongoOp)).isEqualTo(expected);
  }

  @Test
  void shouldThrowOnUnknownOperator() {
    assertThatThrownBy(() -> LogicalOp.fromMongo("$unknown"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("$unknown");
  }

  @ParameterizedTest
  @CsvSource({"AND, AND", "OR, OR", "NOT, NOT", "NOR, NOR"})
  void shouldReturnSqlOperator(LogicalOp op, String expectedSql) {
    assertThat(op.getSqlOperator()).isEqualTo(expectedSql);
  }

  @ParameterizedTest
  @CsvSource({"AND, $and", "OR, $or", "NOT, $not", "NOR, $nor"})
  void shouldReturnMongoOperator(LogicalOp op, String expectedMongo) {
    assertThat(op.getMongoOperator()).isEqualTo(expectedMongo);
  }
}
