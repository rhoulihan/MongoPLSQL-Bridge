/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

class TypeConversionOpTest {

  @Test
  void shouldReturnCorrectMongoOperator() {
    assertThat(TypeConversionOp.TO_INT.getMongoOperator()).isEqualTo("$toInt");
    assertThat(TypeConversionOp.TO_LONG.getMongoOperator()).isEqualTo("$toLong");
    assertThat(TypeConversionOp.TO_DOUBLE.getMongoOperator()).isEqualTo("$toDouble");
    assertThat(TypeConversionOp.TO_DECIMAL.getMongoOperator()).isEqualTo("$toDecimal");
    assertThat(TypeConversionOp.TO_STRING.getMongoOperator()).isEqualTo("$toString");
    assertThat(TypeConversionOp.TO_BOOL.getMongoOperator()).isEqualTo("$toBool");
    assertThat(TypeConversionOp.TO_DATE.getMongoOperator()).isEqualTo("$toDate");
    assertThat(TypeConversionOp.TO_OBJECT_ID.getMongoOperator()).isEqualTo("$toObjectId");
    assertThat(TypeConversionOp.TYPE.getMongoOperator()).isEqualTo("$type");
    assertThat(TypeConversionOp.CONVERT.getMongoOperator()).isEqualTo("$convert");
    assertThat(TypeConversionOp.IS_NUMBER.getMongoOperator()).isEqualTo("$isNumber");
    assertThat(TypeConversionOp.IS_STRING.getMongoOperator()).isEqualTo("$isString");
  }

  @Test
  void shouldParseFromMongoOperator() {
    assertThat(TypeConversionOp.fromMongoOperator("$toInt")).isEqualTo(TypeConversionOp.TO_INT);
    assertThat(TypeConversionOp.fromMongoOperator("$toLong")).isEqualTo(TypeConversionOp.TO_LONG);
    assertThat(TypeConversionOp.fromMongoOperator("$toDouble"))
        .isEqualTo(TypeConversionOp.TO_DOUBLE);
    assertThat(TypeConversionOp.fromMongoOperator("$toDecimal"))
        .isEqualTo(TypeConversionOp.TO_DECIMAL);
    assertThat(TypeConversionOp.fromMongoOperator("$toString"))
        .isEqualTo(TypeConversionOp.TO_STRING);
    assertThat(TypeConversionOp.fromMongoOperator("$toBool")).isEqualTo(TypeConversionOp.TO_BOOL);
    assertThat(TypeConversionOp.fromMongoOperator("$toDate")).isEqualTo(TypeConversionOp.TO_DATE);
    assertThat(TypeConversionOp.fromMongoOperator("$toObjectId"))
        .isEqualTo(TypeConversionOp.TO_OBJECT_ID);
    assertThat(TypeConversionOp.fromMongoOperator("$type")).isEqualTo(TypeConversionOp.TYPE);
    assertThat(TypeConversionOp.fromMongoOperator("$convert")).isEqualTo(TypeConversionOp.CONVERT);
    assertThat(TypeConversionOp.fromMongoOperator("$isNumber"))
        .isEqualTo(TypeConversionOp.IS_NUMBER);
    assertThat(TypeConversionOp.fromMongoOperator("$isString"))
        .isEqualTo(TypeConversionOp.IS_STRING);
  }

  @Test
  void shouldThrowOnUnknownOperator() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> TypeConversionOp.fromMongoOperator("$unknown"))
        .withMessageContaining("Unknown type conversion operator");
  }

  @Test
  void shouldHaveAllEnumValues() {
    assertThat(TypeConversionOp.values()).hasSize(12);
  }
}
