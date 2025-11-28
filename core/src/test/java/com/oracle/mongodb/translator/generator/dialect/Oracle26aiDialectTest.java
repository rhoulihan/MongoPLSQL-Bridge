/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator.dialect;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class Oracle26aiDialectTest {

  @Test
  void shouldHaveSingletonInstance() {
    OracleDialect dialect1 = Oracle26aiDialect.INSTANCE;
    OracleDialect dialect2 = Oracle26aiDialect.INSTANCE;

    assertThat(dialect1).isSameAs(dialect2);
  }

  @Test
  void shouldReturnCorrectName() {
    OracleDialect dialect = Oracle26aiDialect.INSTANCE;

    assertThat(dialect.name()).isEqualTo("Oracle 26ai");
  }

  @Test
  void shouldSupportJsonValueReturning() {
    OracleDialect dialect = Oracle26aiDialect.INSTANCE;

    assertThat(dialect.supportsJsonValueReturning()).isTrue();
  }

  @Test
  void shouldSupportNestedPath() {
    OracleDialect dialect = Oracle26aiDialect.INSTANCE;

    assertThat(dialect.supportsNestedPath()).isTrue();
  }

  @Test
  void shouldSupportJsonCollectionTables() {
    OracleDialect dialect = Oracle26aiDialect.INSTANCE;

    assertThat(dialect.supportsJsonCollectionTables()).isTrue();
  }

  @Test
  void shouldBeInstanceOfOracleDialect() {
    assertThat(Oracle26aiDialect.INSTANCE).isInstanceOf(OracleDialect.class);
  }
}
