/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Tests for ProceduralSqlConverter including complexity detection.
 */
class ProceduralSqlConverterTest {

  @Test
  void shouldNotRecommendProceduralForSimpleQuery() {
    // A simple query with only 2 CTEs should not need procedural mode
    String simpleSql = """
        WITH "Q1" ("DATA") AS (SELECT "DATA" FROM "ORDERS"),
        "Q2" ("DATA") AS (SELECT "DATA" FROM "Q1" WHERE "DATA".status = 'active')
        SELECT * FROM "Q2"
        """;

    assertThat(ProceduralSqlConverter.shouldUseProcedural(simpleSql)).isFalse();
  }

  @Test
  void shouldRecommendProceduralForComplexQuery() {
    // A complex query with 20+ CTEs should recommend procedural mode
    StringBuilder complexSql = new StringBuilder("WITH ");
    for (int i = 1; i <= 25; i++) {
      if (i > 1) {
        complexSql.append(", ");
      }
      complexSql.append("\"Q").append(i).append("\" (\"DATA\") AS (SELECT \"DATA\" FROM ");
      if (i == 1) {
        complexSql.append("\"ORDERS\"");
      } else {
        complexSql.append("\"Q").append(i - 1).append("\"");
      }
      complexSql.append(")");
    }
    complexSql.append(" SELECT * FROM \"Q25\"");

    assertThat(ProceduralSqlConverter.shouldUseProcedural(complexSql.toString())).isTrue();
  }

  @Test
  void shouldRecommendProceduralAt15Ctes() {
    // The threshold should be around 15 CTEs
    StringBuilder sql = new StringBuilder("WITH ");
    for (int i = 1; i <= 15; i++) {
      if (i > 1) {
        sql.append(", ");
      }
      sql.append("\"Q").append(i).append("\" (\"DATA\") AS (SELECT \"DATA\" FROM ");
      if (i == 1) {
        sql.append("\"ORDERS\"");
      } else {
        sql.append("\"Q").append(i - 1).append("\"");
      }
      sql.append(")");
    }
    sql.append(" SELECT * FROM \"Q15\"");

    assertThat(ProceduralSqlConverter.shouldUseProcedural(sql.toString())).isTrue();
  }

  @Test
  void shouldNotRecommendProceduralAt14Ctes() {
    // Just under the threshold
    StringBuilder sql = new StringBuilder("WITH ");
    for (int i = 1; i <= 14; i++) {
      if (i > 1) {
        sql.append(", ");
      }
      sql.append("\"Q").append(i).append("\" (\"DATA\") AS (SELECT \"DATA\" FROM ");
      if (i == 1) {
        sql.append("\"ORDERS\"");
      } else {
        sql.append("\"Q").append(i - 1).append("\"");
      }
      sql.append(")");
    }
    sql.append(" SELECT * FROM \"Q14\"");

    assertThat(ProceduralSqlConverter.shouldUseProcedural(sql.toString())).isFalse();
  }

  @Test
  void shouldNotRecommendProceduralForNonCteSql() {
    String simpleSql = "SELECT * FROM \"ORDERS\" WHERE status = 'active'";

    assertThat(ProceduralSqlConverter.shouldUseProcedural(simpleSql)).isFalse();
  }

  @Test
  void shouldHandleNullSql() {
    assertThat(ProceduralSqlConverter.shouldUseProcedural(null)).isFalse();
  }

  @Test
  void shouldHandleEmptySql() {
    assertThat(ProceduralSqlConverter.shouldUseProcedural("")).isFalse();
    assertThat(ProceduralSqlConverter.shouldUseProcedural("   ")).isFalse();
  }

  @Test
  void shouldCountCtes() {
    // Test that CTE counting works correctly
    String sql = """
        WITH "Q1" ("DATA") AS (SELECT 1 FROM DUAL),
        "Q2" ("DATA") AS (SELECT 2 FROM DUAL),
        "Q3" ("DATA") AS (SELECT 3 FROM DUAL)
        SELECT * FROM "Q3"
        """;

    assertThat(ProceduralSqlConverter.countCtes(sql)).isEqualTo(3);
  }

  @Test
  void shouldCountZeroCtesForNonCteQuery() {
    String sql = "SELECT * FROM ORDERS";

    assertThat(ProceduralSqlConverter.countCtes(sql)).isEqualTo(0);
  }

  @Test
  void shouldAddIsJsonConstraintForDotNotationSupport() {
    // The generated procedural SQL should include ALTER TABLE ... ADD CONSTRAINT ... IS JSON
    // to enable dot notation access on materialized tables
    String cteSql = """
        WITH "Q1" ("DATA") AS (SELECT "DATA" FROM "ORDERS"),
        "Q2" ("DATA") AS (SELECT "DATA" FROM "Q1" WHERE "DATA".status = 'active')
        SELECT * FROM "Q2"
        """;

    String result = ProceduralSqlConverter.convert(cteSql);

    // Should contain IS JSON constraint for each table with DATA column
    assertThat(result).contains("ALTER TABLE TMP_PIPE_Q1");
    assertThat(result).contains("IS JSON");
    assertThat(result).contains("ALTER TABLE TMP_PIPE_Q2");
  }

  @Test
  void shouldAddIsJsonConstraintAfterCreateTable() {
    String cteSql = """
        WITH "Q1" ("DATA") AS (SELECT JSON_OBJECT('x' VALUE 1) AS "DATA" FROM DUAL)
        SELECT * FROM "Q1"
        """;

    String result = ProceduralSqlConverter.convert(cteSql);

    // ALTER TABLE should come right after CREATE TABLE
    int createPos = result.indexOf("CREATE TABLE TMP_PIPE_Q1");
    int alterPos = result.indexOf("ALTER TABLE TMP_PIPE_Q1");

    assertThat(createPos).isGreaterThan(0);
    assertThat(alterPos).isGreaterThan(createPos);
    // The ALTER should come before the next CREATE TABLE or final SELECT
    assertThat(result).contains("CHECK (\"DATA\" IS JSON)");
  }
}
