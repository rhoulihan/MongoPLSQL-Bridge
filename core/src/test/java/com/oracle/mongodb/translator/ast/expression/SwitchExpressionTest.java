/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for SwitchExpression. */
class SwitchExpressionTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void testSingleBranch() {
    // {$switch: {branches: [{case: {$eq: ["$status", "A"]}, then: "Active"}], default: "Other"}}
    Expression caseExpr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("A"));
    Expression thenExpr = LiteralExpression.of("Active");
    Expression defaultExpr = LiteralExpression.of("Other");

    SwitchExpression switchExpr =
        SwitchExpression.of(
            List.of(new SwitchExpression.SwitchBranch(caseExpr, thenExpr)), defaultExpr);

    switchExpr.render(context);

    assertThat(context.toSql())
        .contains("CASE")
        .contains("WHEN")
        .contains("THEN")
        .contains("ELSE")
        .contains("END");
  }

  @Test
  void testMultipleBranches() {
    // {$switch: {
    //   branches: [
    //     {case: {$eq: ["$status", "A"]}, then: "Active"},
    //     {case: {$eq: ["$status", "B"]}, then: "Blocked"},
    //     {case: {$eq: ["$status", "C"]}, then: "Cancelled"}
    //   ],
    //   default: "Unknown"
    // }}
    Expression case1 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("A"));
    Expression case2 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("B"));
    Expression case3 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("C"));

    SwitchExpression switchExpr =
        SwitchExpression.of(
            List.of(
                new SwitchExpression.SwitchBranch(case1, LiteralExpression.of("Active")),
                new SwitchExpression.SwitchBranch(case2, LiteralExpression.of("Blocked")),
                new SwitchExpression.SwitchBranch(case3, LiteralExpression.of("Cancelled"))),
            LiteralExpression.of("Unknown"));

    switchExpr.render(context);

    String sql = context.toSql();
    // Should have 3 WHEN clauses
    assertEquals(3, countOccurrences(sql, "WHEN"));
    assertEquals(3, countOccurrences(sql, "THEN"));
  }

  @Test
  void testNoDefault() {
    // When all cases are covered, default can be omitted
    Expression caseExpr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("A"));
    Expression thenExpr = LiteralExpression.of("Active");

    SwitchExpression switchExpr =
        SwitchExpression.of(
            List.of(new SwitchExpression.SwitchBranch(caseExpr, thenExpr)), null);

    switchExpr.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("CASE").contains("WHEN").contains("THEN").contains("END");
    assertThat(sql).doesNotContain("ELSE");
  }

  @Test
  void testBranchWithFieldExpressions() {
    // {$switch: {branches: [{case: {$gt: ["$score", 90]}, then: "$bonus"}], default: 0}}
    Expression caseExpr =
        new ComparisonExpression(
            ComparisonOp.GT, FieldPathExpression.of("score"), LiteralExpression.of(90));
    Expression thenExpr = FieldPathExpression.of("bonus");
    Expression defaultExpr = LiteralExpression.of(0);

    SwitchExpression switchExpr =
        SwitchExpression.of(
            List.of(new SwitchExpression.SwitchBranch(caseExpr, thenExpr)), defaultExpr);

    switchExpr.render(context);

    assertThat(context.toSql()).contains("data.score").contains("data.bonus");
  }

  @Test
  void testRequiresAtLeastOneBranch() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SwitchExpression.of(List.of(), LiteralExpression.of("default")));
  }

  @Test
  void testRequiresNonNullBranches() {
    assertThrows(IllegalArgumentException.class, () -> SwitchExpression.of(null, null));
  }

  @Test
  void testBranchRequiresNonNullCaseExpression() {
    assertThrows(
        NullPointerException.class,
        () -> new SwitchExpression.SwitchBranch(null, LiteralExpression.of("then")));
  }

  @Test
  void testBranchRequiresNonNullThenExpression() {
    Expression caseExpr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("A"));
    assertThrows(
        NullPointerException.class, () -> new SwitchExpression.SwitchBranch(caseExpr, null));
  }

  @Test
  void testGetBranches() {
    Expression caseExpr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("A"));
    Expression thenExpr = LiteralExpression.of("Active");

    SwitchExpression switchExpr =
        SwitchExpression.of(
            List.of(new SwitchExpression.SwitchBranch(caseExpr, thenExpr)),
            LiteralExpression.of("Other"));

    List<SwitchExpression.SwitchBranch> branches = switchExpr.getBranches();
    assertEquals(1, branches.size());
    assertEquals(caseExpr, branches.get(0).caseExpr());
    assertEquals(thenExpr, branches.get(0).thenExpr());
  }

  @Test
  void testGetDefaultExpr() {
    Expression caseExpr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("A"));
    Expression defaultExpr = LiteralExpression.of("Other");

    SwitchExpression switchExpr =
        SwitchExpression.of(
            List.of(
                new SwitchExpression.SwitchBranch(caseExpr, LiteralExpression.of("Active"))),
            defaultExpr);

    assertEquals(defaultExpr, switchExpr.getDefaultExpr());
  }

  @Test
  void testToString() {
    Expression caseExpr =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("A"));
    Expression thenExpr = LiteralExpression.of("Active");

    SwitchExpression switchExpr =
        SwitchExpression.of(
            List.of(new SwitchExpression.SwitchBranch(caseExpr, thenExpr)),
            LiteralExpression.of("Other"));

    String str = switchExpr.toString();
    assertNotNull(str);
    assertTrue(str.contains("Switch"));
    assertTrue(str.contains("case"));
    assertTrue(str.contains("then"));
    assertTrue(str.contains("default"));
  }

  private int countOccurrences(String str, String sub) {
    int count = 0;
    int idx = 0;
    while ((idx = str.indexOf(sub, idx)) != -1) {
      count++;
      idx += sub.length();
    }
    return count;
  }
}
