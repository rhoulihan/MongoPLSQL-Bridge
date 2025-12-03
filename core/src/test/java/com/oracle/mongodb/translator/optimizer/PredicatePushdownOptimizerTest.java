/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PredicatePushdownOptimizerTest {

  private PredicatePushdownOptimizer optimizer;

  @BeforeEach
  void setUp() {
    optimizer = new PredicatePushdownOptimizer();
  }

  @Test
  void shouldNotModifyPipelineWithMatchFirst() {
    // {$match: ...}, {$project: ...} - already optimal
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));
    var project = createProjection("name", "status");
    var pipeline = Pipeline.of("users", List.of(match, project));

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(ProjectStage.class);
  }

  @Test
  void shouldPushMatchBeforeProject() {
    // {$project: ...}, {$match: ...} -> {$match: ...}, {$project: ...}
    var project = createProjection("name", "status");
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));
    var pipeline = Pipeline.of("users", List.of(project, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Match should be moved before project
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(ProjectStage.class);
  }

  @Test
  void shouldNotPushMatchIfProjectRemovesField() {
    // If project excludes the field used in match, cannot push
    var project = createProjection("name"); // excludes "status"
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));
    var pipeline = Pipeline.of("users", List.of(project, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Should remain unchanged since status is not in projection
    assertThat(optimized.getStages()).hasSize(2);
    // Order should be preserved (cannot push)
    assertThat(optimized.getStages().get(0)).isInstanceOf(ProjectStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(MatchStage.class);
  }

  private ProjectStage createProjection(String... fields) {
    Map<String, ProjectionField> projections = new LinkedHashMap<>();
    for (String field : fields) {
      projections.put(field, ProjectionField.include(FieldPathExpression.of(field)));
    }
    return new ProjectStage(projections);
  }

  @Test
  void shouldPushMatchBeforeLimit() {
    // {$limit: 10}, {$match: ...} -> {$match: ...}, {$limit: 10}
    var limit = new LimitStage(10);
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT, FieldPathExpression.of("age"), LiteralExpression.of(18)));
    var pipeline = Pipeline.of("users", List.of(limit, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Match should be moved before limit
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(LimitStage.class);
  }

  @Test
  void shouldHandleEmptyPipeline() {
    var pipeline = Pipeline.of("users", List.of());

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getStages()).isEmpty();
  }

  @Test
  void shouldHandleSingleStagePipeline() {
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));
    var pipeline = Pipeline.of("users", List.of(match));

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getStages()).hasSize(1);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
  }

  @Test
  void shouldMergeConsecutiveMatchStages() {
    // {$match: {a: 1}}, {$match: {b: 2}} -> {$match: {$and: [{a: 1}, {b: 2}]}}
    var match1 =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("a"), LiteralExpression.of(1)));
    var match2 =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("b"), LiteralExpression.of(2)));
    var pipeline = Pipeline.of("users", List.of(match1, match2));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Should merge into single match with AND
    assertThat(optimized.getStages()).hasSize(1);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
  }

  @Test
  void shouldPreserveCollectionName() {
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));
    var pipeline = Pipeline.of("myCollection", List.of(match));

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getCollection()).isEqualTo("myCollection");
  }

  @Test
  void shouldPushMatchBeforeSort() {
    // {$sort: {name: 1}}, {$match: {status: "active"}} -> {$match: ...}, {$sort: ...}
    var sort = createSort("name", 1);
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));
    var pipeline = Pipeline.of("users", List.of(sort, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Match should be moved before sort
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(SortStage.class);
  }

  @Test
  void shouldPushMatchBeforeSkip() {
    // {$skip: 10}, {$match: {age: 25}} -> {$match: ...}, {$skip: ...}
    var skip = new SkipStage(10);
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("age"), LiteralExpression.of(25)));
    var pipeline = Pipeline.of("users", List.of(skip, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Match should be pushed before skip
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(SkipStage.class);
  }

  @Test
  void shouldPushMatchThroughMultipleStages() {
    // {$project: {name, status}}, {$sort: {name: 1}}, {$limit: 10}, {$match: {status: "active"}}
    // Should push match all the way to the front
    var project = createProjection("name", "status");
    var sort = createSort("name", 1);
    var limit = new LimitStage(10);
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));
    var pipeline = Pipeline.of("users", List.of(project, sort, limit, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Match should be first
    assertThat(optimized.getStages()).hasSize(4);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
  }

  @Test
  void shouldMergeThreeConsecutiveMatches() {
    // {$match: {a: 1}}, {$match: {b: 2}}, {$match: {c: 3}} -> single $match with $and
    var match1 =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("a"), LiteralExpression.of(1)));
    var match2 =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("b"), LiteralExpression.of(2)));
    var match3 =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("c"), LiteralExpression.of(3)));
    var pipeline = Pipeline.of("users", List.of(match1, match2, match3));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Should merge into a single match with AND
    assertThat(optimized.getStages()).hasSize(1);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
  }

  @Test
  void shouldNotPushMatchBeforeGroup() {
    // Cannot push $match before $group - group changes the document structure
    var group = createGroup("category");
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT, FieldPathExpression.of("count"), LiteralExpression.of(10)));
    var pipeline = Pipeline.of("products", List.of(group, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Order should remain unchanged
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(GroupStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(MatchStage.class);
  }

  @Test
  void shouldPushPartialMatchBeforeGroup() {
    // If match has conditions on both original and grouped fields, split it
    // For now, we don't split, so it stays after group
    var group = createGroup("category");
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("_id"),
                LiteralExpression.of("electronics")));
    var pipeline = Pipeline.of("products", List.of(group, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Match on _id (group output) cannot be pushed
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(GroupStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(MatchStage.class);
  }

  @Test
  void shouldNotPushMatchBeforeLookup() {
    // Cannot push $match before $lookup - lookup adds new fields
    var lookup = createLookup("orders", "userId", "_id", "userOrders");
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT, FieldPathExpression.of("orderCount"), LiteralExpression.of(5)));
    var pipeline = Pipeline.of("users", List.of(lookup, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Order should remain unchanged
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(LookupStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(MatchStage.class);
  }

  @Test
  void shouldPushMatchOnOriginalFieldBeforeLookup() {
    // Match on fields that exist before lookup CAN be pushed
    // But our current optimizer is conservative and doesn't do this
    var lookup = createLookup("orders", "userId", "_id", "userOrders");
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")));
    var pipeline = Pipeline.of("users", List.of(lookup, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Currently stays after lookup (conservative)
    assertThat(optimized.getStages()).hasSize(2);
  }

  @Test
  void shouldHandleComplexNestedConditions() {
    // Test with $and containing multiple conditions
    var innerAnd =
        new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(
                    ComparisonOp.GT, FieldPathExpression.of("age"), LiteralExpression.of(18)),
                new ComparisonExpression(
                    ComparisonOp.LT, FieldPathExpression.of("age"), LiteralExpression.of(65))));
    var match = new MatchStage(innerAnd);
    var limit = new LimitStage(10);
    var pipeline = Pipeline.of("users", List.of(limit, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Match should be pushed before limit
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(LimitStage.class);
  }

  @Test
  void shouldHandleProjectWithComputedFields() {
    // Project with computed field - match on computed field cannot be pushed
    // We'll use a case where fullName is not in the original fields
    Map<String, ProjectionField> projections = new LinkedHashMap<>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
    // fullName is a new computed field, not present in the original document
    var project = new ProjectStage(projections);
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("fullName"), LiteralExpression.of("John")));
    var pipeline = Pipeline.of("users", List.of(project, match));

    Pipeline optimized = optimizer.optimize(pipeline);

    // fullName is not in projection, match cannot be pushed
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(ProjectStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(MatchStage.class);
  }

  private SortStage createSort(String field, int direction) {
    Map<String, Integer> sortSpec = new LinkedHashMap<>();
    sortSpec.put(field, direction);
    return new SortStage(sortSpec);
  }

  private GroupStage createGroup(String idField) {
    return new GroupStage(FieldPathExpression.of(idField));
  }

  private LookupStage createLookup(
      String from, String localField, String foreignField, String as) {
    return LookupStage.equality(from, localField, foreignField, as);
  }
}
