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
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SortLimitOptimizerTest {

  private SortLimitOptimizer optimizer;

  @BeforeEach
  void setUp() {
    optimizer = new SortLimitOptimizer();
  }

  @Test
  void shouldCombineSortAndLimit() {
    // {$sort: {price: 1}}, {$limit: 10} -> combined for Oracle Top-N optimization
    var sort = createSort("price", 1);
    var limit = new LimitStage(10);
    var pipeline = Pipeline.of("products", List.of(sort, limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Should remain as two stages but with sort marked for limit optimization
    assertThat(optimized.getStages()).hasSize(2);
    assertThat(optimized.getStages().get(0)).isInstanceOf(SortStage.class);
    assertThat(optimized.getStages().get(1)).isInstanceOf(LimitStage.class);

    // The sort stage should be marked with the limit for FETCH FIRST N ROWS ONLY
    SortStage sortStage = (SortStage) optimized.getStages().get(0);
    assertThat(sortStage.getLimitHint()).isEqualTo(10);
  }

  @Test
  void shouldNotModifyNonConsecutiveSortLimit() {
    // {$sort: ...}, {$match: ...}, {$limit: 10}
    // Cannot combine because there's a stage in between
    var sort = createSort("price", 1);
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT, FieldPathExpression.of("price"), LiteralExpression.of(0)));
    var limit = new LimitStage(10);
    var pipeline = Pipeline.of("products", List.of(sort, match, limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getStages()).hasSize(3);
    SortStage sortStage = (SortStage) optimized.getStages().get(0);
    assertThat(sortStage.getLimitHint()).isNull();
  }

  @Test
  void shouldHandleSortWithoutLimit() {
    var sort = createSort("name", 1);
    var pipeline = Pipeline.of("users", List.of(sort));

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getStages()).hasSize(1);
    SortStage sortStage = (SortStage) optimized.getStages().get(0);
    assertThat(sortStage.getLimitHint()).isNull();
  }

  @Test
  void shouldHandleLimitWithoutSort() {
    var limit = new LimitStage(10);
    var pipeline = Pipeline.of("users", List.of(limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getStages()).hasSize(1);
    assertThat(optimized.getStages().get(0)).isInstanceOf(LimitStage.class);
  }

  @Test
  void shouldHandleEmptyPipeline() {
    var pipeline = Pipeline.of("users", List.of());

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getStages()).isEmpty();
  }

  @Test
  void shouldPreserveCollectionName() {
    var sort = createSort("name", 1);
    var limit = new LimitStage(5);
    var pipeline = Pipeline.of("myCollection", List.of(sort, limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    assertThat(optimized.getCollection()).isEqualTo("myCollection");
  }

  @Test
  void shouldCombineSortWithSkipAndLimit() {
    // {$sort: {price: 1}}, {$skip: 5}, {$limit: 10} -> combined for OFFSET/FETCH
    var sort = createSort("price", 1);
    var skip = new SkipStage(5);
    var limit = new LimitStage(10);
    var pipeline = Pipeline.of("products", List.of(sort, skip, limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Sort should have the limit hint (for top-N optimization)
    assertThat(optimized.getStages()).hasSize(3);
    SortStage sortStage = (SortStage) optimized.getStages().get(0);
    // The limit hint should account for skip + limit = 15 rows needed
    assertThat(sortStage.getLimitHint()).isEqualTo(15);
  }

  @Test
  void shouldMergeMultipleLimits() {
    // {$limit: 100}, {$limit: 50} -> {$limit: 50} (minimum)
    var limit1 = new LimitStage(100);
    var limit2 = new LimitStage(50);
    var pipeline = Pipeline.of("products", List.of(limit1, limit2));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Should merge to a single limit with minimum value
    assertThat(optimized.getStages()).hasSize(1);
    LimitStage merged = (LimitStage) optimized.getStages().get(0);
    assertThat(merged.getLimit()).isEqualTo(50);
  }

  @Test
  void shouldMergeMultipleSkips() {
    // {$skip: 10}, {$skip: 5} -> {$skip: 15} (sum)
    var skip1 = new SkipStage(10);
    var skip2 = new SkipStage(5);
    var pipeline = Pipeline.of("products", List.of(skip1, skip2));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Should merge to a single skip with sum
    assertThat(optimized.getStages()).hasSize(1);
    SkipStage merged = (SkipStage) optimized.getStages().get(0);
    assertThat(merged.getSkip()).isEqualTo(15);
  }

  @Test
  void shouldHandleMultipleSortsWithDifferentFields() {
    // {$sort: {a: 1}}, {$sort: {b: -1}} -> last sort wins
    var sort1 = createSort("a", 1);
    var sort2 = createSort("b", -1);
    var pipeline = Pipeline.of("products", List.of(sort1, sort2));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Only last sort should remain (first is redundant)
    assertThat(optimized.getStages()).hasSize(1);
    assertThat(optimized.getStages().get(0)).isInstanceOf(SortStage.class);
  }

  @Test
  void shouldHandleMultipleSortsOnSameField() {
    // {$sort: {a: 1}}, {$sort: {a: -1}} -> last sort wins
    var sort1 = createSort("a", 1);
    var sort2 = createSort("a", -1);
    var pipeline = Pipeline.of("products", List.of(sort1, sort2));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Only last sort should remain
    assertThat(optimized.getStages()).hasSize(1);
  }

  @Test
  void shouldHandleSortWithMultipleFields() {
    // {$sort: {a: 1, b: -1}}, {$limit: 10} -> sort with limit hint
    Map<String, Integer> sortSpec = new LinkedHashMap<>();
    sortSpec.put("a", 1);
    sortSpec.put("b", -1);
    var sort = new SortStage(sortSpec);
    var limit = new LimitStage(10);
    var pipeline = Pipeline.of("products", List.of(sort, limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    SortStage sortStage = (SortStage) optimized.getStages().get(0);
    assertThat(sortStage.getLimitHint()).isEqualTo(10);
    assertThat(sortStage.getSortFields()).hasSize(2);
  }

  @Test
  void shouldNotCombineSortLimitWithMatchBetween() {
    // {$sort: ...}, {$match: ...}, {$limit: 10}
    // Cannot combine because match might filter some rows after sort
    var sort = createSort("price", 1);
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT, FieldPathExpression.of("price"), LiteralExpression.of(0)));
    var limit = new LimitStage(10);
    var pipeline = Pipeline.of("products", List.of(sort, match, limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    // Should not have limit hint on sort
    assertThat(optimized.getStages()).hasSize(3);
    SortStage sortStage = (SortStage) optimized.getStages().get(0);
    assertThat(sortStage.getLimitHint()).isNull();
  }

  @Test
  void shouldCombineSortWithVerySmallLimit() {
    // {$sort: {price: 1}}, {$limit: 1} -> special case for min/max
    var sort = createSort("price", 1);
    var limit = new LimitStage(1);
    var pipeline = Pipeline.of("products", List.of(sort, limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    SortStage sortStage = (SortStage) optimized.getStages().get(0);
    assertThat(sortStage.getLimitHint()).isEqualTo(1);
  }

  @Test
  void shouldHandleSortDescendingWithLimit() {
    // {$sort: {price: -1}}, {$limit: 5} -> top 5 most expensive
    var sort = createSort("price", -1);
    var limit = new LimitStage(5);
    var pipeline = Pipeline.of("products", List.of(sort, limit));

    Pipeline optimized = optimizer.optimize(pipeline);

    SortStage sortStage = (SortStage) optimized.getStages().get(0);
    assertThat(sortStage.getLimitHint()).isEqualTo(5);
  }

  private SortStage createSort(String field, int direction) {
    Map<String, Integer> sortSpec = new LinkedHashMap<>();
    sortSpec.put(field, direction);
    return new SortStage(sortSpec);
  }
}
