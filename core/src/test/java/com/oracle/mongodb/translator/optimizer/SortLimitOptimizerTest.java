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
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("price"),
                LiteralExpression.of(0)
            )
        );
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

    private SortStage createSort(String field, int direction) {
        Map<String, Integer> sortSpec = new LinkedHashMap<>();
        sortSpec.put(field, direction);
        return new SortStage(sortSpec);
    }
}
