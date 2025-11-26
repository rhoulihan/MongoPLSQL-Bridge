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
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OptimizationChainTest {

    private OptimizationChain chain;

    @BeforeEach
    void setUp() {
        chain = OptimizationChain.standard();
    }

    @Test
    void shouldApplyMultipleOptimizers() {
        // {$project: {name, status}}, {$sort: {name: 1}}, {$limit: 10}, {$match: {status: "active"}}
        // After optimization:
        // 1. Match should be pushed before project (predicate pushdown)
        // 2. Sort should have limit hint (sort-limit)
        var project = createProjection("name", "status");
        var sort = createSort("name", 1);
        var limit = new LimitStage(10);
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            )
        );
        var pipeline = Pipeline.of("users", List.of(project, sort, limit, match));

        Pipeline optimized = chain.optimize(pipeline);

        // Should have optimized the pipeline
        assertThat(optimized.getStages()).hasSize(4);
        // Match should be first (pushed before project)
        assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
        // Sort should have limit hint
        SortStage sortStage = null;
        for (var stage : optimized.getStages()) {
            if (stage instanceof SortStage s) {
                sortStage = s;
                break;
            }
        }
        assertThat(sortStage).isNotNull();
        assertThat(sortStage.getLimitHint()).isEqualTo(10);
    }

    @Test
    void shouldReturnOptimizerNames() {
        var names = chain.getOptimizerNames();

        assertThat(names).contains("predicate-pushdown", "sort-limit");
    }

    @Test
    void shouldHandleEmptyPipeline() {
        var pipeline = Pipeline.of("users", List.of());

        Pipeline optimized = chain.optimize(pipeline);

        assertThat(optimized.getStages()).isEmpty();
    }

    @Test
    void shouldHandleSingleStagePipeline() {
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            )
        );
        var pipeline = Pipeline.of("users", List.of(match));

        Pipeline optimized = chain.optimize(pipeline);

        assertThat(optimized.getStages()).hasSize(1);
    }

    @Test
    void shouldPreserveCollectionName() {
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("x"),
                LiteralExpression.of(1)
            )
        );
        var pipeline = Pipeline.of("myCollection", List.of(match));

        Pipeline optimized = chain.optimize(pipeline);

        assertThat(optimized.getCollection()).isEqualTo("myCollection");
    }

    @Test
    void shouldAllowCustomOptimizersChain() {
        var customChain = OptimizationChain.builder()
            .add(new PredicatePushdownOptimizer())
            .build();

        var names = customChain.getOptimizerNames();

        assertThat(names).containsExactly("predicate-pushdown");
    }

    private ProjectStage createProjection(String... fields) {
        Map<String, ProjectionField> projections = new LinkedHashMap<>();
        for (String field : fields) {
            projections.put(field, ProjectionField.include(FieldPathExpression.of(field)));
        }
        return new ProjectStage(projections);
    }

    private SortStage createSort(String field, int direction) {
        Map<String, Integer> sortSpec = new LinkedHashMap<>();
        sortSpec.put(field, direction);
        return new SortStage(sortSpec);
    }
}
