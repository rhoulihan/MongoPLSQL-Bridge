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
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            )
        );
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
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            )
        );
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
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            )
        );
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
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("age"),
                LiteralExpression.of(18)
            )
        );
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
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            )
        );
        var pipeline = Pipeline.of("users", List.of(match));

        Pipeline optimized = optimizer.optimize(pipeline);

        assertThat(optimized.getStages()).hasSize(1);
        assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    }

    @Test
    void shouldMergeConsecutiveMatchStages() {
        // {$match: {a: 1}}, {$match: {b: 2}} -> {$match: {$and: [{a: 1}, {b: 2}]}}
        var match1 = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("a"),
                LiteralExpression.of(1)
            )
        );
        var match2 = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("b"),
                LiteralExpression.of(2)
            )
        );
        var pipeline = Pipeline.of("users", List.of(match1, match2));

        Pipeline optimized = optimizer.optimize(pipeline);

        // Should merge into single match with AND
        assertThat(optimized.getStages()).hasSize(1);
        assertThat(optimized.getStages().get(0)).isInstanceOf(MatchStage.class);
    }

    @Test
    void shouldPreserveCollectionName() {
        var match = new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("active")
            )
        );
        var pipeline = Pipeline.of("myCollection", List.of(match));

        Pipeline optimized = optimizer.optimize(pipeline);

        assertThat(optimized.getCollection()).isEqualTo("myCollection");
    }
}
