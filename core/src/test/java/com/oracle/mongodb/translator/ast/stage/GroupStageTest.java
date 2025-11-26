/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.JsonReturnType;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GroupStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderGroupByWithSingleField() {
        // { $group: { _id: "$status" } }
        var stage = new GroupStage(FieldPathExpression.of("status"));

        stage.render(context);

        assertThat(context.toSql())
            .isEqualTo("SELECT JSON_VALUE(data, '$.status') AS _id GROUP BY JSON_VALUE(data, '$.status')");
    }

    @Test
    void shouldRenderGroupByWithAccumulator() {
        // { $group: { _id: "$status", totalAmount: { $sum: "$amount" } } }
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("totalAmount", AccumulatorExpression.sum(
            FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var stage = new GroupStage(FieldPathExpression.of("status"), accumulators);

        stage.render(context);

        assertThat(context.toSql())
            .contains("SELECT")
            .contains("JSON_VALUE(data, '$.status') AS _id")
            .contains("SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS totalAmount")
            .contains("GROUP BY JSON_VALUE(data, '$.status')");
    }

    @Test
    void shouldRenderGroupByWithMultipleAccumulators() {
        // { $group: { _id: "$category", count: { $sum: 1 }, avgPrice: { $avg: "$price" } } }
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.sum(LiteralExpression.of(1)));
        accumulators.put("avgPrice", AccumulatorExpression.avg(
            FieldPathExpression.of("price", JsonReturnType.NUMBER)));

        var stage = new GroupStage(FieldPathExpression.of("category"), accumulators);

        stage.render(context);

        assertThat(context.toSql())
            .contains("SELECT")
            .contains("AS _id")
            .contains("AS count")
            .contains("AS avgPrice")
            .contains("GROUP BY");
    }

    @Test
    void shouldRenderGroupAllWithCount() {
        // { $group: { _id: null, count: { $count: {} } } }
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        var stage = new GroupStage(null, accumulators);

        stage.render(context);

        assertThat(context.toSql())
            .isEqualTo("SELECT COUNT(*) AS count");
    }

    @Test
    void shouldRenderGroupAllWithMinMax() {
        // { $group: { _id: null, minScore: { $min: "$score" }, maxScore: { $max: "$score" } } }
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("minScore", AccumulatorExpression.min(
            FieldPathExpression.of("score", JsonReturnType.NUMBER)));
        accumulators.put("maxScore", AccumulatorExpression.max(
            FieldPathExpression.of("score", JsonReturnType.NUMBER)));

        var stage = new GroupStage(null, accumulators);

        stage.render(context);

        assertThat(context.toSql())
            .startsWith("SELECT")
            .contains("MIN(")
            .contains("MAX(")
            .doesNotContain("GROUP BY");
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new GroupStage(FieldPathExpression.of("status"));

        assertThat(stage.getOperatorName()).isEqualTo("$group");
    }

    @Test
    void shouldReturnIdExpression() {
        var idExpr = FieldPathExpression.of("status");
        var stage = new GroupStage(idExpr);

        assertThat(stage.getIdExpression()).isEqualTo(idExpr);
    }

    @Test
    void shouldReturnAccumulators() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());
        var stage = new GroupStage(null, accumulators);

        assertThat(stage.getAccumulators()).containsKey("count");
    }

    @Test
    void shouldProvideReadableToString() {
        var stage = new GroupStage(FieldPathExpression.of("status"));

        assertThat(stage.toString()).contains("GroupStage");
    }
}
