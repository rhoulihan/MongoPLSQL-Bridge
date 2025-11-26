/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.JsonReturnType;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortDirection;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortField;
import java.util.LinkedHashMap;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PipelineRendererTest {

    private PipelineRenderer renderer;
    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        OracleConfiguration config = OracleConfiguration.builder()
            .collectionName("orders")
            .build();
        renderer = new PipelineRenderer(config);
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderEmptyPipeline() {
        Pipeline pipeline = Pipeline.of("orders");

        renderer.render(pipeline, context);

        assertThat(context.toSql()).isEqualTo("SELECT data FROM orders");
    }

    @Test
    void shouldRenderLimitOnly() {
        Pipeline pipeline = Pipeline.of("orders", new LimitStage(10));

        renderer.render(pipeline, context);

        assertThat(context.toSql()).isEqualTo("SELECT data FROM orders FETCH FIRST 10 ROWS ONLY");
    }

    @Test
    void shouldRenderSkipOnly() {
        Pipeline pipeline = Pipeline.of("orders", new SkipStage(5));

        renderer.render(pipeline, context);

        assertThat(context.toSql()).isEqualTo("SELECT data FROM orders OFFSET 5 ROWS");
    }

    @Test
    void shouldRenderSkipAndLimit() {
        Pipeline pipeline = Pipeline.of("orders",
            new SkipStage(10),
            new LimitStage(5)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .isEqualTo("SELECT data FROM orders OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY");
    }

    @Test
    void shouldRenderMatchStage() {
        var filter = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );
        Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .startsWith("SELECT data FROM orders WHERE")
            .contains("$.status");
    }

    @Test
    void shouldCombineMultipleMatchStagesWithAnd() {
        var filter1 = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );
        var filter2 = new ComparisonExpression(
            ComparisonOp.GT,
            FieldPathExpression.of("amount", JsonReturnType.NUMBER),
            LiteralExpression.of(100)
        );

        Pipeline pipeline = Pipeline.of("orders",
            new MatchStage(filter1),
            new MatchStage(filter2)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("WHERE")
            .contains("AND")
            .contains("$.status")
            .contains("$.amount");
    }

    @Test
    void shouldRenderSortStage() {
        Pipeline pipeline = Pipeline.of("orders",
            new SortStage(List.of(
                new SortField(FieldPathExpression.of("createdAt"), SortDirection.DESC)
            ))
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .isEqualTo("SELECT data FROM orders ORDER BY JSON_VALUE(data, '$.createdAt') DESC");
    }

    @Test
    void shouldRenderMultipleSortFields() {
        Pipeline pipeline = Pipeline.of("orders",
            new SortStage(List.of(
                new SortField(FieldPathExpression.of("status"), SortDirection.ASC),
                new SortField(FieldPathExpression.of("amount", JsonReturnType.NUMBER), SortDirection.DESC)
            ))
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("ORDER BY")
            .contains("$.status")
            .contains("$.amount")
            .contains("DESC");
    }

    @Test
    void shouldRenderGroupStage() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(
            FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("SELECT")
            .contains("AS _id")
            .contains("SUM(")
            .contains("AS total")
            .contains("GROUP BY");
    }

    @Test
    void shouldRenderGroupStageWithNullId() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(null, accumulators)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("SELECT COUNT(*) AS count")
            .doesNotContain("GROUP BY");
    }

    @Test
    void shouldRenderProjectStage() {
        var projections = new LinkedHashMap<String, ProjectionField>();
        projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
        projections.put("status", ProjectionField.include(FieldPathExpression.of("status")));

        Pipeline pipeline = Pipeline.of("orders",
            new ProjectStage(projections)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("SELECT")
            .contains("AS name")
            .contains("AS status")
            .doesNotContain("data FROM");
    }

    @Test
    void shouldRenderComplexPipeline() {
        // { $match: { status: "active" } }
        // { $group: { _id: "$category", total: { $sum: "$amount" } } }
        // { $sort: { total: -1 } }
        // { $limit: 10 }

        var filter = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );

        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(
            FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        Pipeline pipeline = Pipeline.of("orders",
            new MatchStage(filter),
            new GroupStage(FieldPathExpression.of("category"), accumulators),
            new SortStage(List.of(
                new SortField(FieldPathExpression.of("total", JsonReturnType.NUMBER), SortDirection.DESC)
            )),
            new LimitStage(10)
        );

        renderer.render(pipeline, context);

        String sql = context.toSql();

        // Verify clause order
        int selectPos = sql.indexOf("SELECT");
        int fromPos = sql.indexOf("FROM");
        int wherePos = sql.indexOf("WHERE");
        int groupByPos = sql.indexOf("GROUP BY");
        int orderByPos = sql.indexOf("ORDER BY");
        int fetchPos = sql.indexOf("FETCH FIRST");

        assertThat(selectPos).isLessThan(fromPos);
        assertThat(fromPos).isLessThan(wherePos);
        assertThat(wherePos).isLessThan(groupByPos);
        assertThat(groupByPos).isLessThan(orderByPos);
        assertThat(orderByPos).isLessThan(fetchPos);
    }

    @Test
    void shouldRenderMatchSortSkipLimit() {
        var filter = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );

        Pipeline pipeline = Pipeline.of("orders",
            new MatchStage(filter),
            new SortStage(List.of(
                new SortField(FieldPathExpression.of("createdAt"), SortDirection.DESC)
            )),
            new SkipStage(10),
            new LimitStage(5)
        );

        renderer.render(pipeline, context);

        String sql = context.toSql();

        assertThat(sql)
            .contains("SELECT data")
            .contains("WHERE")
            .contains("ORDER BY")
            .contains("OFFSET 10 ROWS")
            .contains("FETCH FIRST 5 ROWS ONLY");

        // Verify order
        int wherePos = sql.indexOf("WHERE");
        int orderByPos = sql.indexOf("ORDER BY");
        int offsetPos = sql.indexOf("OFFSET");
        int fetchPos = sql.indexOf("FETCH");

        assertThat(wherePos).isLessThan(orderByPos);
        assertThat(orderByPos).isLessThan(offsetPos);
        assertThat(offsetPos).isLessThan(fetchPos);
    }

    @Test
    void shouldUseSchemaAndTableFromConfig() {
        OracleConfiguration config = OracleConfiguration.builder()
            .collectionName("order_collection")
            .schemaName("myschema")
            .build();
        PipelineRenderer schemaRenderer = new PipelineRenderer(config);
        var ctx = new DefaultSqlGenerationContext();

        Pipeline pipeline = Pipeline.of("order_collection");

        schemaRenderer.render(pipeline, ctx);

        assertThat(ctx.toSql()).isEqualTo("SELECT data FROM myschema.order_collection");
    }
}
