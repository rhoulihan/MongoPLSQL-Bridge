/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticOp;
import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.CompoundIdExpression;
import com.oracle.mongodb.translator.ast.expression.ConditionalExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.JsonReturnType;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.AddFieldsStage;
import com.oracle.mongodb.translator.ast.stage.BucketStage;
import com.oracle.mongodb.translator.ast.stage.BucketAutoStage;
import com.oracle.mongodb.translator.ast.stage.FacetStage;
import com.oracle.mongodb.translator.ast.stage.GraphLookupStage;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage.WindowField;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortDirection;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortField;
import com.oracle.mongodb.translator.ast.stage.UnionWithStage;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import com.oracle.mongodb.translator.ast.stage.CountStage;
import com.oracle.mongodb.translator.ast.stage.SampleStage;
import com.oracle.mongodb.translator.ast.stage.RedactStage;
import org.bson.Document;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
        // Use context with "base" alias to match actual translator behavior
        context = new DefaultSqlGenerationContext(false, null, "base");
    }

    @Test
    void shouldRenderEmptyPipeline() {
        Pipeline pipeline = Pipeline.of("orders");

        renderer.render(pipeline, context);

        assertThat(context.toSql()).isEqualTo("SELECT base.data FROM orders base");
    }

    @Test
    void shouldRenderLimitOnly() {
        Pipeline pipeline = Pipeline.of("orders", new LimitStage(10));

        renderer.render(pipeline, context);

        assertThat(context.toSql()).isEqualTo("SELECT base.data FROM orders base FETCH FIRST 10 ROWS ONLY");
    }

    @Test
    void shouldRenderSkipOnly() {
        Pipeline pipeline = Pipeline.of("orders", new SkipStage(5));

        renderer.render(pipeline, context);

        assertThat(context.toSql()).isEqualTo("SELECT base.data FROM orders base OFFSET 5 ROWS");
    }

    @Test
    void shouldRenderSkipAndLimit() {
        Pipeline pipeline = Pipeline.of("orders",
            new SkipStage(10),
            new LimitStage(5)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .isEqualTo("SELECT base.data FROM orders base OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY");
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
            .startsWith("SELECT base.data FROM orders base WHERE")
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
            .isEqualTo("SELECT base.data FROM orders base ORDER BY JSON_VALUE(base.data, '$.createdAt') DESC");
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
            .contains("AS \"_id\"")
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
            .contains("SELECT base.data")
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
        var ctx = new DefaultSqlGenerationContext(false, null, "base");

        Pipeline pipeline = Pipeline.of("order_collection");

        schemaRenderer.render(pipeline, ctx);

        assertThat(ctx.toSql()).isEqualTo("SELECT base.data FROM myschema.order_collection base");
    }

    // Additional tests for better coverage

    @Test
    void shouldRenderLookupStage() {
        Pipeline pipeline = Pipeline.of("orders",
            LookupStage.equality("products", "productId", "_id", "product")
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("LEFT OUTER JOIN")
            .contains("products")
            .contains("product");
    }

    @Test
    void shouldRenderUnwindStage() {
        Pipeline pipeline = Pipeline.of("orders",
            new UnwindStage("items", null, false)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("JSON_TABLE")
            .contains("items");
    }

    @Test
    void shouldRenderUnwindWithIncludeArrayIndex() {
        Pipeline pipeline = Pipeline.of("orders",
            new UnwindStage("items", "itemIndex", false)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("JSON_TABLE")
            .contains("FOR ORDINALITY");
    }

    @Test
    void shouldRenderAddFieldsStage() {
        var fields = new LinkedHashMap<String, Expression>();
        fields.put("fullName", FieldPathExpression.of("name"));

        Pipeline pipeline = Pipeline.of("orders",
            new AddFieldsStage(fields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("SELECT")
            .contains("fullName");
    }

    @Test
    void shouldRenderUnionWithStage() {
        Pipeline pipeline = Pipeline.of("orders",
            new UnionWithStage("inventory", List.of())
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("UNION ALL")
            .contains("inventory");
    }

    @Test
    void shouldRenderUnionWithPipeline() {
        Pipeline pipeline = Pipeline.of("orders",
            new UnionWithStage("inventory", List.of(new LimitStage(5)))
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("UNION ALL")
            .contains("pipeline");
    }

    @Test
    void shouldRenderBucketStage() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        Pipeline pipeline = Pipeline.of("products",
            new BucketStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                List.of(0, 100, 200, 500),
                null,
                accumulators
            )
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("CASE")
            .contains("WHEN")
            .contains("GROUP BY");
    }

    @Test
    void shouldRenderBucketStageWithDefault() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        Pipeline pipeline = Pipeline.of("products",
            new BucketStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                List.of(0, 100, 200),
                "Other",
                accumulators
            )
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("CASE")
            .contains("ELSE");
    }

    @Test
    void shouldRenderBucketStageWithNullLiteral() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        Pipeline pipeline = Pipeline.of("products",
            new BucketStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                List.of(0, 100, 200),
                null,
                accumulators
            )
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("CASE")
            .contains("GROUP BY");
    }

    @Test
    void shouldRenderBucketAutoStage() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        Pipeline pipeline = Pipeline.of("products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                4,
                accumulators,
                null
            )
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("NTILE(4)")
            .contains("GROUP BY");
    }

    @Test
    void shouldRenderFacetStage() {
        var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
        facets.put("byStatus", List.of(
            new GroupStage(FieldPathExpression.of("status"), new LinkedHashMap<>())
        ));

        Pipeline pipeline = Pipeline.of("orders",
            new FacetStage(facets)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("SELECT");
    }

    @Test
    void shouldRenderSetWindowFieldsStage() {
        var output = new LinkedHashMap<String, WindowField>();
        output.put("rank", new WindowField("$rank", null, null));

        Pipeline pipeline = Pipeline.of("orders",
            new SetWindowFieldsStage("$category", Map.of("amount", -1), output)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("RANK()")
            .contains("OVER")
            .contains("PARTITION BY");
    }

    @Test
    void shouldRenderGraphLookupStage() {
        Pipeline pipeline = Pipeline.of("employees",
            new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("LATERAL")
            .contains("JSON_ARRAYAGG")
            .contains("hierarchy");
    }

    @Test
    void shouldRenderGraphLookupWithDepthField() {
        Pipeline pipeline = Pipeline.of("employees",
            new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, "level")
        );

        renderer.render(pipeline, context);

        // Note: depthField is not rendered in LATERAL join implementation for non-recursive case
        assertThat(context.toSql())
            .contains("LATERAL")
            .contains("hierarchy");
    }

    @Test
    void shouldRenderProjectWithExcludedFields() {
        var projections = new LinkedHashMap<String, ProjectionField>();
        projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
        projections.put("password", ProjectionField.exclude());

        Pipeline pipeline = Pipeline.of("users",
            new ProjectStage(projections)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("AS name")
            .doesNotContain("password");
    }

    @Test
    void shouldRenderProjectWithAllExcluded() {
        var projections = new LinkedHashMap<String, ProjectionField>();
        projections.put("password", ProjectionField.exclude());
        projections.put("secret", ProjectionField.exclude());

        Pipeline pipeline = Pipeline.of("users",
            new ProjectStage(projections)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("NULL AS dummy");
    }

    @Test
    void shouldRenderGroupWithEmptyAccumulators() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(null, accumulators)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("NULL AS dummy");
    }

    @Test
    void shouldRenderSortAscending() {
        Pipeline pipeline = Pipeline.of("orders",
            new SortStage(List.of(
                new SortField(FieldPathExpression.of("name"), SortDirection.ASC)
            ))
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("ORDER BY")
            .doesNotContain("DESC");
    }

    @Test
    void shouldRenderMultipleLookupStages() {
        Pipeline pipeline = Pipeline.of("orders",
            LookupStage.equality("customers", "customerId", "_id", "customer"),
            LookupStage.equality("products", "productId", "_id", "product")
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("LEFT OUTER JOIN customers")
            .contains("LEFT OUTER JOIN products");
    }

    @Test
    void shouldRenderMultipleAddFieldsStages() {
        var fields1 = new LinkedHashMap<String, Expression>();
        fields1.put("computed1", LiteralExpression.of(1));

        var fields2 = new LinkedHashMap<String, Expression>();
        fields2.put("computed2", LiteralExpression.of(2));

        Pipeline pipeline = Pipeline.of("orders",
            new AddFieldsStage(fields1),
            new AddFieldsStage(fields2)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("computed1")
            .contains("computed2");
    }

    @Test
    void shouldRenderEmptyAddFieldsStage() {
        var fields = new LinkedHashMap<String, Expression>();

        Pipeline pipeline = Pipeline.of("orders",
            new AddFieldsStage(fields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .isEqualTo("SELECT base.data FROM orders base");
    }

    @Test
    void shouldRenderMultipleGraphLookupStages() {
        Pipeline pipeline = Pipeline.of("employees",
            new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "managers", null, null),
            new GraphLookupStage("employees", "$managerId", "managerId", "_id", "subordinates", null, null)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("LATERAL")
            .contains("managers")
            .contains("subordinates");
    }

    // Tests for $count stage
    @Test
    void shouldRenderCountStage() {
        Pipeline pipeline = Pipeline.of("orders",
            new CountStage("totalCount")
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("JSON_OBJECT")
            .contains("totalCount")
            .contains("COUNT(*)");
    }

    // Tests for $sample stage
    @Test
    void shouldRenderSampleStage() {
        Pipeline pipeline = Pipeline.of("orders",
            new SampleStage(10)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("ORDER BY DBMS_RANDOM.VALUE")
            .contains("FETCH FIRST 10 ROWS ONLY");
    }

    // Tests for $redact stage
    @Test
    void shouldRenderRedactStage() {
        var redactExpr = ConditionalExpression.cond(
            new ComparisonExpression(ComparisonOp.EQ,
                FieldPathExpression.of("level"),
                LiteralExpression.of(5)),
            LiteralExpression.of("$$PRUNE"),
            LiteralExpression.of("$$KEEP")
        );

        Pipeline pipeline = Pipeline.of("orders",
            new RedactStage(redactExpr)
        );

        renderer.render(pipeline, context);

        // $redact adds a WHERE clause that filters where CASE result != $$PRUNE
        assertThat(context.toSql())
            .contains("WHERE")
            .contains("CASE WHEN")
            .contains("<>");
    }

    // Tests for compound _id in group stage
    @Test
    void shouldRenderGroupWithCompoundId() {
        var idFields = new LinkedHashMap<String, Expression>();
        idFields.put("category", FieldPathExpression.of("category"));
        idFields.put("status", FieldPathExpression.of("status"));

        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(new CompoundIdExpression(idFields), accumulators)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("AS category")
            .contains("AS status")
            .contains("GROUP BY");
    }

    // Tests for post-group $addFields
    @Test
    void shouldRenderPostGroupAddFieldsWithArithmetic() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));
        accumulators.put("qty", AccumulatorExpression.sum(FieldPathExpression.of("quantity", JsonReturnType.NUMBER)));

        // Post-group computed field: average = total / qty
        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("average", new ArithmeticExpression(
            ArithmeticOp.DIVIDE,
            List.of(FieldPathExpression.of("total"), FieldPathExpression.of("qty"))
        ));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("category"), accumulators),
            new AddFieldsStage(computedFields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("SELECT inner_query.*")
            .contains("FROM (");
    }

    @Test
    void shouldRenderPostGroupAddFieldsWithConditional() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        // Post-group computed field with conditional
        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("category", ConditionalExpression.cond(
            new ComparisonExpression(ComparisonOp.GT, FieldPathExpression.of("total"), LiteralExpression.of(1000)),
            LiteralExpression.of("high"),
            LiteralExpression.of("low")
        ));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("CASE WHEN")
            .contains("THEN")
            .contains("ELSE")
            .contains("END");
    }

    @Test
    void shouldRenderPostGroupAddFieldsWithIfNull() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        // Post-group computed field with $ifNull
        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("safeTotal", ConditionalExpression.ifNull(
            FieldPathExpression.of("total"),
            LiteralExpression.of(0)
        ));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("NVL(");
    }

    @Test
    void shouldRenderPostGroupAddFieldsWithLiteral() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("label", LiteralExpression.of("constant"));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("SELECT inner_query.*")
            .contains("AS label");
    }

    @Test
    void shouldRenderPostGroupAddFieldsWithSort() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("computed", FieldPathExpression.of("total"));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields),
            new SortStage(List.of(new SortField(FieldPathExpression.of("total"), SortDirection.DESC)))
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("ORDER BY")
            .contains("total");
    }

    @Test
    void shouldRenderPostGroupArithmeticWithMultiply() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("doubled", new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            List.of(FieldPathExpression.of("total"), LiteralExpression.of(2))
        ));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains(" * ");
    }

    @Test
    void shouldRenderPostGroupArithmeticWithSubtract() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("reduced", new ArithmeticExpression(
            ArithmeticOp.SUBTRACT,
            List.of(FieldPathExpression.of("total"), LiteralExpression.of(100))
        ));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains(" - ");
    }

    @Test
    void shouldRenderPostGroupArithmeticWithAdd() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("increased", new ArithmeticExpression(
            ArithmeticOp.ADD,
            List.of(FieldPathExpression.of("total"), LiteralExpression.of(50))
        ));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains(" + ");
    }

    @Test
    void shouldRenderPostGroupArithmeticWithAbs() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

        var computedFields = new LinkedHashMap<String, Expression>();
        computedFields.put("absTotal", new ArithmeticExpression(
            ArithmeticOp.ABS,
            List.of(FieldPathExpression.of("total"))
        ));

        Pipeline pipeline = Pipeline.of("orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("ABS(");
    }

    // Tests for GraphLookup with restrictSearchWithMatch
    @Test
    void shouldRenderGraphLookupWithStringRestrictSearchWithMatch() {
        var restrictMatch = Document.parse("{\"status\": \"active\"}");
        Pipeline pipeline = Pipeline.of("employees",
            new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy",
                null, null, restrictMatch)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("LATERAL")
            .contains("status")
            .contains("active");
    }

    @Test
    void shouldRenderGraphLookupWithBooleanRestrictSearchWithMatch() {
        var restrictMatch = Document.parse("{\"active\": true}");
        Pipeline pipeline = Pipeline.of("employees",
            new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy",
                null, null, restrictMatch)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("LATERAL")
            .contains("active")
            .contains("true");
    }

    @Test
    void shouldRenderGraphLookupWithNumericRestrictSearchWithMatch() {
        var restrictMatch = new Document("level", 5);
        Pipeline pipeline = Pipeline.of("employees",
            new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy",
                null, null, restrictMatch)
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("LATERAL")
            .contains("5");
    }

    // Test sorting after bucket stage
    @Test
    void shouldRenderSortAfterBucket() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        Pipeline pipeline = Pipeline.of("products",
            new BucketStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                List.of(0, 100, 200),
                null,
                accumulators
            ),
            new SortStage(List.of(new SortField(FieldPathExpression.of("count"), SortDirection.DESC)))
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("GROUP BY")
            .contains("ORDER BY")
            .contains("count");
    }

    // Test sorting after bucketAuto
    @Test
    void shouldRenderSortAfterBucketAuto() {
        var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
        accumulators.put("count", AccumulatorExpression.count());

        Pipeline pipeline = Pipeline.of("products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                4,
                accumulators,
                null
            ),
            new SortStage(List.of(new SortField(FieldPathExpression.of("count"), SortDirection.DESC)))
        );

        renderer.render(pipeline, context);

        assertThat(context.toSql())
            .contains("NTILE")
            .contains("ORDER BY")
            .contains("count");
    }

    // Test with context without base alias
    @Test
    void shouldRenderWithoutBaseAlias() {
        OracleConfiguration config = OracleConfiguration.builder()
            .collectionName("orders")
            .build();
        PipelineRenderer localRenderer = new PipelineRenderer(config);
        var ctx = new DefaultSqlGenerationContext(false, null, "");

        Pipeline pipeline = Pipeline.of("orders");

        localRenderer.render(pipeline, ctx);

        assertThat(ctx.toSql()).isEqualTo("SELECT data FROM orders ");
    }
}
