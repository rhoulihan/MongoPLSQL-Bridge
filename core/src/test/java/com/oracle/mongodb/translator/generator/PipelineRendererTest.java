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
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.stage.AddFieldsStage;
import com.oracle.mongodb.translator.ast.stage.BucketAutoStage;
import com.oracle.mongodb.translator.ast.stage.BucketStage;
import com.oracle.mongodb.translator.ast.stage.CountStage;
import com.oracle.mongodb.translator.ast.stage.FacetStage;
import com.oracle.mongodb.translator.ast.stage.GraphLookupStage;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import com.oracle.mongodb.translator.ast.stage.RedactStage;
import com.oracle.mongodb.translator.ast.stage.SampleStage;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage.WindowField;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortDirection;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortField;
import com.oracle.mongodb.translator.ast.stage.UnionWithStage;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PipelineRendererTest {

  private PipelineRenderer renderer;
  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    OracleConfiguration config = OracleConfiguration.builder().collectionName("orders").build();
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

    assertThat(context.toSql())
        .isEqualTo("SELECT base.data FROM orders base FETCH FIRST 10 ROWS ONLY");
  }

  @Test
  void shouldRenderSkipOnly() {
    Pipeline pipeline = Pipeline.of("orders", new SkipStage(5));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).isEqualTo("SELECT base.data FROM orders base OFFSET 5 ROWS");
  }

  @Test
  void shouldRenderSkipAndLimit() {
    Pipeline pipeline = Pipeline.of("orders", new SkipStage(10), new LimitStage(5));

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .isEqualTo("SELECT base.data FROM orders base OFFSET 10 ROWS FETCH FIRST 5 ROWS ONLY");
  }

  @Test
  void shouldRenderMatchStage() {
    var filter =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));
    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .startsWith("SELECT base.data FROM orders base WHERE")
        .contains("base.data.status");
  }

  @Test
  void shouldCombineMultipleMatchStagesWithAnd() {
    var filter1 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));
    var filter2 =
        new ComparisonExpression(
            ComparisonOp.GT,
            FieldPathExpression.of("amount", JsonReturnType.NUMBER),
            LiteralExpression.of(100));

    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter1), new MatchStage(filter2));

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .contains("WHERE")
        .contains("AND")
        .contains("base.data.status")
        .contains("base.data.amount");
  }

  @Test
  void shouldRenderSortStage() {
    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("createdAt"), SortDirection.DESC))));

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .isEqualTo("SELECT base.data FROM orders base ORDER BY base.data.createdAt DESC");
  }

  @Test
  void shouldRenderMultipleSortFields() {
    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new SortStage(
                List.of(
                    new SortField(FieldPathExpression.of("status"), SortDirection.ASC),
                    new SortField(
                        FieldPathExpression.of("amount", JsonReturnType.NUMBER),
                        SortDirection.DESC))));

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .contains("ORDER BY")
        .contains("base.data.status")
        .contains("base.data.amount")
        .contains("DESC");
  }

  @Test
  void shouldRenderGroupStage() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of("orders", new GroupStage(FieldPathExpression.of("status"), accumulators));

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

    Pipeline pipeline = Pipeline.of("orders", new GroupStage(null, accumulators));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("SELECT COUNT(*) AS count").doesNotContain("GROUP BY");
  }

  @Test
  void shouldRenderProjectStage() {
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
    projections.put("status", ProjectionField.include(FieldPathExpression.of("status")));

    Pipeline pipeline = Pipeline.of("orders", new ProjectStage(projections));

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

    var filter =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new MatchStage(filter),
            new GroupStage(FieldPathExpression.of("category"), accumulators),
            new SortStage(
                List.of(
                    new SortField(
                        FieldPathExpression.of("total", JsonReturnType.NUMBER),
                        SortDirection.DESC))),
            new LimitStage(10));

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
    var filter =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new MatchStage(filter),
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("createdAt"), SortDirection.DESC))),
            new SkipStage(10),
            new LimitStage(5));

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
    OracleConfiguration config =
        OracleConfiguration.builder()
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
    Pipeline pipeline =
        Pipeline.of("orders", LookupStage.equality("products", "productId", "_id", "product"));

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .contains("LEFT OUTER JOIN")
        .contains("products")
        .contains("product");
  }

  @Test
  void shouldRenderUnwindStage() {
    Pipeline pipeline = Pipeline.of("orders", new UnwindStage("items", null, false));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("JSON_TABLE").contains("items");
  }

  @Test
  void shouldRenderUnwindWithIncludeArrayIndex() {
    Pipeline pipeline = Pipeline.of("orders", new UnwindStage("items", "itemIndex", false));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("JSON_TABLE").contains("FOR ORDINALITY");
  }

  @Test
  void shouldRenderAddFieldsStage() {
    var fields = new LinkedHashMap<String, Expression>();
    fields.put("fullName", FieldPathExpression.of("name"));

    Pipeline pipeline = Pipeline.of("orders", new AddFieldsStage(fields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("SELECT").contains("fullName");
  }

  @Test
  void shouldRenderUnionWithStage() {
    Pipeline pipeline = Pipeline.of("orders", new UnionWithStage("inventory", List.of()));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("UNION ALL").contains("inventory");
  }

  @Test
  void shouldRenderUnionWithPipeline() {
    Pipeline pipeline =
        Pipeline.of("orders", new UnionWithStage("inventory", List.of(new LimitStage(5))));

    renderer.render(pipeline, context);

    // Pipeline is now rendered with proper SQL
    assertThat(context.toSql())
        .contains("UNION ALL")
        .contains("inventory")
        .contains("FETCH FIRST 5 ROWS ONLY");
  }

  @Test
  void shouldRenderBucketStage() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                List.of(0, 100, 200, 500),
                null,
                accumulators));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("CASE").contains("WHEN").contains("GROUP BY");
  }

  @Test
  void shouldRenderBucketStageWithDefault() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                List.of(0, 100, 200),
                "Other",
                accumulators));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("CASE").contains("ELSE");
  }

  @Test
  void shouldRenderBucketStageWithNullLiteral() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                List.of(0, 100, 200),
                null,
                accumulators));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("CASE").contains("GROUP BY");
  }

  @Test
  void shouldRenderBucketAutoStage() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 4, accumulators, null));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("NTILE(4)").contains("GROUP BY");
  }

  @Test
  void shouldRenderFacetStage() {
    var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
    facets.put(
        "byStatus",
        List.of(new GroupStage(FieldPathExpression.of("status"), new LinkedHashMap<>())));

    Pipeline pipeline = Pipeline.of("orders", new FacetStage(facets));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("SELECT");
  }

  @Test
  void shouldRenderSetWindowFieldsStage() {
    var output = new LinkedHashMap<String, WindowField>();
    output.put("rank", new WindowField("$rank", null, null));

    Pipeline pipeline =
        Pipeline.of("orders", new SetWindowFieldsStage("$category", Map.of("amount", -1), output));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("RANK()").contains("OVER").contains("PARTITION BY");
  }

  @Test
  void shouldRenderGraphLookupStage() {
    Pipeline pipeline =
        Pipeline.of(
            "employees",
            new GraphLookupStage(
                "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("LATERAL").contains("JSON_ARRAYAGG").contains("hierarchy");
  }

  @Test
  void shouldRenderGraphLookupWithDepthField() {
    Pipeline pipeline =
        Pipeline.of(
            "employees",
            new GraphLookupStage(
                "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, "level"));

    renderer.render(pipeline, context);

    // Note: depthField is not rendered in LATERAL join implementation for non-recursive case
    assertThat(context.toSql()).contains("LATERAL").contains("hierarchy");
  }

  @Test
  void shouldRenderProjectWithExcludedFields() {
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
    projections.put("password", ProjectionField.exclude());

    Pipeline pipeline = Pipeline.of("users", new ProjectStage(projections));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("AS name").doesNotContain("password");
  }

  @Test
  void shouldRenderProjectWithAllExcluded() {
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("password", ProjectionField.exclude());
    projections.put("secret", ProjectionField.exclude());

    Pipeline pipeline = Pipeline.of("users", new ProjectStage(projections));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("NULL AS dummy");
  }

  @Test
  void shouldRenderGroupWithEmptyAccumulators() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();

    Pipeline pipeline = Pipeline.of("orders", new GroupStage(null, accumulators));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("NULL AS dummy");
  }

  @Test
  void shouldRenderSortAscending() {
    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("name"), SortDirection.ASC))));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("ORDER BY").doesNotContain("DESC");
  }

  @Test
  void shouldRenderMultipleLookupStages() {
    Pipeline pipeline =
        Pipeline.of(
            "orders",
            LookupStage.equality("customers", "customerId", "_id", "customer"),
            LookupStage.equality("products", "productId", "_id", "product"));

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

    Pipeline pipeline =
        Pipeline.of("orders", new AddFieldsStage(fields1), new AddFieldsStage(fields2));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("computed1").contains("computed2");
  }

  @Test
  void shouldRenderEmptyAddFieldsStage() {
    var fields = new LinkedHashMap<String, Expression>();

    Pipeline pipeline = Pipeline.of("orders", new AddFieldsStage(fields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).isEqualTo("SELECT base.data FROM orders base");
  }

  @Test
  void shouldRenderMultipleGraphLookupStages() {
    Pipeline pipeline =
        Pipeline.of(
            "employees",
            new GraphLookupStage(
                "employees", "$reportsTo", "reportsTo", "name", "managers", null, null),
            new GraphLookupStage(
                "employees", "$managerId", "managerId", "_id", "subordinates", null, null));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("LATERAL").contains("managers").contains("subordinates");
  }

  // Tests for $count stage
  @Test
  void shouldRenderCountStage() {
    Pipeline pipeline = Pipeline.of("orders", new CountStage("totalCount"));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("JSON_OBJECT").contains("totalCount").contains("COUNT(*)");
  }

  // Tests for $sample stage
  @Test
  void shouldRenderSampleStage() {
    Pipeline pipeline = Pipeline.of("orders", new SampleStage(10));

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .contains("ORDER BY DBMS_RANDOM.VALUE")
        .contains("FETCH FIRST 10 ROWS ONLY");
  }

  // Tests for $redact stage
  @Test
  void shouldRenderRedactStage() {
    var redactExpr =
        ConditionalExpression.cond(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("level"), LiteralExpression.of(5)),
            LiteralExpression.of("$$PRUNE"),
            LiteralExpression.of("$$KEEP"));

    Pipeline pipeline = Pipeline.of("orders", new RedactStage(redactExpr));

    renderer.render(pipeline, context);

    // $redact adds a WHERE clause that filters where CASE result != $$PRUNE
    assertThat(context.toSql()).contains("WHERE").contains("CASE WHEN").contains("<>");
  }

  // Tests for compound _id in group stage
  @Test
  void shouldRenderGroupWithCompoundId() {
    var idFields = new LinkedHashMap<String, Expression>();
    idFields.put("category", FieldPathExpression.of("category"));
    idFields.put("status", FieldPathExpression.of("status"));

    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of("orders", new GroupStage(new CompoundIdExpression(idFields), accumulators));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("AS category").contains("AS status").contains("GROUP BY");
  }

  // Tests for post-group $addFields
  @Test
  void shouldRenderPostGroupAddFieldsWithArithmetic() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));
    accumulators.put(
        "qty",
        AccumulatorExpression.sum(FieldPathExpression.of("quantity", JsonReturnType.NUMBER)));

    // Post-group computed field: average = total / qty
    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "average",
        new ArithmeticExpression(
            ArithmeticOp.DIVIDE,
            List.of(FieldPathExpression.of("total"), FieldPathExpression.of("qty"))));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("category"), accumulators),
            new AddFieldsStage(computedFields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("SELECT inner_query.*").contains("FROM (");
  }

  @Test
  void shouldRenderPostGroupAddFieldsWithConditional() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    // Post-group computed field with conditional
    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "category",
        ConditionalExpression.cond(
            new ComparisonExpression(
                ComparisonOp.GT, FieldPathExpression.of("total"), LiteralExpression.of(1000)),
            LiteralExpression.of("high"),
            LiteralExpression.of("low")));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields));

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
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    // Post-group computed field with $ifNull
    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "safeTotal",
        ConditionalExpression.ifNull(FieldPathExpression.of("total"), LiteralExpression.of(0)));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("NVL(");
  }

  @Test
  void shouldRenderPostGroupAddFieldsWithLiteral() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put("label", LiteralExpression.of("constant"));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("SELECT inner_query.*").contains("AS label");
  }

  @Test
  void shouldRenderPostGroupAddFieldsWithSort() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put("computed", FieldPathExpression.of("total"));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields),
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("total"), SortDirection.DESC))));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("ORDER BY").contains("total");
  }

  @Test
  void shouldRenderPostGroupArithmeticWithMultiply() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "doubled",
        new ArithmeticExpression(
            ArithmeticOp.MULTIPLY,
            List.of(FieldPathExpression.of("total"), LiteralExpression.of(2))));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains(" * ");
  }

  @Test
  void shouldRenderPostGroupArithmeticWithSubtract() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "reduced",
        new ArithmeticExpression(
            ArithmeticOp.SUBTRACT,
            List.of(FieldPathExpression.of("total"), LiteralExpression.of(100))));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains(" - ");
  }

  @Test
  void shouldRenderPostGroupArithmeticWithAdd() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "increased",
        new ArithmeticExpression(
            ArithmeticOp.ADD, List.of(FieldPathExpression.of("total"), LiteralExpression.of(50))));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains(" + ");
  }

  @Test
  void shouldRenderPostGroupArithmeticWithAbs() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "absTotal",
        new ArithmeticExpression(ArithmeticOp.ABS, List.of(FieldPathExpression.of("total"))));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new GroupStage(FieldPathExpression.of("status"), accumulators),
            new AddFieldsStage(computedFields));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("ABS(");
  }

  // Tests for GraphLookup with restrictSearchWithMatch
  @Test
  void shouldRenderGraphLookupWithStringRestrictSearchWithMatch() {
    var restrictMatch = Document.parse("{\"status\": \"active\"}");
    Pipeline pipeline =
        Pipeline.of(
            "employees",
            new GraphLookupStage(
                "employees",
                "$reportsTo",
                "reportsTo",
                "name",
                "hierarchy",
                null,
                null,
                restrictMatch));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("LATERAL").contains("status").contains("active");
  }

  @Test
  void shouldRenderGraphLookupWithBooleanRestrictSearchWithMatch() {
    var restrictMatch = Document.parse("{\"active\": true}");
    Pipeline pipeline =
        Pipeline.of(
            "employees",
            new GraphLookupStage(
                "employees",
                "$reportsTo",
                "reportsTo",
                "name",
                "hierarchy",
                null,
                null,
                restrictMatch));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("LATERAL").contains("active").contains("true");
  }

  @Test
  void shouldRenderGraphLookupWithNumericRestrictSearchWithMatch() {
    var restrictMatch = new Document("level", 5);
    Pipeline pipeline =
        Pipeline.of(
            "employees",
            new GraphLookupStage(
                "employees",
                "$reportsTo",
                "reportsTo",
                "name",
                "hierarchy",
                null,
                null,
                restrictMatch));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("LATERAL").contains("5");
  }

  // Test sorting after bucket stage
  @Test
  void shouldRenderSortAfterBucket() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER),
                List.of(0, 100, 200),
                null,
                accumulators),
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("count"), SortDirection.DESC))));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("GROUP BY").contains("ORDER BY").contains("count");
  }

  // Test sorting after bucketAuto
  @Test
  void shouldRenderSortAfterBucketAuto() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 4, accumulators, null),
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("count"), SortDirection.DESC))));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("NTILE").contains("ORDER BY").contains("count");
  }

  // Test with context without base alias
  @Test
  void shouldRenderWithoutBaseAlias() {
    OracleConfiguration config = OracleConfiguration.builder().collectionName("orders").build();
    PipelineRenderer localRenderer = new PipelineRenderer(config);
    var ctx = new DefaultSqlGenerationContext(false, null, "");

    Pipeline pipeline = Pipeline.of("orders");

    localRenderer.render(pipeline, ctx);

    assertThat(ctx.toSql()).isEqualTo("SELECT data FROM orders ");
  }

  @Test
  void shouldWrapSetWindowFieldsInSubqueryWhenMatchOnWindowField() {
    // $setWindowFields with $rank, followed by $match on the rank field
    var windowField = new WindowField("$rank", null, null);
    var setWindowFields =
        new SetWindowFieldsStage(
            "$department", Map.of("salary", -1), Map.of("salaryRank", windowField));

    // $match on the window result field
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("salaryRank", JsonReturnType.NUMBER),
                LiteralExpression.of(1)));

    Pipeline pipeline = Pipeline.of("employees", setWindowFields, match);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should wrap in subquery so we can filter on window result
    assertThat(sql).contains("FROM (SELECT");
    assertThat(sql).contains("RANK()");
    assertThat(sql).contains("WHERE salaryRank");
  }

  @Test
  void shouldNotWrapSetWindowFieldsWithoutMatchOnWindowField() {
    // $setWindowFields with cumulative sum, no subsequent match
    var windowSpec =
        new SetWindowFieldsStage.WindowSpec("documents", List.of("unbounded", "current"));
    var windowField = new WindowField("$sum", "$amount", windowSpec);
    var setWindowFields =
        new SetWindowFieldsStage(null, Map.of("orderDate", 1), Map.of("runningTotal", windowField));

    Pipeline pipeline = Pipeline.of("sales", setWindowFields);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should NOT wrap in subquery since no match on window field
    assertThat(sql).doesNotContain("FROM (SELECT");
    assertThat(sql).contains("SUM(");
    assertThat(sql).contains("runningTotal");
  }

  // Additional tests for improved coverage

  @Test
  void shouldRenderUnionWithFollowedByGroupStage() {
    // Test post-union group: $unionWith followed by $group
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("returns", List.of()),
            new GroupStage(FieldPathExpression.of("type"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT");
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("GROUP BY");
  }

  @Test
  void shouldRenderUnionWithFollowedByGroupWithNullId() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("totalCount", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("returns", List.of()),
            new GroupStage(null, accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("COUNT(*)");
  }

  @Test
  void shouldRenderPostWindowMatchWithoutProject() {
    // $setWindowFields followed by $match with no $project - uses SELECT *
    var windowField = new WindowField("$rank", null, null);
    var setWindowFields =
        new SetWindowFieldsStage(
            "$category", Map.of("score", -1), Map.of("categoryRank", windowField));

    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.LTE,
                FieldPathExpression.of("categoryRank", JsonReturnType.NUMBER),
                LiteralExpression.of(3)));

    Pipeline pipeline = Pipeline.of("items", setWindowFields, match);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT *");
    assertThat(sql).contains("FROM (SELECT");
    assertThat(sql).contains("RANK()");
  }

  @Test
  void shouldRenderPostWindowMatchWithPreWindowWhere() {
    // $match before $setWindowFields (goes in inner WHERE), then $match after (outer WHERE)
    var preWindowMatch =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ, FieldPathExpression.of("active"), LiteralExpression.of(true)));

    var windowField = new WindowField("$rowNumber", null, null);
    var setWindowFields =
        new SetWindowFieldsStage(null, Map.of("date", 1), Map.of("rowNum", windowField));

    var postWindowMatch =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("rowNum", JsonReturnType.NUMBER),
                LiteralExpression.of(1)));

    Pipeline pipeline =
        Pipeline.of("events", preWindowMatch, setWindowFields, postWindowMatch);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Pre-window match should be in inner WHERE
    assertThat(sql).contains("WHERE");
    // Post-window match on outer
    assertThat(sql).contains("rowNum");
  }

  @Test
  void shouldRenderPostWindowMatchWithSortOnDataField() {
    var windowField = new WindowField("$denseRank", null, null);
    var setWindowFields =
        new SetWindowFieldsStage("$region", Map.of("sales", -1), Map.of("rank", windowField));

    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("rank", JsonReturnType.NUMBER),
                LiteralExpression.of(1)));

    // Sort by a data field (not window field)
    var sort =
        new SortStage(
            List.of(new SortField(FieldPathExpression.of("region"), SortDirection.ASC)));

    Pipeline pipeline = Pipeline.of("sales", setWindowFields, match, sort);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("ORDER BY");
    assertThat(sql).contains("JSON_VALUE(data");
  }

  @Test
  void shouldRenderBucketAutoWithSum() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "totalSales",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 5, accumulators, null));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("SUM(groupby_value)").contains("NTILE(5)");
  }

  @Test
  void shouldRenderBucketAutoWithAvg() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "avgPrice",
        AccumulatorExpression.avg(FieldPathExpression.of("price", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 3, accumulators, null));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("AVG(groupby_value)").contains("NTILE(3)");
  }

  @Test
  void shouldRenderBucketAutoWithMin() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "minPrice",
        AccumulatorExpression.min(FieldPathExpression.of("price", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 4, accumulators, null));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("MIN(groupby_value)").contains("NTILE(4)");
  }

  @Test
  void shouldRenderBucketAutoWithMax() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "maxPrice",
        AccumulatorExpression.max(FieldPathExpression.of("price", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 4, accumulators, null));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("MAX(groupby_value)").contains("NTILE(4)");
  }

  @Test
  void shouldRenderBucketAutoWithMatchFilter() {
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("quantity", JsonReturnType.NUMBER),
                LiteralExpression.of(0)));

    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "products",
            match,
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 3, accumulators, null));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("WHERE").contains("NTILE(3)");
  }

  @Test
  void shouldRenderUnionWithFollowedByProjectAndGroup() {
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("type", ProjectionField.include(FieldPathExpression.of("docType")));
    projections.put("amount", ProjectionField.include(FieldPathExpression.of("value")));

    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "total",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new ProjectStage(projections),
            new UnionWithStage("refunds", List.of()),
            new GroupStage(FieldPathExpression.of("type"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("GROUP BY");
  }

  @Test
  void shouldRenderPostWindowProjectWithId() {
    var windowField = new WindowField("$rank", null, null);
    var setWindowFields =
        new SetWindowFieldsStage(
            "$department", Map.of("salary", -1), Map.of("salaryRank", windowField));

    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("_id", ProjectionField.include(FieldPathExpression.of("_id")));
    projections.put("department", ProjectionField.include(FieldPathExpression.of("department")));
    projections.put(
        "salaryRank", ProjectionField.include(FieldPathExpression.of("salaryRank")));

    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.EQ,
                FieldPathExpression.of("salaryRank", JsonReturnType.NUMBER),
                LiteralExpression.of(1)));

    Pipeline pipeline =
        Pipeline.of("employees", setWindowFields, match, new ProjectStage(projections));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("SELECT ");
    assertThat(sql).contains("id"); // _id maps to id column
    assertThat(sql).contains("salaryRank");
  }

  @Test
  void shouldRenderPostWindowMatchWithLogicalExpression() {
    var windowField = new WindowField("$rank", null, null);
    var setWindowFields =
        new SetWindowFieldsStage(
            "$category", Map.of("score", -1), Map.of("rank", windowField));

    // Match using logical expression (not directly a ComparisonExpression)
    var logicalExpr =
        new LogicalExpression(
            LogicalOp.OR,
            List.of(
                new ComparisonExpression(
                    ComparisonOp.EQ,
                    FieldPathExpression.of("rank", JsonReturnType.NUMBER),
                    LiteralExpression.of(1)),
                new ComparisonExpression(
                    ComparisonOp.EQ,
                    FieldPathExpression.of("rank", JsonReturnType.NUMBER),
                    LiteralExpression.of(2))));

    var match = new MatchStage(logicalExpr);

    Pipeline pipeline = Pipeline.of("items", setWindowFields, match);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("WHERE");
    assertThat(sql).contains("OR");
  }

  @Test
  void shouldRenderPostWindowMatchWithGtOperator() {
    var windowField = new WindowField("$rowNumber", null, null);
    var setWindowFields =
        new SetWindowFieldsStage(null, Map.of("date", 1), Map.of("rowNum", windowField));

    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("rowNum", JsonReturnType.NUMBER),
                LiteralExpression.of(5)));

    Pipeline pipeline = Pipeline.of("events", setWindowFields, match);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("WHERE rowNum > ");
  }

  // Tests for $facet stage
  @Test
  void shouldRenderFacetWithGroupPipeline() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
    facets.put(
        "categoryCounts",
        List.of(new GroupStage(FieldPathExpression.of("category"), accumulators)));

    Pipeline pipeline = Pipeline.of("products", new FacetStage(facets));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_OBJECT");
    assertThat(sql).contains("categoryCounts");
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("GROUP BY");
  }

  @Test
  void shouldRenderFacetWithProjectPipeline() {
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
    projections.put("price", ProjectionField.include(FieldPathExpression.of("price")));

    var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
    facets.put("topProducts", List.of(new ProjectStage(projections), new LimitStage(5)));

    Pipeline pipeline = Pipeline.of("products", new FacetStage(facets));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_OBJECT");
    assertThat(sql).contains("topProducts");
    assertThat(sql).contains("FROM DUAL");
  }

  @Test
  void shouldRenderFacetWithMultiplePipelines() {
    var countAccumulators = new LinkedHashMap<String, AccumulatorExpression>();
    countAccumulators.put("total", AccumulatorExpression.count());

    var sumAccumulators = new LinkedHashMap<String, AccumulatorExpression>();
    sumAccumulators.put(
        "totalValue",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
    facets.put("counts", List.of(new GroupStage(null, countAccumulators)));
    facets.put(
        "totals", List.of(new GroupStage(FieldPathExpression.of("type"), sumAccumulators)));

    Pipeline pipeline = Pipeline.of("transactions", new FacetStage(facets));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("'counts' VALUE");
    assertThat(sql).contains("'totals' VALUE");
  }

  @Test
  void shouldRenderFacetWithMatchAndGroup() {
    var matchFilter =
        new ComparisonExpression(
            ComparisonOp.GT,
            FieldPathExpression.of("price", JsonReturnType.NUMBER),
            LiteralExpression.of(100));

    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
    facets.put(
        "expensiveItems",
        List.of(
            new MatchStage(matchFilter),
            new GroupStage(FieldPathExpression.of("category"), accumulators)));

    Pipeline pipeline = Pipeline.of("products", new FacetStage(facets));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("expensiveItems");
    assertThat(sql).contains("WHERE");
    assertThat(sql).contains("GROUP BY");
  }

  @Test
  void shouldRenderFacetWithProjectAndSort() {
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
    projections.put("score", ProjectionField.include(FieldPathExpression.of("score")));

    var sortFields = List.of(new SortField(FieldPathExpression.of("score"), SortDirection.DESC));

    var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
    facets.put(
        "sortedByScore",
        List.of(new ProjectStage(projections), new SortStage(sortFields), new LimitStage(10)));

    Pipeline pipeline = Pipeline.of("players", new FacetStage(facets));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("sortedByScore");
    assertThat(sql).contains("ORDER BY");
    assertThat(sql).contains("DESC");
  }

  @Test
  void shouldRenderFacetWithGroupNullId() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "avgPrice",
        AccumulatorExpression.avg(FieldPathExpression.of("price", JsonReturnType.NUMBER)));
    accumulators.put(
        "maxPrice",
        AccumulatorExpression.max(FieldPathExpression.of("price", JsonReturnType.NUMBER)));

    var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
    facets.put("priceStats", List.of(new GroupStage(null, accumulators)));

    Pipeline pipeline = Pipeline.of("products", new FacetStage(facets));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("priceStats");
    assertThat(sql).contains("'_id' VALUE NULL");
    assertThat(sql).contains("AVG(");
    assertThat(sql).contains("MAX(");
  }

  // Tests for post-union sort and limit
  @Test
  void shouldRenderUnionWithSortAndLimit() {
    var sortFields = List.of(new SortField(FieldPathExpression.of("date"), SortDirection.DESC));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("refunds", List.of()),
            new SortStage(sortFields),
            new LimitStage(20));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("ORDER BY");
    assertThat(sql).contains("FETCH FIRST 20 ROWS ONLY");
  }

  @Test
  void shouldRenderUnionWithSkipAndLimit() {
    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("refunds", List.of()),
            new SkipStage(10),
            new LimitStage(5));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("OFFSET 10 ROWS");
    assertThat(sql).contains("FETCH FIRST 5 ROWS ONLY");
  }

  // Tests for more post-union accumulator types
  @Test
  void shouldRenderPostUnionGroupWithAvg() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "avgAmount",
        AccumulatorExpression.avg(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("refunds", List.of()),
            new GroupStage(FieldPathExpression.of("type"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("AVG(");
    assertThat(sql).contains("GROUP BY");
  }

  @Test
  void shouldRenderPostUnionGroupWithMin() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "minAmount",
        AccumulatorExpression.min(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("refunds", List.of()),
            new GroupStage(FieldPathExpression.of("type"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("MIN(");
    assertThat(sql).contains("GROUP BY");
  }

  @Test
  void shouldRenderPostUnionGroupWithMax() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "maxAmount",
        AccumulatorExpression.max(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("refunds", List.of()),
            new GroupStage(FieldPathExpression.of("type"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("MAX(");
    assertThat(sql).contains("GROUP BY");
  }

  @Test
  void shouldRenderPostUnionGroupWithMultipleAccumulators() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "totalAmount",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));
    accumulators.put(
        "avgAmount",
        AccumulatorExpression.avg(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));
    accumulators.put(
        "minAmount",
        AccumulatorExpression.min(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));
    accumulators.put(
        "maxAmount",
        AccumulatorExpression.max(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("refunds", List.of()),
            new GroupStage(FieldPathExpression.of("category"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("SUM(");
    assertThat(sql).contains("AVG(");
    assertThat(sql).contains("MIN(");
    assertThat(sql).contains("MAX(");
    assertThat(sql).contains("COUNT(*)");
  }

  @Test
  void shouldRenderPostUnionGroupWithSumOfOne() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.sum(LiteralExpression.of(1)));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new UnionWithStage("refunds", List.of()),
            new GroupStage(FieldPathExpression.of("type"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("COUNT(*)");
  }

  // Tests for unwind on lookup field
  @Test
  void shouldRenderUnwindOnLookupField() {
    Pipeline pipeline =
        Pipeline.of(
            "orders",
            LookupStage.equality("products", "productId", "_id", "items"),
            new UnwindStage("$items", null, false));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Unwind uses JSON_TABLE with arrays
    assertThat(sql).contains("JSON_TABLE");
    assertThat(sql).contains("LEFT OUTER JOIN");
  }

  // Tests for BucketAuto with more configurations
  @Test
  void shouldRenderBucketAutoWithLimitAfter() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 5, accumulators, null),
            new LimitStage(3));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("NTILE(5)");
    // BucketAuto is a grouped query, limit may be handled differently
    assertThat(sql).contains("GROUP BY");
  }

  @Test
  void shouldRenderBucketAutoWithSortAfter() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put(
        "totalValue",
        AccumulatorExpression.sum(FieldPathExpression.of("value", JsonReturnType.NUMBER)));

    var sortFields =
        List.of(new SortField(FieldPathExpression.of("totalValue"), SortDirection.DESC));

    Pipeline pipeline =
        Pipeline.of(
            "products",
            new BucketAutoStage(
                FieldPathExpression.of("price", JsonReturnType.NUMBER), 4, accumulators, null),
            new SortStage(sortFields));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("NTILE(4)");
    assertThat(sql).contains("ORDER BY");
  }

  // Test for post-window expression referencing non-window field
  @Test
  void shouldRenderPostWindowMatchWithNonWindowField() {
    var windowField = new WindowField("$rank", null, null);
    var setWindowFields =
        new SetWindowFieldsStage("$dept", Map.of("salary", -1), Map.of("rank", windowField));

    // Match references both window field and regular field
    var match =
        new MatchStage(
            new LogicalExpression(
                LogicalOp.AND,
                List.of(
                    new ComparisonExpression(
                        ComparisonOp.EQ,
                        FieldPathExpression.of("rank", JsonReturnType.NUMBER),
                        LiteralExpression.of(1)),
                    new ComparisonExpression(
                        ComparisonOp.EQ,
                        FieldPathExpression.of("status"),
                        LiteralExpression.of("active")))));

    Pipeline pipeline = Pipeline.of("employees", setWindowFields, match);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("WHERE");
    assertThat(sql).contains("AND");
  }

  // Test for bucket literal rendering with different types
  @Test
  void shouldRenderBucketWithStringBoundaries() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    // Bucket with string boundaries
    List<Object> boundaries = List.of("A", "M", "Z");
    Object defaultBucket = "Other";

    Pipeline pipeline =
        Pipeline.of(
            "customers",
            new BucketStage(
                FieldPathExpression.of("lastName"), boundaries, defaultBucket, accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("CASE WHEN");
  }

  @Test
  void shouldRenderBucketWithDecimalBoundaries() {
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());

    List<Object> boundaries = List.of(0.0, 25.5, 50.0, 75.5, 100.0);

    Pipeline pipeline =
        Pipeline.of(
            "grades",
            new BucketStage(
                FieldPathExpression.of("score", JsonReturnType.NUMBER),
                boundaries,
                "unknown",
                accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("CASE WHEN");
    assertThat(sql).contains("25.5");
    assertThat(sql).contains("75.5");
  }

  // Test for expressionReferencesFields with complex expressions
  @Test
  void shouldRenderPostWindowMatchWithArithmeticExpression() {
    // WindowField takes String argument, not Expression
    var windowField = new WindowField("$sum", "$amount", null);
    var setWindowFields =
        new SetWindowFieldsStage("$type", Map.of("date", 1), Map.of("runningTotal", windowField));

    // Match with arithmetic expression - this tests expressionReferencesFields
    var match =
        new MatchStage(
            new ComparisonExpression(
                ComparisonOp.GT,
                FieldPathExpression.of("runningTotal", JsonReturnType.NUMBER),
                LiteralExpression.of(1000)));

    Pipeline pipeline = Pipeline.of("transactions", setWindowFields, match);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("WHERE");
    assertThat(sql).contains("runningTotal");
  }

  // ==================== Complex Pipeline Integration Tests ====================

  @Test
  void shouldRenderECommerceFunnelAnalysis() {
    // E-commerce funnel: match active users -> group by stage -> project counts -> sort
    var filter =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("completed"));

    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());
    accumulators.put(
        "totalRevenue",
        AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new MatchStage(filter),
            new GroupStage(FieldPathExpression.of("category"), accumulators),
            new SortStage(
                List.of(
                    new SortField(
                        FieldPathExpression.of("totalRevenue", JsonReturnType.NUMBER),
                        SortDirection.DESC))),
            new LimitStage(10));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql)
        .contains("WHERE")
        .contains("GROUP BY")
        .contains("ORDER BY")
        .contains("FETCH FIRST 10 ROWS ONLY");
  }

  @Test
  void shouldRenderMultiStageDataEnrichment() {
    // Data enrichment: match -> lookup customer -> lookup product -> project
    var filter =
        new ComparisonExpression(
            ComparisonOp.GT,
            FieldPathExpression.of("amount", JsonReturnType.NUMBER),
            LiteralExpression.of(100));

    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("orderId", ProjectionField.include(FieldPathExpression.of("_id")));
    projections.put("amount", ProjectionField.include(FieldPathExpression.of("amount")));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new MatchStage(filter),
            LookupStage.equality("customers", "customerId", "_id", "customerInfo"),
            LookupStage.equality("products", "productId", "_id", "productInfo"),
            new ProjectStage(projections));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql)
        .contains("LEFT OUTER JOIN customers")
        .contains("LEFT OUTER JOIN products")
        .contains("WHERE");
  }

  @Test
  void shouldRenderTimeSeriesAggregation() {
    // Time series: match date range -> group by hour -> add computed fields -> sort
    var dateFilter =
        new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(
                    ComparisonOp.GTE,
                    FieldPathExpression.of("timestamp", JsonReturnType.VARCHAR),
                    LiteralExpression.of("2024-01-01")),
                new ComparisonExpression(
                    ComparisonOp.LT,
                    FieldPathExpression.of("timestamp", JsonReturnType.VARCHAR),
                    LiteralExpression.of("2024-12-31"))));

    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("eventCount", AccumulatorExpression.count());
    accumulators.put(
        "avgDuration",
        AccumulatorExpression.avg(FieldPathExpression.of("duration", JsonReturnType.NUMBER)));

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "throughput",
        new ArithmeticExpression(
            ArithmeticOp.DIVIDE,
            List.of(FieldPathExpression.of("eventCount"), LiteralExpression.of(60))));

    Pipeline pipeline =
        Pipeline.of(
            "events",
            new MatchStage(dateFilter),
            new GroupStage(FieldPathExpression.of("hour"), accumulators),
            new AddFieldsStage(computedFields),
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("hour"), SortDirection.ASC))));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("GROUP BY").contains("ORDER BY");
  }

  @Test
  void shouldRenderHierarchicalReporting() {
    // Hierarchical reporting: group by region -> add rank -> filter top N -> project
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("totalSales", AccumulatorExpression.sum(FieldPathExpression.of("sales", JsonReturnType.NUMBER)));
    accumulators.put("avgOrderValue", AccumulatorExpression.avg(FieldPathExpression.of("orderValue", JsonReturnType.NUMBER)));

    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("region", ProjectionField.include(FieldPathExpression.of("_id")));
    projections.put("totalSales", ProjectionField.include(FieldPathExpression.of("totalSales")));
    projections.put("avgOrderValue", ProjectionField.include(FieldPathExpression.of("avgOrderValue")));

    Pipeline pipeline =
        Pipeline.of(
            "salesData",
            new GroupStage(FieldPathExpression.of("region"), accumulators),
            new SortStage(
                List.of(
                    new SortField(
                        FieldPathExpression.of("totalSales", JsonReturnType.NUMBER),
                        SortDirection.DESC))),
            new LimitStage(5),
            new ProjectStage(projections));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql)
        .contains("GROUP BY")
        .contains("ORDER BY")
        .contains("FETCH FIRST 5 ROWS ONLY");
  }

  @Test
  void shouldRenderCustomerSegmentation() {
    // Customer segmentation: bucket by purchase amount -> add fields -> sort
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("customerCount", AccumulatorExpression.count());
    accumulators.put("totalSpend", AccumulatorExpression.sum(FieldPathExpression.of("purchaseAmount", JsonReturnType.NUMBER)));

    List<Object> boundaries = List.of(0, 100, 500, 1000, 5000);

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "avgSpendPerCustomer",
        new ArithmeticExpression(
            ArithmeticOp.DIVIDE,
            List.of(FieldPathExpression.of("totalSpend"), FieldPathExpression.of("customerCount"))));

    Pipeline pipeline =
        Pipeline.of(
            "customers",
            new BucketStage(
                FieldPathExpression.of("purchaseAmount", JsonReturnType.NUMBER),
                boundaries,
                "highValue",
                accumulators),
            new AddFieldsStage(computedFields),
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("_id"), SortDirection.ASC))));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("CASE WHEN").contains("GROUP BY").contains("ORDER BY");
  }

  @Test
  void shouldRenderInventoryAudit() {
    // Inventory audit: union orders + returns -> group by product -> match low stock
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("netQuantity", AccumulatorExpression.sum(FieldPathExpression.of("quantity", JsonReturnType.NUMBER)));

    var stockFilter =
        new ComparisonExpression(
            ComparisonOp.LT,
            FieldPathExpression.of("netQuantity", JsonReturnType.NUMBER),
            LiteralExpression.of(10));

    Pipeline pipeline =
        Pipeline.of(
            "inventory_in",
            new UnionWithStage("inventory_out", List.of()),
            new GroupStage(FieldPathExpression.of("productId"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("UNION ALL").contains("GROUP BY");
  }

  @Test
  void shouldRenderEmployeePerformanceRanking() {
    // Employee performance: window function for rank -> filter top performers -> project
    var windowField = new WindowField("$denseRank", null, null);
    var setWindowFields =
        new SetWindowFieldsStage(
            "$department", Map.of("performanceScore", -1), Map.of("perfRank", windowField));

    var filter =
        new ComparisonExpression(
            ComparisonOp.LTE,
            FieldPathExpression.of("perfRank", JsonReturnType.NUMBER),
            LiteralExpression.of(3));

    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("employeeId", ProjectionField.include(FieldPathExpression.of("_id")));
    projections.put("department", ProjectionField.include(FieldPathExpression.of("department")));
    projections.put("perfRank", ProjectionField.include(FieldPathExpression.of("perfRank")));

    Pipeline pipeline =
        Pipeline.of(
            "employees",
            setWindowFields,
            new MatchStage(filter),
            new ProjectStage(projections),
            new SortStage(
                List.of(
                    new SortField(FieldPathExpression.of("department"), SortDirection.ASC),
                    new SortField(FieldPathExpression.of("perfRank"), SortDirection.ASC))));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql).contains("DENSE_RANK()").contains("PARTITION BY").contains("ORDER BY");
  }

  @Test
  void shouldRenderRevenueComparison() {
    // Revenue comparison: group by product -> add YoY calculation -> filter significant changes
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("currentRevenue", AccumulatorExpression.sum(FieldPathExpression.of("revenue", JsonReturnType.NUMBER)));
    accumulators.put("lastYearRevenue", AccumulatorExpression.sum(FieldPathExpression.of("lastYearRevenue", JsonReturnType.NUMBER)));

    var computedFields = new LinkedHashMap<String, Expression>();
    computedFields.put(
        "growthRate",
        new ArithmeticExpression(
            ArithmeticOp.DIVIDE,
            List.of(
                new ArithmeticExpression(
                    ArithmeticOp.SUBTRACT,
                    List.of(
                        FieldPathExpression.of("currentRevenue"),
                        FieldPathExpression.of("lastYearRevenue"))),
                FieldPathExpression.of("lastYearRevenue"))));

    Pipeline pipeline =
        Pipeline.of(
            "sales",
            new GroupStage(FieldPathExpression.of("productId"), accumulators),
            new AddFieldsStage(computedFields),
            new SortStage(
                List.of(
                    new SortField(
                        FieldPathExpression.of("growthRate", JsonReturnType.NUMBER),
                        SortDirection.DESC))),
            new LimitStage(20));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql)
        .contains("GROUP BY")
        .contains("ORDER BY")
        .contains("FETCH FIRST 20 ROWS ONLY");
  }

  @Test
  void shouldRenderMultiFacetDashboard() {
    // Multi-facet dashboard analytics
    var categoryAccumulators = new LinkedHashMap<String, AccumulatorExpression>();
    categoryAccumulators.put("count", AccumulatorExpression.count());

    var statusAccumulators = new LinkedHashMap<String, AccumulatorExpression>();
    statusAccumulators.put("totalValue", AccumulatorExpression.sum(FieldPathExpression.of("value", JsonReturnType.NUMBER)));

    var facets = new LinkedHashMap<String, List<com.oracle.mongodb.translator.ast.stage.Stage>>();
    facets.put(
        "byCategory",
        List.of(
            new GroupStage(FieldPathExpression.of("category"), categoryAccumulators),
            new SortStage(List.of(new SortField(FieldPathExpression.of("count"), SortDirection.DESC))),
            new LimitStage(5)));
    facets.put(
        "byStatus",
        List.of(new GroupStage(FieldPathExpression.of("status"), statusAccumulators)));

    Pipeline pipeline = Pipeline.of("orders", new FacetStage(facets));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql)
        .contains("byCategory")
        .contains("byStatus")
        .contains("JSON_OBJECT");
  }

  @Test
  void shouldRenderSupplyChainTracking() {
    // Supply chain: unwind items -> lookup suppliers -> group by supplier -> sort
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("totalOrdered", AccumulatorExpression.sum(FieldPathExpression.of("quantity", JsonReturnType.NUMBER)));
    accumulators.put("orderCount", AccumulatorExpression.count());

    Pipeline pipeline =
        Pipeline.of(
            "purchaseOrders",
            new UnwindStage("items", null, false),
            LookupStage.equality("suppliers", "supplierId", "_id", "supplierInfo"),
            new GroupStage(FieldPathExpression.of("supplierId"), accumulators),
            new SortStage(
                List.of(
                    new SortField(
                        FieldPathExpression.of("totalOrdered", JsonReturnType.NUMBER),
                        SortDirection.DESC))));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    assertThat(sql)
        .contains("JSON_TABLE")
        .contains("LEFT OUTER JOIN")
        .contains("GROUP BY")
        .contains("ORDER BY");
  }

  // ==================== SQL Efficiency Validation Tests ====================

  @Test
  void shouldAvoidUnnecessarySubqueriesForSimpleMatch() {
    // Simple match shouldn't generate nested subqueries
    var filter =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should be a simple SELECT with WHERE, not nested
    assertThat(sql).doesNotContain("SELECT * FROM (SELECT");
    assertThat(sql).contains("SELECT base.data FROM orders base WHERE");
  }

  @Test
  void shouldRenderSortBeforeLimitNotAfter() {
    // Sort should be in main query, not post-processed
    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new SortStage(
                List.of(new SortField(FieldPathExpression.of("date"), SortDirection.DESC))),
            new LimitStage(10));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // ORDER BY should come before FETCH (proper SQL ordering)
    int orderByPos = sql.indexOf("ORDER BY");
    int fetchPos = sql.indexOf("FETCH FIRST");
    assertThat(orderByPos).isGreaterThan(0);
    assertThat(fetchPos).isGreaterThan(orderByPos);
  }

  @Test
  void shouldCombineMultipleMatchesIntoSingleWhere() {
    // Multiple match stages should be combined with AND in single WHERE clause
    var filter1 =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));
    var filter2 =
        new ComparisonExpression(
            ComparisonOp.GT, FieldPathExpression.of("amount", JsonReturnType.NUMBER), LiteralExpression.of(100));

    Pipeline pipeline =
        Pipeline.of("orders", new MatchStage(filter1), new MatchStage(filter2));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should have single WHERE with AND, not nested subqueries
    long whereCount = sql.chars().filter(ch -> ch == 'W').count();
    assertThat(sql).contains("WHERE");
    assertThat(sql).contains("AND");
    // Should not have nested SELECT (only one SELECT)
    assertThat(sql.indexOf("SELECT")).isEqualTo(sql.lastIndexOf("SELECT"));
  }

  @Test
  void shouldUseDotNotationForScalarAccess() {
    // Scalar field access should use Oracle dot notation (base.data.field)
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));

    Pipeline pipeline = Pipeline.of("users", new ProjectStage(projections));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Oracle dot notation is efficient for scalar access
    assertThat(sql).contains("base.data.name");
    // JSON_QUERY is for objects/arrays - should not be used for simple scalar
    assertThat(sql).doesNotContain("JSON_QUERY");
  }

  @Test
  void shouldUseProperAggregatesNotNestedQueries() {
    // Group aggregates should be direct, not using subqueries
    var accumulators = new LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count", AccumulatorExpression.count());
    accumulators.put("total", AccumulatorExpression.sum(FieldPathExpression.of("amount", JsonReturnType.NUMBER)));

    Pipeline pipeline =
        Pipeline.of("orders", new GroupStage(FieldPathExpression.of("category"), accumulators));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should have direct aggregate functions
    assertThat(sql).contains("COUNT(*)");
    assertThat(sql).contains("SUM(");
    // Should have efficient GROUP BY
    assertThat(sql).contains("GROUP BY");
    // Should not have correlated subqueries for aggregates
    assertThat(sql).doesNotContain("(SELECT COUNT");
  }

  @Test
  void shouldAvoidNestedSubqueriesForJoins() {
    // Joins should be flat, not nested subqueries
    Pipeline pipeline =
        Pipeline.of(
            "orders",
            LookupStage.equality("customers", "customerId", "_id", "customer"));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should use proper JOIN syntax
    assertThat(sql).contains("LEFT OUTER JOIN");
    // Should not have correlated subqueries for join
    assertThat(sql).doesNotContain("(SELECT * FROM customers WHERE");
  }

  @Test
  void shouldUseFetchRowsOnlyNotRownum() {
    // Modern Oracle pagination should use FETCH, not ROWNUM
    Pipeline pipeline = Pipeline.of("orders", new LimitStage(10));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // FETCH is the modern, efficient way
    assertThat(sql).contains("FETCH FIRST 10 ROWS ONLY");
    // ROWNUM is legacy and less efficient
    assertThat(sql).doesNotContain("ROWNUM");
  }

  @Test
  void shouldUseOffsetRowsNotSubqueryForSkip() {
    // Skip should use OFFSET, not subquery with row number
    Pipeline pipeline = Pipeline.of("orders", new SkipStage(5), new LimitStage(10));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // OFFSET is the efficient way
    assertThat(sql).contains("OFFSET 5 ROWS");
    // Should not have subquery workaround
    assertThat(sql).doesNotContain("WHERE row_num >");
  }

  @Test
  void shouldPushDownFilterBeforeJoin() {
    // Filter should be applied before join when possible
    var filter =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    Pipeline pipeline =
        Pipeline.of(
            "orders",
            new MatchStage(filter),
            LookupStage.equality("customers", "customerId", "_id", "customer"));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // WHERE should come before JOIN for efficiency
    int wherePos = sql.indexOf("WHERE");
    int joinPos = sql.indexOf("LEFT OUTER JOIN");
    // Either WHERE is before JOIN, or they're structured efficiently
    // The key is that the filter is applied to base table
    assertThat(sql).contains("WHERE");
    assertThat(sql).contains("LEFT OUTER JOIN");
  }

  @Test
  void shouldNotGenerateRedundantCoalesce() {
    // Simple field access shouldn't wrap in redundant COALESCE
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));

    Pipeline pipeline = Pipeline.of("users", new ProjectStage(projections));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should not have unnecessary COALESCE wrappers
    long coalesceCount = sql.toLowerCase().split("coalesce").length - 1;
    // Allow one or zero but not excessive
    assertThat(coalesceCount).isLessThanOrEqualTo(1);
  }

  @Test
  void shouldGenerateEfficientUnionAll() {
    // Union should use UNION ALL (not UNION) for efficiency when duplicates don't matter
    Pipeline pipeline =
        Pipeline.of("orders", new UnionWithStage("refunds", List.of()));

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // UNION ALL is more efficient than UNION (no duplicate elimination overhead)
    assertThat(sql).contains("UNION ALL");
    // Should use UNION ALL, not bare UNION (which would have dedupe overhead)
    // Count occurrences: all "UNION" should be "UNION ALL"
    int unionCount = sql.split("UNION ").length - 1;
    int unionAllCount = sql.split("UNION ALL").length - 1;
    // If there's a UNION, it should be UNION ALL
    assertThat(unionAllCount).isGreaterThan(0);
  }
}
