/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.AccumulatorOp;
import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticOp;
import com.oracle.mongodb.translator.ast.expression.ArrayExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.CompoundIdExpression;
import com.oracle.mongodb.translator.ast.expression.ConditionalExpression;
import com.oracle.mongodb.translator.ast.expression.DateExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.AddFieldsStage;
import com.oracle.mongodb.translator.ast.stage.BucketAutoStage;
import com.oracle.mongodb.translator.ast.stage.BucketStage;
import com.oracle.mongodb.translator.ast.stage.CountStage;
import com.oracle.mongodb.translator.ast.stage.FacetStage;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.RedactStage;
import com.oracle.mongodb.translator.ast.stage.SampleStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.ast.stage.UnionWithStage;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests for CTE-based SQL generation following Oracle MongoDB API patterns.
 *
 * <p>These tests define the target SQL output format using:
 * <ul>
 *   <li>CTE (WITH clause) structure where each stage becomes a CTE</li>
 *   <li>JSON_EXISTS with type-safe predicates (stringOnly, numberOnly, etc.)</li>
 *   <li>json_transform for projections (KEEP, SET, REMOVE)</li>
 *   <li>DATA column preservation (returns JSON, not extracted columns)</li>
 * </ul>
 *
 * <p>Reference: Oracle MongoDB API generated SQL pattern:
 * <pre>
 * WITH
 * "Q1" ("ID", "DATA") AS (SELECT "RESID", "DATA" FROM "collection"),
 * "Q2" ("DATA") AS (
 *   SELECT "DATA" FROM "Q1" q
 *   WHERE JSON_EXISTS("DATA", '$?(@.field.stringOnly() == $B0)'
 *         PASSING ? AS "B0" TYPE(strict))
 * )
 * SELECT "DATA" FROM "Q2"
 * </pre>
 */
class CteBasedPipelineRendererTest {

  private PipelineRenderer renderer;
  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    OracleConfiguration config = OracleConfiguration.builder()
        .collectionName("orders")
        .useCteBasedRendering(true)
        .build();
    renderer = new PipelineRenderer(config);
    context = new DefaultSqlGenerationContext(false, null, null);
  }

  // =========================================================================
  // Phase 1: CTE Infrastructure Tests
  // =========================================================================

  @Test
  void shouldRenderEmptyPipelineAsCte() {
    Pipeline pipeline = Pipeline.of("orders");

    renderer.render(pipeline, context);

    // Empty pipeline should still use CTE structure
    assertThat(context.toSql()).isEqualTo(
        "WITH \"Q1\" (\"ID\", \"DATA\") AS ("
            + "SELECT \"ID\", \"DATA\" FROM \"ORDERS\""
            + ") SELECT JSON_ARRAYAGG(\"DATA\" RETURNING CLOB) FROM \"Q1\"");
  }

  @Test
  // @Disabled("Phase 1: CTE infrastructure not yet implemented")
  void shouldRenderSimpleMatchAsCte() {
    var filter = new ComparisonExpression(
        ComparisonOp.EQ,
        FieldPathExpression.of("status"),
        LiteralExpression.of("active"));
    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

    renderer.render(pipeline, context);

    // Match should create Q1 (base) and Q2 (filter) CTEs
    String expectedSql =
        "WITH \"Q1\" (\"ID\", \"DATA\") AS ("
            + "SELECT \"ID\", \"DATA\" FROM \"ORDERS\""
            + "), \"Q2\" (\"DATA\") AS ("
            + "SELECT \"DATA\" FROM \"Q1\" q "
            + "WHERE JSON_EXISTS(\"DATA\", '$?(@.status.stringOnly() == $B0)' "
            + "PASSING :1 AS \"B0\" TYPE(strict))"
            + ") SELECT JSON_ARRAYAGG(\"DATA\" RETURNING CLOB) FROM \"Q2\"";

    assertThat(context.toSql()).isEqualTo(expectedSql);
    assertThat(context.getBindVariables()).containsExactly("active");
  }

  // =========================================================================
  // Phase 2: Type-Safe Predicates Tests
  // =========================================================================

  @Test
  void shouldInferStringTypeFromBindVariable() {
    // When bind value is a String, use stringOnly()
    var filter = new ComparisonExpression(
        ComparisonOp.EQ,
        FieldPathExpression.of("status"),
        LiteralExpression.of("active")); // String value
    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("@.status.stringOnly() == $B0");
  }

  @Test
  void shouldInferNumberTypeFromBindVariable() {
    // When bind value is a Number, use numberOnly()
    var filter = new ComparisonExpression(
        ComparisonOp.GT,
        FieldPathExpression.of("amount"),
        LiteralExpression.of(100)); // Integer value
    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("@.amount.numberOnly() > $B0");
  }

  @Test
  void shouldInferBooleanTypeFromBindVariable() {
    // When bind value is a Boolean, use booleanOnly()
    var filter = new ComparisonExpression(
        ComparisonOp.EQ,
        FieldPathExpression.of("active"),
        LiteralExpression.of(true)); // Boolean value
    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

    renderer.render(pipeline, context);

    assertThat(context.toSql()).contains("@.active.booleanOnly() == $B0");
  }

  @Test
  void shouldHandleNestedArrayPredicateForMviPickup() {
    // Nested array predicates should use JSON_EXISTS filter syntax for MVI support
    var filter = new ComparisonExpression(
        ComparisonOp.EQ,
        FieldPathExpression.of("items.product"),
        LiteralExpression.of("Widget"));
    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

    renderer.render(pipeline, context);

    // Should produce MVI-compatible JSON_EXISTS
    assertThat(context.toSql())
        .contains("$.items[*]?(@.product.stringOnly() == $B0)");
  }

  @Test
  void shouldRenderInOperatorWithJsonExists() {
    // $in should use JSON_EXISTS with 'in' predicate
    var filter = new ComparisonExpression(
        ComparisonOp.IN,
        FieldPathExpression.of("status"),
        LiteralExpression.of(java.util.List.of("pending", "active", "completed")));
    Pipeline pipeline = Pipeline.of("orders", new MatchStage(filter));

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .contains("@.status.stringOnly() in ($B0, $B1, $B2)");
  }

  // =========================================================================
  // Phase 3: json_transform Projection Tests
  // =========================================================================

  @Test
  void shouldRenderSimpleProjectionWithJsonTransformKeep() {
    // Simple inclusion projection should use json_transform KEEP
    var projections =
        new java.util.LinkedHashMap<String, ProjectStage.ProjectionField>();
    projections.put("_id",
        ProjectStage.ProjectionField.include(FieldPathExpression.of("_id")));
    projections.put("orderId",
        ProjectStage.ProjectionField.include(FieldPathExpression.of("orderId")));
    projections.put("status",
        ProjectStage.ProjectionField.include(FieldPathExpression.of("status")));

    var projectStage = new ProjectStage(projections);
    Pipeline pipeline = Pipeline.of("orders", projectStage);

    renderer.render(pipeline, context);

    assertThat(context.toSql())
        .contains("json_transform")
        .contains("KEEP")
        .contains("'$.\"_id\"'")
        .contains("'$.\"orderId\"'")
        .contains("'$.\"status\"'");
  }

  // =========================================================================
  // Combined Pipeline Tests
  // =========================================================================

  @Test
  void shouldRenderMatchAndProjectAsCteChain() {
    // MongoDB: [{$match: {status: "active"}}, {$project: {_id: 1, orderId: 1}}]
    var filter = new ComparisonExpression(
        ComparisonOp.EQ,
        FieldPathExpression.of("status"),
        LiteralExpression.of("active"));

    var projections =
        new java.util.LinkedHashMap<String, ProjectStage.ProjectionField>();
    projections.put("_id",
        ProjectStage.ProjectionField.include(FieldPathExpression.of("_id")));
    projections.put("orderId",
        ProjectStage.ProjectionField.include(FieldPathExpression.of("orderId")));

    var matchStage = new MatchStage(filter);
    var projectStage = new ProjectStage(projections);
    Pipeline pipeline = Pipeline.of("orders", matchStage, projectStage);

    renderer.render(pipeline, context);

    // Should produce 3 CTEs: Q1 (base), Q2 (match), Q3 (project)
    String expectedPattern =
        "WITH \"Q1\" (\"ID\", \"DATA\") AS ("
            + "SELECT \"ID\", \"DATA\" FROM \"ORDERS\""
            + "), \"Q2\" (\"DATA\") AS ("
            + "SELECT \"DATA\" FROM \"Q1\" q "
            + "WHERE JSON_EXISTS(\"DATA\", '$?(@.status.stringOnly() == $B0)' "
            + "PASSING :1 AS \"B0\" TYPE(strict))"
            + "), \"Q3\" (\"DATA\") AS ("
            + "SELECT json_transform(\"DATA\", KEEP '$.\"_id\"', '$.\"orderId\"') "
            + "FROM \"Q2\" q"
            + ") SELECT JSON_ARRAYAGG(\"DATA\" RETURNING CLOB) FROM \"Q3\"";

    assertThat(context.toSql()).isEqualTo(expectedPattern);
  }

  @Test
  void shouldRenderMatchWithNullComparisonAsCte() {
    // MongoDB: [{$match: {"metadata.campaign": null}}]
    // This tests NULL003 - matching field with null value
    var filter = new ComparisonExpression(
        ComparisonOp.EQ,
        FieldPathExpression.of("metadata.campaign"),
        LiteralExpression.ofNull());

    var matchStage = new MatchStage(filter);
    Pipeline pipeline = Pipeline.of("sales", matchStage);

    renderer.render(pipeline, context);

    // Should handle null comparison specially - Oracle JSON path can use null directly
    // Expected pattern: JSON_EXISTS("DATA", '$?(@.metadata.campaign == null)' TYPE(strict))
    String sql = context.toSql();
    assertThat(sql)
        .describedAs("Should not throw NullPointerException and should generate valid SQL")
        .contains("\"Q2\"")
        .contains("SELECT \"DATA\" FROM \"Q1\"")
        .contains("WHERE");
    // The null comparison should be rendered as == null in JSON path syntax
    System.out.println("Generated SQL: " + sql);
  }

  // =========================================================================
  // Stage Type Tests - $group
  // =========================================================================

  @Test
  void shouldRenderGroupStageAsCte() {
    // MongoDB: [{$group: {_id: "$status", total: {$sum: "$amount"}}}]
    var idExpr = FieldPathExpression.of("status");
    var accumulators = new java.util.LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("total",
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("amount")));

    var groupStage = new GroupStage(idExpr, accumulators);
    Pipeline pipeline = Pipeline.of("orders", groupStage);

    renderer.render(pipeline, context);

    // Should produce CTE with JSON_OBJECT for result construction
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("JSON_OBJECT")
        .contains("'_id'")
        .contains("'total'")
        .contains("SUM")
        .contains("GROUP BY");
  }

  @Test
  void shouldRenderGroupWithDateExpressionAsCte() {
    // MongoDB: [{$group: {_id: {$month: "$eventDate"}, eventCount: {$sum: 1}}}]
    var idExpr = DateExpression.month(FieldPathExpression.of("eventDate"));
    var accumulators = new java.util.LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("eventCount",
        new AccumulatorExpression(AccumulatorOp.SUM, LiteralExpression.of(1)));

    var groupStage = new GroupStage(idExpr, accumulators);
    Pipeline pipeline = Pipeline.of("events", groupStage);

    renderer.render(pipeline, context);

    // Should produce CTE with EXTRACT(MONTH FROM ...) for date expression
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("JSON_OBJECT")
        .contains("'_id'")
        .contains("EXTRACT(MONTH FROM")
        .contains("'eventCount'")
        .contains("SUM")
        .contains("GROUP BY");
  }

  @Test
  void shouldRenderGroupWithMinMaxUsingReturningNumber() {
    // MongoDB: [{$group: {_id: "$category", minPrice: {$min: "$price"},
    //           maxPrice: {$max: "$price"}}}]
    // MIN/MAX on numeric fields should use RETURNING NUMBER for proper
    // numeric comparison and output
    var idExpr = FieldPathExpression.of("category");
    var accumulators = new java.util.LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("minPrice",
        new AccumulatorExpression(AccumulatorOp.MIN, FieldPathExpression.of("price")));
    accumulators.put("maxPrice",
        new AccumulatorExpression(AccumulatorOp.MAX, FieldPathExpression.of("price")));

    var groupStage = new GroupStage(idExpr, accumulators);
    Pipeline pipeline = Pipeline.of("products", groupStage);

    renderer.render(pipeline, context);

    // Should use RETURNING NUMBER for MIN/MAX to ensure proper numeric
    // comparison and type preservation
    String sql = context.toSql();
    assertThat(sql)
        .contains("MIN(JSON_VALUE(\"DATA\", '$.price' RETURNING NUMBER))")
        .contains("MAX(JSON_VALUE(\"DATA\", '$.price' RETURNING NUMBER))");
  }

  @Test
  void shouldRenderCompoundIdWithTypePreservation() {
    // MongoDB: [{$group: {_id: {year: "$year", quarter: "$quarter"}, count: {$sum: 1}}}]
    // When grouping by computed fields (like from $addFields with $year), the compound _id
    // should preserve the original JSON types (numbers) not convert them to strings.
    var idFields = new java.util.LinkedHashMap<String, Expression>();
    idFields.put("year", FieldPathExpression.of("year"));
    idFields.put("quarter", FieldPathExpression.of("quarter"));
    var compoundId = new CompoundIdExpression(idFields);

    var accumulators = new java.util.LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count",
        new AccumulatorExpression(AccumulatorOp.SUM, LiteralExpression.of(1)));

    var groupStage = new GroupStage(compoundId, accumulators);
    Pipeline pipeline = Pipeline.of("orders", groupStage);

    renderer.render(pipeline, context);

    // Should use JSON_QUERY to preserve original types when building compound _id
    // JSON_QUERY naturally preserves JSON types and handles non-scalars
    String sql = context.toSql();
    assertThat(sql)
        .contains("JSON_OBJECT('year' VALUE JSON_QUERY(\"DATA\", '$.year')")
        .contains("'quarter' VALUE JSON_QUERY(\"DATA\", '$.quarter')");
  }

  // =========================================================================
  // Stage Type Tests - $limit
  // =========================================================================

  @Test
  void shouldRenderLimitStageAsCte() {
    // MongoDB: [{$limit: 10}]
    var limitStage = new LimitStage(10);
    Pipeline pipeline = Pipeline.of("orders", limitStage);

    renderer.render(pipeline, context);

    // Should produce CTE with FETCH FIRST
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("FETCH FIRST 10 ROWS ONLY");
  }

  // =========================================================================
  // Stage Type Tests - $skip
  // =========================================================================

  @Test
  void shouldRenderSkipStageAsCte() {
    // MongoDB: [{$skip: 5}]
    var skipStage = new SkipStage(5);
    Pipeline pipeline = Pipeline.of("orders", skipStage);

    renderer.render(pipeline, context);

    // Should produce CTE with OFFSET
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("OFFSET 5 ROWS");
  }

  // =========================================================================
  // Stage Type Tests - $sort
  // =========================================================================

  @Test
  void shouldRenderSortStageAsCte() {
    // MongoDB: [{$sort: {amount: -1, name: 1}}]
    var sortSpec = new java.util.LinkedHashMap<String, Integer>();
    sortSpec.put("amount", -1);  // DESC
    sortSpec.put("name", 1);     // ASC

    var sortStage = new SortStage(sortSpec);
    Pipeline pipeline = Pipeline.of("orders", sortStage);

    renderer.render(pipeline, context);

    // Should produce CTE with ORDER BY
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("ORDER BY")
        .contains("DESC")
        .contains("ASC");
  }

  // =========================================================================
  // Stage Type Tests - $count
  // =========================================================================

  @Test
  void shouldRenderCountStageAsCte() {
    // MongoDB: [{$count: "totalOrders"}]
    var countStage = new CountStage("totalOrders");
    Pipeline pipeline = Pipeline.of("orders", countStage);

    renderer.render(pipeline, context);

    // Should produce CTE with JSON_OBJECT COUNT
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("JSON_OBJECT")
        .contains("'totalOrders'")
        .contains("COUNT(*)");
  }

  // =========================================================================
  // Stage Type Tests - $sample
  // =========================================================================

  @Test
  void shouldRenderSampleStageAsCte() {
    // MongoDB: [{$sample: {size: 5}}]
    var sampleStage = new SampleStage(5);
    Pipeline pipeline = Pipeline.of("orders", sampleStage);

    renderer.render(pipeline, context);

    // Should produce CTE with DBMS_RANDOM.VALUE and FETCH FIRST
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("DBMS_RANDOM.VALUE")
        .contains("FETCH FIRST 5 ROWS ONLY");
  }

  // =========================================================================
  // Stage Type Tests - $addFields
  // =========================================================================

  @Test
  void shouldRenderAddFieldsStageAsCte() {
    // MongoDB: [{$addFields: {total: {$add: ["$price", "$tax"]}}}]
    var fields = new java.util.LinkedHashMap<String,
        Expression>();
    fields.put("total", new ArithmeticExpression(
        ArithmeticOp.ADD,
        java.util.List.of(FieldPathExpression.of("price"), FieldPathExpression.of("tax"))));

    var addFieldsStage = new AddFieldsStage(fields);
    Pipeline pipeline = Pipeline.of("orders", addFieldsStage);

    renderer.render(pipeline, context);

    // Should produce CTE with json_transform SET
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("json_transform")
        .contains("SET")
        .contains("'$.\"total\"'");
  }

  @Test
  void shouldRenderAddFieldsWithSizeExpressionAsCte() {
    // MongoDB: [{$addFields: {itemCount: {$size: "$items"}}}]
    var fields = new java.util.LinkedHashMap<String, Expression>();
    fields.put("itemCount", ArrayExpression.size(FieldPathExpression.of("items")));

    var addFieldsStage = new AddFieldsStage(fields);
    Pipeline pipeline = Pipeline.of("orders", addFieldsStage);

    renderer.render(pipeline, context);
    String sql = context.toSql();

    // Should produce CTE with json_transform SET using JSON_VALUE with .size()
    assertThat(sql)
        .contains("\"Q2\"")
        .contains("json_transform")
        .contains("SET")
        .contains("'$.\"itemCount\"'")
        .contains("JSON_VALUE")
        .contains("$.items.size()")
        .doesNotContain("= NULL");
  }

  @Test
  void shouldRenderAddFieldsWithDateSubtractionAsCte() {
    // MongoDB: [{$addFields: {processingHours: {$divide: [{$subtract: ...}, 3600000]}}}]
    var fields = new java.util.LinkedHashMap<String, Expression>();
    var dateSubtraction = new ArithmeticExpression(
        ArithmeticOp.SUBTRACT,
        java.util.List.of(
            FieldPathExpression.of("shippedDate"), FieldPathExpression.of("orderDate")));
    var divisionExpr = new ArithmeticExpression(
        ArithmeticOp.DIVIDE,
        java.util.List.of(dateSubtraction, LiteralExpression.of(3600000)));
    fields.put("processingHours", divisionExpr);

    var addFieldsStage = new AddFieldsStage(fields);
    Pipeline pipeline = Pipeline.of("orders", addFieldsStage);

    renderer.render(pipeline, context);
    String sql = context.toSql();

    // Should handle date subtraction properly (using EXTRACT for interval conversion)
    assertThat(sql)
        .contains("\"Q2\"")
        .contains("json_transform")
        .contains("SET")
        .contains("'$.\"processingHours\"'")
        // Should have date subtraction handling, not just a simple minus
        .contains("EXTRACT");
  }

  @Test
  void shouldRenderAddFieldsWithDivideOfSumAndSizeAsCte() {
    // MongoDB: [{$addFields: {
    //   avgOrderValue: {$cond: [
    //     {$gt: [{$size: "$orders"}, 0]},
    //     {$divide: [{$sum: "$orders.amount"}, {$size: "$orders"}]},
    //     0
    //   ]}
    // }}]
    // This tests expression-level $sum (array sum) and $size within $divide
    var fields = new java.util.LinkedHashMap<String, Expression>();

    // $size: "$orders"
    var sizeExpr = ArrayExpression.size(FieldPathExpression.of("orders"));

    // $sum: "$orders.amount" - expression-level sum (ArrayOp.SUM_ARRAY, not AccumulatorExpression)
    var sumExpr = ArrayExpression.sumArray(FieldPathExpression.of("orders.amount"));

    // $divide: [$sum, $size]
    var divideExpr = new ArithmeticExpression(
        ArithmeticOp.DIVIDE,
        java.util.List.of(sumExpr, sizeExpr));

    // $gt: [$size, 0]
    var gtCondition = new ComparisonExpression(
        ComparisonOp.GT, sizeExpr, LiteralExpression.of(0));

    // $cond: [condition, thenExpr, elseExpr]
    var condExpr = ConditionalExpression.cond(gtCondition, divideExpr, LiteralExpression.of(0));

    fields.put("avgOrderValue", condExpr);

    var addFieldsStage = new AddFieldsStage(fields);
    Pipeline pipeline = Pipeline.of("customers", addFieldsStage);

    renderer.render(pipeline, context);
    String sql = context.toSql();

    // Should properly render both $sum and $size as numeric operands, NOT as NULL
    assertThat(sql)
        .contains("\"Q2\"")
        .contains("json_transform")
        .contains("SET")
        .contains("'$.\"avgOrderValue\"'")
        // Critical: should NOT contain NULL / NULL pattern
        .doesNotContain("NULL / NULL")
        .doesNotContain("(NULL / NULL)")
        // Should have proper $size rendering
        .contains("$.orders.size()")
        // Should have proper $sum array sum rendering with JSON_TABLE
        .contains("SUM");
  }

  // =========================================================================
  // Stage Type Tests - $unwind
  // =========================================================================

  @Test
  void shouldRenderUnwindStageAsCte() {
    // MongoDB: [{$unwind: "$items"}]
    var unwindStage = new UnwindStage("items");
    Pipeline pipeline = Pipeline.of("orders", unwindStage);

    renderer.render(pipeline, context);

    // Should produce CTE with JSON_TABLE for array flattening
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("JSON_TABLE")
        .contains("$.items[*]");
  }

  // =========================================================================
  // Stage Type Tests - $lookup
  // =========================================================================

  @Test
  void shouldRenderLookupStageAsCte() {
    // MongoDB: [{$lookup: {from: "products", localField: "productId",
    //            foreignField: "_id", as: "product"}}]
    var lookupStage = LookupStage.equality(
        "products", "productId", "_id", "product");
    Pipeline pipeline = Pipeline.of("orders", lookupStage);

    renderer.render(pipeline, context);

    // Should produce CTE that adds array field using correlated subquery
    // The lookup adds "product" array to each document via json_transform
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("json_transform")
        .contains("SET")
        .contains("'$.\"product\"'")
        .contains("JSON_ARRAYAGG")
        .contains("\"PRODUCTS\"");
  }

  @Test
  void shouldRenderLookupWithPipelineAsCte() {
    // MongoDB: [{$lookup: {from: "inventory", let: {productId: "$_id"},
    //            pipeline: [{$match: {$expr: {$eq: ["$productId", "$$productId"]}}},
    //                       {$group: {_id: null, totalStock: {$sum: "$quantity"}}}],
    //            as: "inventoryData"}}]
    var matchStage = new MatchStage(
        new ComparisonExpression(ComparisonOp.EQ,
            FieldPathExpression.of("productId"),
            FieldPathExpression.of("$productId"))); // $$productId becomes $productId
    var groupStage = new GroupStage(
        null, // _id: null
        java.util.Map.of("totalStock",
            new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("quantity"))));
    var lookupStage = LookupStage.withPipeline(
        "inventory",
        java.util.Map.of("productId", "$_id"),
        java.util.List.of(matchStage, groupStage),
        "inventoryData");
    Pipeline pipeline = Pipeline.of("products", lookupStage);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should produce CTE with correlated subquery using LATERAL or subquery
    assertThat(sql)
        .contains("\"Q2\"")
        .contains("json_transform")
        .contains("inventoryData")
        .contains("INVENTORY"); // References the inventory collection
  }

  // =========================================================================
  // Stage Type Tests - $unionWith
  // =========================================================================

  @Test
  void shouldRenderUnionWithStageAsCte() {
    // MongoDB: [{$unionWith: "archived_orders"}]
    var unionWithStage = new UnionWithStage("archived_orders");
    Pipeline pipeline = Pipeline.of("orders", unionWithStage);

    renderer.render(pipeline, context);

    // Should produce CTE with UNION ALL
    assertThat(context.toSql())
        .contains("UNION ALL")
        .contains("\"ARCHIVED_ORDERS\"");
  }

  @Test
  void shouldRenderUnionWithPipelineAsCte() {
    // MongoDB: [{$match: {status: "completed"}},
    //           {$unionWith: {coll: "sales", pipeline: [{$match: {status: "pending"}}]}}]
    var matchStage1 = new MatchStage(
        new ComparisonExpression(ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("completed")));
    var pipelineStages = java.util.List.<Stage>of(
        new MatchStage(
            new ComparisonExpression(ComparisonOp.EQ,
                FieldPathExpression.of("status"),
                LiteralExpression.of("pending"))));
    var unionWithStage = new UnionWithStage("sales", pipelineStages);
    Pipeline pipeline = Pipeline.of("sales", matchStage1, unionWithStage);

    renderer.render(pipeline, context);

    String sql = context.toSql();
    // Should produce CTE with UNION ALL where right side has JSON_EXISTS predicate
    assertThat(sql)
        .contains("UNION ALL")
        .contains("JSON_EXISTS"); // Both $match stages should use JSON_EXISTS
    // Verify bind variables contain the match values (uses bind variables, not literals)
    assertThat(context.getBindVariables())
        .contains("completed", "pending");
  }

  // =========================================================================
  // Stage Type Tests - $bucket
  // =========================================================================

  @Test
  void shouldRenderBucketStageAsCte() {
    // MongoDB: [{$bucket: {groupBy: "$amount", boundaries: [0, 100, 500, 1000]}}]
    var bucketStage = new BucketStage(
        FieldPathExpression.of("amount"),
        java.util.List.of(0, 100, 500, 1000),
        null,
        java.util.Map.of());
    Pipeline pipeline = Pipeline.of("orders", bucketStage);

    renderer.render(pipeline, context);

    // Should produce CTE with CASE expression and GROUP BY
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("CASE")
        .contains("WHEN")
        .contains("GROUP BY");
  }

  @Test
  void shouldRenderBucketWithMixedTypesAsCte() {
    // MongoDB: [{$bucket: {groupBy: "$attendees", boundaries: [0, 50, 100],
    //           default: "large"}}]
    // When boundaries are numbers but default is string, all values must be strings
    var bucketStage = new BucketStage(
        FieldPathExpression.of("attendees"),
        java.util.List.of(0, 50, 100),
        "large",  // String default with numeric boundaries = mixed types
        java.util.Map.of());
    Pipeline pipeline = Pipeline.of("events", bucketStage);

    renderer.render(pipeline, context);

    // Should cast numeric boundaries to strings for type compatibility
    assertThat(context.toSql())
        .contains("THEN '0'")   // Numbers cast to strings
        .contains("THEN '50'")  // Numbers cast to strings
        .contains("'large'");   // String default
  }

  // =========================================================================
  // Stage Type Tests - $facet
  // =========================================================================

  @Test
  void shouldRenderFacetStageAsCte() {
    // MongoDB: [{$facet: {
    //   "byStatus": [{$group: {_id: "$status", count: {$sum: 1}}}],
    //   "topN": [{$sort: {amount: -1}}, {$limit: 5}]
    // }}]
    var groupIdExpr = FieldPathExpression.of("status");
    var accumulators = new java.util.LinkedHashMap<String,
        AccumulatorExpression>();
    accumulators.put("count", new AccumulatorExpression(
        AccumulatorOp.SUM,
        LiteralExpression.of(1)));
    var groupStage = new GroupStage(groupIdExpr, accumulators);

    var sortSpec = new java.util.LinkedHashMap<String, Integer>();
    sortSpec.put("amount", -1);
    var sortStage = new SortStage(sortSpec);
    var limitStage = new LimitStage(5);

    var facets = new java.util.LinkedHashMap<String, java.util.List<
        Stage>>();
    facets.put("byStatus", java.util.List.of(groupStage));
    facets.put("topN", java.util.List.of(sortStage, limitStage));

    var facetStage = new FacetStage(facets);
    Pipeline pipeline = Pipeline.of("orders", facetStage);

    renderer.render(pipeline, context);

    // Should produce separate facet CTEs and combine with JSON_OBJECT
    // Each facet should execute its sub-pipeline, not just dump all data
    assertThat(context.toSql())
        .contains("JSON_OBJECT")
        .contains("'byStatus'")
        .contains("'topN'")
        // byStatus facet should have GROUP BY since it has $group
        .contains("GROUP BY")
        // topN facet should have ORDER BY and FETCH FIRST since it has $sort/$limit
        .contains("ORDER BY")
        .contains("FETCH FIRST 5");
  }

  @Test
  void shouldRenderPostFacetProjectionWithNestedFieldExtraction() {
    // MongoDB: [
    //   {$facet: {
    //     "recordCount": [{$count: "count"}],
    //     "data": [{$limit: 5}]
    //   }},
    //   {$project: {
    //     "elements": "$data._id",
    //     "metadata": {$arrayElemAt: ["$recordCount", 0]}
    //   }}
    // ]
    // The "$data._id" expression should extract _id from each element of the data array

    // Create facet stage
    var countStage = new com.oracle.mongodb.translator.ast.stage.CountStage("count");
    var limitStage = new LimitStage(5);

    var facets = new java.util.LinkedHashMap<String, java.util.List<Stage>>();
    facets.put("recordCount", java.util.List.of(countStage));
    facets.put("data", java.util.List.of(limitStage));

    var facetStage = new FacetStage(facets);

    // Create project stage referencing facet fields
    // "elements": "$data._id" - should extract _id from each element in data array
    var projections = new java.util.LinkedHashMap<String, ProjectStage.ProjectionField>();
    projections.put("elements", ProjectStage.ProjectionField.include(
        FieldPathExpression.of("data._id")));
    projections.put("metadata", ProjectStage.ProjectionField.include(
        ArrayExpression.arrayElemAt(
            FieldPathExpression.of("recordCount"),
            LiteralExpression.of(0))));

    var projectStage = new ProjectStage(projections, false);

    Pipeline pipeline = Pipeline.of("orders", facetStage, projectStage);

    renderer.render(pipeline, context);

    String sql = context.toSql();

    // The SQL should use array element extraction for "$data._id"
    // It should use [*] syntax to iterate over the data array and extract _id from each
    assertThat(sql)
        .contains("Q2_data")  // Facet CTE for data
        .contains("Q2_recordCount")  // Facet CTE for recordCount
        .describedAs("Should extract _id from each element of data array using [*] syntax")
        .containsPattern("data\\[\\*\\]\\._id|\\$\\.data\\[\\*\\]\\._id")
        .contains("WITH WRAPPER");  // WITH WRAPPER returns results as array
    // Note: json_transform cannot use RETURNING clause - only WITH WRAPPER works
  }

  // =========================================================================
  // Stage Type Tests - $bucketAuto
  // =========================================================================

  @Test
  void shouldRenderBucketAutoStageAsCte() {
    // MongoDB: [{$bucketAuto: {
    //   groupBy: "$price",
    //   buckets: 3,
    //   output: {count: {$sum: 1}, avgPrice: {$avg: "$price"}}
    // }}]
    var accumulators = new java.util.LinkedHashMap<String, AccumulatorExpression>();
    accumulators.put("count",
        new AccumulatorExpression(AccumulatorOp.SUM, LiteralExpression.of(1)));
    accumulators.put("avgPrice",
        new AccumulatorExpression(AccumulatorOp.AVG, FieldPathExpression.of("price")));

    var bucketAutoStage = new BucketAutoStage(
        FieldPathExpression.of("price"),
        3,
        accumulators,
        null);
    Pipeline pipeline = Pipeline.of("products", bucketAutoStage);

    renderer.render(pipeline, context);

    // Should produce CTE with NTILE window function and GROUP BY
    assertThat(context.toSql())
        .contains("NTILE(3)")
        .contains("OVER")
        .contains("ORDER BY")
        .contains("GROUP BY")
        .contains("'count'")
        .contains("'avgPrice'")
        .contains("SUM")
        .contains("AVG");
  }

  // =========================================================================
  // Stage Type Tests - $redact
  // =========================================================================

  @Test
  void shouldRenderRedactStageAsCte() {
    // MongoDB: [{$redact: {$cond: {if: {$gte: ["$salary", 80000]},
    //           then: "$$KEEP", else: "$$PRUNE"}}}]
    // Creates a $cond expression: if salary >= 80000, KEEP; else PRUNE
    var condition = new ComparisonExpression(
        ComparisonOp.GTE,
        FieldPathExpression.of("salary"),
        LiteralExpression.of(80000));
    var condExpr = ConditionalExpression.cond(
        condition,
        LiteralExpression.of("$$KEEP"),
        LiteralExpression.of("$$PRUNE"));

    var redactStage = new RedactStage(condExpr);
    Pipeline pipeline = Pipeline.of("employees", redactStage);

    renderer.render(pipeline, context);

    // Should produce CTE with WHERE clause filtering
    // Documents with salary >= 80000 are KEPT, others are PRUNED
    assertThat(context.toSql())
        .contains("\"Q2\"")
        .contains("WHERE")
        .contains("salary")
        .contains("80000");
  }
}
