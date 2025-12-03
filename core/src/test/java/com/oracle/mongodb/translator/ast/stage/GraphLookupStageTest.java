/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GraphLookupStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    // GraphLookupStage.render() requires a base table alias for the outer query reference
    context = new DefaultSqlGenerationContext(true, null, "base");
  }

  @Test
  void shouldCreateWithRequiredFields() {
    var stage = new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy");

    assertThat(stage.getFrom()).isEqualTo("employees");
    assertThat(stage.getStartWith()).isEqualTo("$reportsTo");
    assertThat(stage.getConnectFromField()).isEqualTo("reportsTo");
    assertThat(stage.getConnectToField()).isEqualTo("name");
    assertThat(stage.getAs()).isEqualTo("hierarchy");
    assertThat(stage.getMaxDepth()).isNull();
    assertThat(stage.getDepthField()).isNull();
  }

  @Test
  void shouldCreateWithAllOptions() {
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, "level");

    assertThat(stage.getMaxDepth()).isEqualTo(5);
    assertThat(stage.getDepthField()).isEqualTo("level");
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new GraphLookupStage("col", "$f", "from", "to", "result");

    assertThat(stage.getOperatorName()).isEqualTo("$graphLookup");
  }

  @Test
  void shouldRenderRecursiveCte() {
    var stage = new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy");

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("WITH graph_hierarchy");
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("JSON_ARRAYAGG");
    assertThat(sql).contains("employees");
  }

  @Test
  void shouldRenderCteWithMaxDepth() {
    var stage =
        new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, null);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("graph_depth < 5");
  }

  @Test
  void shouldRenderCteWithDepthField() {
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, "depth");

    stage.render(context);

    String sql = context.toSql();
    // Note: "level" is a reserved word and would be quoted; use "depth" instead
    assertThat(sql).contains("g.graph_depth AS depth");
  }

  @Test
  void shouldThrowOnNullFrom() {
    assertThatNullPointerException()
        .isThrownBy(() -> new GraphLookupStage(null, "$f", "from", "to", "result"))
        .withMessageContaining("from");
  }

  @Test
  void shouldThrowOnNullStartWith() {
    assertThatNullPointerException()
        .isThrownBy(() -> new GraphLookupStage("col", null, "from", "to", "result"))
        .withMessageContaining("startWith");
  }

  @Test
  void shouldProvideReadableToString() {
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, "level");

    assertThat(stage.toString())
        .contains("GraphLookupStage")
        .contains("employees")
        .contains("maxDepth=5")
        .contains("depthField=level");
  }

  // restrictSearchWithMatch tests

  @Test
  void shouldCreateWithRestrictSearchWithMatch() {
    var restrictMatch = Document.parse("{\"status\": \"active\"}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    assertThat(stage.getRestrictSearchWithMatch()).isNotNull();
    assertThat(stage.getRestrictSearchWithMatch().getString("status")).isEqualTo("active");
  }

  @Test
  void shouldCreateWithAllOptionsIncludingRestrictSearchWithMatch() {
    var restrictMatch = Document.parse("{\"active\": true}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, "level", restrictMatch);

    assertThat(stage.getFrom()).isEqualTo("employees");
    assertThat(stage.getMaxDepth()).isEqualTo(5);
    assertThat(stage.getDepthField()).isEqualTo("level");
    assertThat(stage.getRestrictSearchWithMatch()).isNotNull();
  }

  @Test
  void shouldRenderCteWithRestrictSearchWithMatch() {
    var restrictMatch = Document.parse("{\"status\": \"active\"}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("WITH graph_hierarchy");
    assertThat(sql).contains("UNION ALL");
    // The restrict match should add a WHERE clause filtering by status
    assertThat(sql).contains("status");
  }

  @Test
  void shouldRenderCteWithMaxDepthAndRestrictSearchWithMatch() {
    var restrictMatch = Document.parse("{\"department\": \"Engineering\"}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("graph_depth < 5");
    assertThat(sql).contains("department");
  }

  @Test
  void shouldIncludeRestrictSearchWithMatchInToString() {
    var restrictMatch = Document.parse("{\"status\": \"active\"}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    assertThat(stage.toString()).contains("GraphLookupStage").contains("restrictSearchWithMatch");
  }

  // Tests for renderOperatorCondition - covering all operator branches

  @Test
  void shouldRenderRestrictSearchWithMatchWithInOperator() {
    var restrictMatch = Document.parse("{\"status\": {\"$in\": [\"active\", \"pending\"]}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("IN (");
    assertThat(sql).contains("'active'");
    assertThat(sql).contains("'pending'");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithEqOperator() {
    var restrictMatch = Document.parse("{\"status\": {\"$eq\": \"active\"}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("= 'active'");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithNeOperator() {
    var restrictMatch = Document.parse("{\"status\": {\"$ne\": \"deleted\"}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("!= 'deleted'");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithGtOperator() {
    var restrictMatch = Document.parse("{\"age\": {\"$gt\": 21}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("> 21");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithGteOperator() {
    var restrictMatch = Document.parse("{\"age\": {\"$gte\": 18}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains(">= 18");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithLtOperator() {
    var restrictMatch = Document.parse("{\"age\": {\"$lt\": 65}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("< 65");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithLteOperator() {
    var restrictMatch = Document.parse("{\"age\": {\"$lte\": 100}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("<= 100");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithUnsupportedOperator() {
    var restrictMatch = Document.parse("{\"tags\": {\"$all\": [\"a\"]}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    // Unsupported operators fall back to equality
    String sql = context.toSql();
    assertThat(sql).contains("tags");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithBooleanValue() {
    var restrictMatch = Document.parse("{\"active\": true}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("'true'");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithBooleanFalseValue() {
    var restrictMatch = Document.parse("{\"deleted\": false}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("'false'");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithNullValue() {
    var restrictMatch = new Document("deletedAt", null);
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("NULL");
  }

  // Tests for renderCteDefinition

  @Test
  void shouldRenderCteDefinitionWithMaxDepthZero() {
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 0, null, null);

    var cteContext = new DefaultSqlGenerationContext();
    stage.renderCteDefinition(cteContext, "source");

    String sql = cteContext.toSql();
    assertThat(sql).contains("graph_hierarchy");
    assertThat(sql).contains("SELECT g.id, g.data FROM employees g");
    assertThat(sql).doesNotContain("DUAL");
  }

  @Test
  void shouldRenderCteDefinitionWithMaxDepthZeroAndRestrictSearchWithMatch() {
    var restrictMatch = Document.parse("{\"status\": \"active\"}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 0, null, restrictMatch);

    var cteContext = new DefaultSqlGenerationContext();
    stage.renderCteDefinition(cteContext, "source");

    String sql = cteContext.toSql();
    assertThat(sql).contains("graph_hierarchy");
    assertThat(sql).contains("status");
    assertThat(sql).contains("'active'");
  }

  @Test
  void shouldRenderCteDefinitionWithRecursiveDepth() {
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, null, null);

    var cteContext = new DefaultSqlGenerationContext();
    stage.renderCteDefinition(cteContext, "source");

    String sql = cteContext.toSql();
    // For recursive cases, placeholder is used
    assertThat(sql).contains("DUAL");
    assertThat(sql).contains("1=0");
  }

  @Test
  void shouldRenderCteDefinitionWithNoMaxDepth() {
    var stage = new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy");

    var cteContext = new DefaultSqlGenerationContext();
    stage.renderCteDefinition(cteContext, "source");

    String sql = cteContext.toSql();
    // No max depth means it uses recursive placeholder
    assertThat(sql).contains("DUAL");
  }

  @Test
  void shouldGetCteName() {
    var stage = new GraphLookupStage("employees", "$reportsTo", "reportsTo", "name", "hierarchy");

    assertThat(stage.getCteName()).isEqualTo("graph_hierarchy");
  }

  @Test
  void shouldRenderStartWithWithoutDollarSign() {
    var stage = new GraphLookupStage("employees", "reportsTo", "reportsTo", "name", "hierarchy");

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("$.reportsTo");
  }

  @Test
  void shouldThrowOnNullConnectFromField() {
    assertThatNullPointerException()
        .isThrownBy(() -> new GraphLookupStage("col", "$f", null, "to", "result"))
        .withMessageContaining("connectFromField");
  }

  @Test
  void shouldThrowOnNullConnectToField() {
    assertThatNullPointerException()
        .isThrownBy(() -> new GraphLookupStage("col", "$f", "from", null, "result"))
        .withMessageContaining("connectToField");
  }

  @Test
  void shouldThrowOnNullAs() {
    assertThatNullPointerException()
        .isThrownBy(() -> new GraphLookupStage("col", "$f", "from", "to", null))
        .withMessageContaining("as");
  }

  @Test
  void shouldRenderWithMaxDepthAndWithoutRestrictSearchWithMatch() {
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, null, null);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("WHERE g.graph_depth < 5");
    assertThat(sql).doesNotContain("AND JSON_VALUE(c.data");
  }

  @Test
  void shouldRenderWithRestrictSearchWithMatchWithoutMaxDepth() {
    var restrictMatch = Document.parse("{\"status\": \"active\"}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    // Without maxDepth, WHERE clause starts differently
    assertThat(sql).contains("WHERE 1=1");
    assertThat(sql).contains("AND JSON_VALUE(c.data");
  }

  @Test
  void shouldRenderRestrictSearchWithMatchWithStringContainingSingleQuote() {
    var restrictMatch = Document.parse("{\"name\": \"O'Reilly\"}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    // Single quotes should be escaped
    assertThat(sql).contains("O''Reilly");
  }

  @Test
  void shouldRenderWithEmptyRestrictSearchWithMatch() {
    var restrictMatch = new Document();
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    // Empty document should not add any additional conditions
    assertThat(sql).doesNotContain("WHERE 1=1");
  }

  // ==================== Additional Recursive Query Tests ====================

  @Test
  void shouldRenderDeepHierarchyTraversal() {
    // Test organizational hierarchy traversal (manager -> reports)
    var stage =
        new GraphLookupStage(
            "employees", "$managerId", "managerId", "_id", "reportingChain", 10, "depth");

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("WITH graph_reportingChain");
    assertThat(sql).contains("UNION ALL");
    assertThat(sql).contains("graph_depth < 10");
    assertThat(sql).contains("AS depth");
  }

  @Test
  void shouldRenderCategoryTreeTraversal() {
    // Test category/taxonomy tree traversal
    var stage =
        new GraphLookupStage("categories", "$parentId", "parentId", "_id", "ancestors", 5, null);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("categories");
    assertThat(sql).contains("parentId");
  }

  @Test
  void shouldRenderGraphTraversalWithComplexFilter() {
    // Social network friends-of-friends with status filter
    var restrictMatch = Document.parse("{\"status\": {\"$in\": [\"active\", \"verified\"]}}");
    var stage =
        new GraphLookupStage(
            "users", "$friends", "friends", "_id", "network", 3, "distance", restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("IN (");
    assertThat(sql).contains("'active'");
    assertThat(sql).contains("'verified'");
    assertThat(sql).contains("graph_depth < 3");
    assertThat(sql).contains("AS distance");
  }

  @Test
  void shouldRenderBillOfMaterialsTraversal() {
    // Product components / bill of materials traversal
    var stage =
        new GraphLookupStage(
            "components", "$componentIds", "componentIds", "_id", "allParts", null, "level");

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("components");
    assertThat(sql).contains("componentIds");
    // Note: depth field is rendered with quotes: AS "level"
    assertThat(sql).contains("\"level\"");
  }

  @Test
  void shouldRenderGraphWithMultipleConditions() {
    // Graph with multiple filter conditions
    var restrictMatch =
        Document.parse("{\"active\": true, \"type\": {\"$ne\": \"archived\"}, \"priority\": {\"$gt\": 0}}");
    var stage =
        new GraphLookupStage(
            "tasks", "$dependencies", "dependencies", "_id", "taskChain", 5, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("'true'"); // active: true
    assertThat(sql).contains("!="); // $ne
    assertThat(sql).contains(">"); // $gt
  }

  @Test
  void shouldRenderGraphWithNestedFieldPath() {
    // Graph lookup using nested field paths
    var stage =
        new GraphLookupStage(
            "orders", "$customer.referralId", "referredBy", "customerId", "referralChain", 5, null);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("customer.referralId");
  }

  @Test
  void shouldRenderGraphWithMaxDepthOne() {
    // Single level graph lookup (maxDepth=1 - only immediate connections)
    var stage =
        new GraphLookupStage(
            "employees", "$directReports", "directReports", "_id", "team", 1, null);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("graph_depth < 1");
  }

  @Test
  void shouldRenderGraphWithNumericFilter() {
    // Graph with numeric range filter
    var restrictMatch = Document.parse("{\"salary\": {\"$gte\": 50000, \"$lte\": 150000}}");
    var stage =
        new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", null, null, restrictMatch);

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains(">=");
    assertThat(sql).contains("<=");
  }
}
