/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.AccumulatorOp;
import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticOp;
import com.oracle.mongodb.translator.ast.expression.ArrayExpression;
import com.oracle.mongodb.translator.ast.expression.CompoundIdExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortDirection;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortField;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * TDD tests for PipelineStageSequence - analyzes pipelines to detect CTE breakpoints.
 *
 * <p>A CTE (Common Table Expression) is needed when:
 *
 * <ul>
 *   <li>Multiple $group stages exist - each group after the first needs intermediate results
 *   <li>$project after $group references grouped output fields with array operations
 *   <li>$unwind → $lookup sequences require intermediate materialization before $group
 * </ul>
 */
class PipelineStageSequenceTest {

  // ==========================================================================
  // Test 1: Detect Multiple $group Stages
  // ==========================================================================

  @Test
  void shouldDetectMultipleGroupStages() {
    // First $group: group by category with sum
    Map<String, AccumulatorExpression> firstAccums = new LinkedHashMap<>();
    AccumulatorExpression sumAmount =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("amount"));
    firstAccums.put("totalRevenue", sumAmount);
    GroupStage firstGroup = new GroupStage(FieldPathExpression.of("category"), firstAccums);

    // Second $group: group by _id.region (references first group output)
    Map<String, AccumulatorExpression> secondAccums = new LinkedHashMap<>();
    AccumulatorExpression sumRevenue =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("totalRevenue"));
    secondAccums.put("grandTotal", sumRevenue);
    GroupStage secondGroup = new GroupStage(FieldPathExpression.of("_id.region"), secondAccums);

    Pipeline pipeline = Pipeline.of("orders", firstGroup, secondGroup);
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    assertThat(sequence.requiresCtes()).isTrue();
    assertThat(sequence.getCteCount()).isEqualTo(1);
    assertThat(sequence.getStageGroups()).hasSize(2);
    assertThat(sequence.getStageGroups().get(0).getStages()).containsExactly(firstGroup);
    assertThat(sequence.getStageGroups().get(1).getStages()).containsExactly(secondGroup);
  }

  @Test
  void shouldNotRequireCteForSingleGroup() {
    Map<String, AccumulatorExpression> accums = new LinkedHashMap<>();
    AccumulatorExpression sumAmount =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("amount"));
    accums.put("total", sumAmount);
    GroupStage group = new GroupStage(FieldPathExpression.of("category"), accums);

    Pipeline pipeline = Pipeline.of("orders", group);
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    assertThat(sequence.requiresCtes()).isFalse();
    assertThat(sequence.getCteCount()).isZero();
  }

  // ==========================================================================
  // Test 2: Detect $project After $group With Field References
  // ==========================================================================

  @Test
  void shouldDetectProjectAfterGroupWithArrayOps() {
    // $group with $push to create array
    Map<String, AccumulatorExpression> accums = new LinkedHashMap<>();
    AccumulatorExpression pushProducts =
        new AccumulatorExpression(AccumulatorOp.PUSH, FieldPathExpression.of("productInfo"));
    accums.put("products", pushProducts);
    GroupStage group = new GroupStage(FieldPathExpression.of("category"), accums);

    // $project with $slice on grouped products array
    ArrayExpression sliceExpr =
        ArrayExpression.slice(FieldPathExpression.of("products"), LiteralExpression.of(3));
    Map<String, ProjectionField> projections = new LinkedHashMap<>();
    projections.put("topProducts", ProjectionField.include(sliceExpr));
    ProjectStage project = new ProjectStage(projections, false);

    Pipeline pipeline = Pipeline.of("orders", group, project);
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    assertThat(sequence.requiresCtes()).isTrue();
    assertThat(sequence.getCteCount()).isEqualTo(1);
    // Group goes in CTE, project in final query
    assertThat(sequence.getStageGroups().get(0).getStages()).containsExactly(group);
    assertThat(sequence.getStageGroups().get(1).getStages()).containsExactly(project);
  }

  @Test
  void shouldNotRequireCteForSimpleProjectAfterGroup() {
    // $group
    Map<String, AccumulatorExpression> accums = new LinkedHashMap<>();
    AccumulatorExpression sumAmount =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("amount"));
    accums.put("total", sumAmount);
    GroupStage group = new GroupStage(FieldPathExpression.of("category"), accums);

    // $project that only includes/excludes fields (no array ops on group output)
    Map<String, ProjectionField> projections = new LinkedHashMap<>();
    projections.put("_id", ProjectionField.exclude());
    projections.put("category", ProjectionField.include(FieldPathExpression.of("_id")));
    projections.put("total", ProjectionField.include(FieldPathExpression.of("total")));
    ProjectStage project = new ProjectStage(projections, false);

    Pipeline pipeline = Pipeline.of("orders", group, project);
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    // Simple field renaming can be done inline, no CTE needed
    assertThat(sequence.requiresCtes()).isFalse();
  }

  // ==========================================================================
  // Test 3: Detect $unwind → $lookup Sequences
  // ==========================================================================

  @Test
  void shouldNotRequireCteForSingleGroupAfterUnwindLookup() {
    // Pipeline: unwind -> lookup -> unwind -> group (single $group)
    // This should NOT require CTEs - only multiple $groups need CTEs
    final UnwindStage unwind = new UnwindStage("items", null, false);
    final LookupStage lookup =
        LookupStage.equality("products", "items.productId", "_id", "productInfo");
    final UnwindStage unwindProduct = new UnwindStage("productInfo", null, false);

    // Following $group that references unwound/looked up fields
    Map<String, AccumulatorExpression> accums = new LinkedHashMap<>();
    AccumulatorExpression sumQty =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("items.quantity"));
    accums.put("totalQuantity", sumQty);

    Map<String, Expression> idFields = new LinkedHashMap<>();
    idFields.put("category", FieldPathExpression.of("productInfo.category"));
    idFields.put("productId", FieldPathExpression.of("productInfo._id"));
    CompoundIdExpression compoundId = new CompoundIdExpression(idFields);
    GroupStage group = new GroupStage(compoundId, accums);

    Pipeline pipeline = Pipeline.of("orders", unwind, lookup, unwindProduct, group);
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    // Single $group does NOT require CTEs - handled by standard rendering
    assertThat(sequence.requiresCtes()).isFalse();
    assertThat(sequence.getCteCount()).isZero();
  }

  // ==========================================================================
  // Test 4: Simple Pipelines Don't Need CTEs
  // ==========================================================================

  @Test
  void shouldNotRequireCteForSimplePipeline() {
    MatchStage match = new MatchStage(LiteralExpression.of(true));
    SortField sortField = new SortField(FieldPathExpression.of("date"), SortDirection.DESC);
    SortStage sort = new SortStage(List.of(sortField));
    LimitStage limit = new LimitStage(10);

    Pipeline pipeline = Pipeline.of("orders", match, sort, limit);
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    assertThat(sequence.requiresCtes()).isFalse();
    assertThat(sequence.getCteCount()).isZero();
  }

  @Test
  void shouldNotRequireCteForEmptyPipeline() {
    Pipeline pipeline = Pipeline.of("orders");
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    assertThat(sequence.requiresCtes()).isFalse();
    assertThat(sequence.getCteCount()).isZero();
  }

  // ==========================================================================
  // Test 5: Complex Multi-Group Scenario (COMPLEX028 pattern)
  // ==========================================================================

  @Test
  void shouldDetectComplex028Pattern() {
    // Stage 1: $unwind items
    final UnwindStage unwindItems = new UnwindStage("items", null, false);

    // Stage 2: $lookup products
    final LookupStage lookupProducts =
        LookupStage.equality("products", "items.productId", "_id", "productInfo");

    // Stage 3: $unwind productInfo
    final UnwindStage unwindProduct = new UnwindStage("productInfo", null, false);

    // Stage 4: First $group (compound _id)
    Map<String, Expression> idFields1 = new LinkedHashMap<>();
    idFields1.put("category", FieldPathExpression.of("productInfo.category"));
    idFields1.put("productId", FieldPathExpression.of("productInfo._id"));
    idFields1.put("productName", FieldPathExpression.of("productInfo.name"));
    CompoundIdExpression compoundId1 = new CompoundIdExpression(idFields1);

    Map<String, AccumulatorExpression> accums1 = new LinkedHashMap<>();
    AccumulatorExpression sumQty =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("items.quantity"));
    accums1.put("totalQuantitySold", sumQty);

    ArithmeticExpression revenueCalc = new ArithmeticExpression(
        ArithmeticOp.MULTIPLY,
        List.of(
            FieldPathExpression.of("items.quantity"),
            FieldPathExpression.of("productInfo.price")));
    AccumulatorExpression sumRevenue = new AccumulatorExpression(AccumulatorOp.SUM, revenueCalc);
    accums1.put("totalRevenue", sumRevenue);
    final GroupStage group1 = new GroupStage(compoundId1, accums1);

    // Stage 5: Second $group (by category from first group's output)
    Map<String, AccumulatorExpression> accums2 = new LinkedHashMap<>();
    AccumulatorExpression sumCatRevenue =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("totalRevenue"));
    accums2.put("totalCategoryRevenue", sumCatRevenue);
    // $push to create products array
    AccumulatorExpression pushRoot =
        new AccumulatorExpression(AccumulatorOp.PUSH, FieldPathExpression.of("$ROOT"));
    accums2.put("products", pushRoot);
    GroupStage group2 = new GroupStage(FieldPathExpression.of("_id.category"), accums2);

    // Stage 6: $project with $slice (complex array ops)
    ArrayExpression sliceExpr =
        ArrayExpression.slice(FieldPathExpression.of("products"), LiteralExpression.of(3));
    Map<String, ProjectionField> projections = new LinkedHashMap<>();
    projections.put("_id", ProjectionField.exclude());
    projections.put("category", ProjectionField.include(FieldPathExpression.of("_id")));
    FieldPathExpression catRevField = FieldPathExpression.of("totalCategoryRevenue");
    projections.put("totalCategoryRevenue", ProjectionField.include(catRevField));
    projections.put("topProducts", ProjectionField.include(sliceExpr));
    ProjectStage project = new ProjectStage(projections, false);

    Pipeline pipeline = Pipeline.of(
        "orders", unwindItems, lookupProducts, unwindProduct, group1, group2, project);
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    // Should require CTEs for this complex pipeline
    assertThat(sequence.requiresCtes()).isTrue();
    // At minimum: 2 CTEs (unwind/lookup chain, first group) + final query
    assertThat(sequence.getCteCount()).isGreaterThanOrEqualTo(2);
    assertThat(sequence.getStageGroups().size()).isGreaterThanOrEqualTo(3);
  }

  // ==========================================================================
  // Test 6: StageGroup Naming
  // ==========================================================================

  @Test
  void shouldGenerateCteName() {
    Map<String, AccumulatorExpression> accums = new LinkedHashMap<>();
    AccumulatorExpression sumAmount =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("amount"));
    accums.put("total", sumAmount);
    GroupStage firstGroup = new GroupStage(FieldPathExpression.of("category"), accums);

    Map<String, AccumulatorExpression> accums2 = new LinkedHashMap<>();
    AccumulatorExpression sumTotal =
        new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("total"));
    accums2.put("grandTotal", sumTotal);
    GroupStage secondGroup = new GroupStage(FieldPathExpression.of("region"), accums2);

    Pipeline pipeline = Pipeline.of("orders", firstGroup, secondGroup);
    PipelineStageSequence sequence = PipelineStageSequence.analyze(pipeline);

    // First stage group should have a CTE name
    assertThat(sequence.getStageGroups().get(0).getCteName()).isNotNull();
    assertThat(sequence.getStageGroups().get(0).getCteName()).startsWith("cte_");

    // Final stage group (final query) should not have CTE name
    int lastIndex = sequence.getStageGroups().size() - 1;
    assertThat(sequence.getStageGroups().get(lastIndex).getCteName()).isNull();
  }
}
