/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticOp;
import com.oracle.mongodb.translator.ast.expression.ArrayExpression;
import com.oracle.mongodb.translator.ast.expression.ArrayOp;
import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.CompoundIdExpression;
import com.oracle.mongodb.translator.ast.expression.ConditionalExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.InlineObjectExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.expression.SwitchExpression;
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
import com.oracle.mongodb.translator.ast.stage.MergeStage;
import com.oracle.mongodb.translator.ast.stage.OutStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.RedactStage;
import com.oracle.mongodb.translator.ast.stage.ReplaceRootStage;
import com.oracle.mongodb.translator.ast.stage.SampleStage;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.ast.stage.UnionWithStage;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import com.oracle.mongodb.translator.util.FieldNameValidator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Renders a MongoDB aggregation pipeline as a properly structured Oracle SQL query.
 *
 * <p>This class analyzes the pipeline stages and combines them into a single SQL query with the
 * correct clause ordering:
 *
 * <pre>
 * SELECT ...
 * FROM table [alias]
 * [LEFT OUTER JOIN ...]
 * WHERE ...
 * GROUP BY ...
 * ORDER BY ...
 * OFFSET n ROWS
 * FETCH FIRST m ROWS ONLY
 * </pre>
 *
 * <p>Multiple $match stages are combined with AND. The last $project or $group determines the
 * SELECT clause.
 */
public final class PipelineRenderer {

  private final OracleConfiguration config;

  public PipelineRenderer(OracleConfiguration config) {
    this.config = config;
  }

  /** Renders the pipeline to the given context. */
  public void render(Pipeline pipeline, SqlGenerationContext ctx) {
    // Analyze pipeline to extract components
    PipelineComponents components = analyzePipeline(pipeline);

    // If there's an $out stage, render INSERT INTO ... SELECT pattern
    if (components.outStage != null) {
      renderWithOutStage(pipeline, components, ctx);
      return;
    }

    // If there's a $merge stage, render MERGE INTO ... USING ... ON ... pattern
    if (components.mergeStage != null) {
      renderWithMergeStage(pipeline, components, ctx);
      return;
    }

    // If pipeline requires CTEs for multiple $group stages, use CTE rendering path
    // But not if there's a $facet stage - those need special rendering via standard path
    if (components.stageSequence != null
        && components.stageSequence.requiresCtes()
        && components.facetStage == null) {
      renderWithMultiGroupCtes(pipeline, components, ctx);
      return;
    }

    // Register virtual fields from $addFields stages
    // These fields can then be referenced by subsequent stages like $group
    for (AddFieldsStage addFields : components.addFieldsStages) {
      for (var entry : addFields.getFields().entrySet()) {
        ctx.registerVirtualField(entry.getKey(), entry.getValue());
      }
    }

    // Register lookup fields so $size can generate correlated subqueries
    // Also pre-register table aliases so field paths can resolve correctly
    for (LookupStage lookup : components.lookupStages) {
      if (!lookup.isPipelineForm()) {
        ctx.registerLookupField(
            lookup.getAs(), lookup.getFrom(), lookup.getLocalField(), lookup.getForeignField());
        // Pre-register the table alias for this lookup
        // This allows field paths like "$customer.tier" to resolve during SELECT rendering
        String alias = ctx.generateTableAlias(lookup.getFrom());
        ctx.registerLookupTableAlias(lookup.getAs(), alias);
      } else {
        // Pre-register pipeline form lookup alias
        // Pipeline lookups produce a JSON array column via LATERAL subquery
        // This allows $size and $sum on "$orders" to reference the LATERAL result
        String alias = ctx.generateTableAlias(lookup.getFrom());
        ctx.registerLookupTableAlias(lookup.getAs(), alias);
        ctx.registerPipelineLookupAlias(lookup.getAs(), alias);
      }
    }

    // Pre-register graphLookup aliases so field paths can resolve correctly
    // GraphLookup produces a JSON array column similar to pipeline lookups
    // This allows $size: "$colleagues" to reference the LATERAL/CTE result
    for (GraphLookupStage graphLookup : components.graphLookupStages) {
      String alias = graphLookup.getAs() + "_cte";
      ctx.registerPipelineLookupAlias(graphLookup.getAs(), alias);
    }

    // Pre-register unwind paths so field paths can resolve correctly
    // After $unwind: "$items", references like "$items.product" should access the JSON_TABLE column
    for (UnwindStage unwind : components.unwindStages) {
      // Skip unwinds on lookup fields - they don't produce separate JSON_TABLE joins
      if (!isUnwindOnLookupField(unwind.getPath(), components)) {
        String alias = ctx.generateTableAlias("unwind");
        ctx.registerUnwoundPath(unwind.getPath(), alias);

        // If includeArrayIndex is specified, register it as a virtual field
        // that references the JSON_TABLE ORDINALITY column directly
        String indexField = unwind.getIncludeArrayIndex();
        if (indexField != null) {
          final String unwindAlias = alias;
          final String indexName = indexField;
          // Register as virtual field that renders as "(alias.columnName - 1)"
          // Subtract 1 because Oracle FOR ORDINALITY is 1-based, MongoDB index is 0-based
          ctx.registerVirtualField(
              indexField,
              new Expression() {
                @Override
                public void render(SqlGenerationContext renderCtx) {
                  renderCtx.sql("(");
                  renderCtx.sql(unwindAlias);
                  renderCtx.sql(".");
                  renderCtx.sql(indexName);
                  renderCtx.sql(" - 1)");
                }
              });
        }
      }
    }

    // Render WITH clause for CTEs ($graphLookup stages)
    renderCteClause(components, ctx);

    // If we have post-union $group, we need to wrap the entire query (including UNION) in a
    // subquery
    if (components.hasPostUnionGroup) {
      renderWithPostUnionGroup(components, ctx);
    } else {
      // If we have post-window $match (filtering on window function results), wrap in subquery
      if (components.hasPostWindowMatch) {
        renderWithPostWindowMatch(components, ctx);
      } else if (components.hasPostGroupSetWindowFields) {
        // If we have $setWindowFields after $group, wrap GROUP query and add window functions
        renderWithPostGroupSetWindowFields(components, ctx);
      } else if (components.hasPostGroupAddFields) {
        // If we have post-group $addFields, we need to wrap the GROUP query in a subquery
        renderWithPostGroupAddFields(components, ctx);
      } else if (components.bucketAutoStage != null) {
        // $bucketAuto uses NTILE which requires a subquery pattern
        renderWithBucketAuto(components, ctx);
      } else {
        renderStandardQuery(components, ctx);
      }

      // Render UNION ALL clauses ($unionWith stages)
      renderUnionWithClauses(components, ctx);

      // Render post-union ORDER BY and FETCH FIRST (if sort/limit came after unionWith)
      renderPostUnionSortAndLimit(components, ctx);
    }
  }

  /** Renders the standard query without post-group $addFields wrapping. */
  private void renderStandardQuery(PipelineComponents components, SqlGenerationContext ctx) {
    // Check if we need the subquery pattern for $project with type-preserving JSON output
    // This is needed when there's a $project because JSON_ARRAYAGG aggregates all rows,
    // but FETCH FIRST / OFFSET must apply BEFORE aggregation
    // Note: UNION queries and nested pipelines need separate handling since they require
    // row-by-row output, not aggregated JSON
    if (components.projectStage != null
        && components.groupStage == null
        && components.facetStage == null
        && components.countStage == null
        && components.bucketStage == null
        && components.bucketAutoStage == null
        && components.replaceRootStage == null
        && components.unionWithStages.isEmpty()
        && !ctx.isNestedPipeline()) {
      renderProjectWithSubquery(components, ctx);
      return;
    }

    // $count needs subquery pattern to avoid nested aggregate functions error (ORA-00978)
    // Output: SELECT JSON_ARRAYAGG(JSON_OBJECT('field' VALUE cnt) RETURNING CLOB)
    //         FROM (SELECT COUNT(*) AS cnt FROM table [WHERE ...])
    if (components.countStage != null && !ctx.isNestedPipeline()) {
      renderCountWithSubquery(components, ctx);
      return;
    }

    // Render SELECT clause
    renderSelectClause(components, ctx);

    // Render FROM clause
    renderFromClause(components, ctx);

    // For $facet, all processing happens in the subqueries - skip the rest
    if (components.facetStage != null) {
      return;
    }

    // Render JOIN clauses ($lookup stages)
    renderJoinClauses(components, ctx);

    // Render $graphLookup joins
    renderGraphLookupJoins(components, ctx);

    // Render WHERE clause (combined $match stages)
    renderWhereClause(components, ctx);

    // Render GROUP BY clause
    renderGroupByClause(components, ctx);

    // Render ORDER BY clause
    renderOrderByClause(components, ctx);

    // Render OFFSET clause
    renderOffsetClause(components, ctx);

    // Render FETCH clause
    renderFetchClause(components, ctx);
  }

  /**
   * Renders a $project query using the subquery pattern for type-preserving JSON output. This
   * pattern ensures that FETCH FIRST / OFFSET apply to rows before aggregation:
   *
   * <pre>
   * SELECT JSON_ARRAYAGG(JSON_OBJECT(*) RETURNING CLOB)
   * FROM (
   *   SELECT JSON_QUERY(base.data, '$._id') AS "_id", ...
   *   FROM table base
   *   WHERE ...
   *   ORDER BY base.data.field
   *   FETCH FIRST n ROWS ONLY
   * ) sub
   * </pre>
   */
  private void renderProjectWithSubquery(PipelineComponents components, SqlGenerationContext ctx) {
    final ProjectStage project = components.projectStage;

    // Outer query: wrap everything with JSON_ARRAYAGG(JSON_OBJECT(*))
    ctx.sql("SELECT JSON_ARRAYAGG(JSON_OBJECT(*) RETURNING CLOB) FROM (");

    // Inner query: SELECT with JSON_QUERY for each projected field
    ctx.sql("SELECT ");
    ctx.setJsonOutputMode(true);

    // Collect computed field names from $addFields, $setWindowFields, and $graphLookup
    Set<String> computedFieldNames = new HashSet<>();
    for (AddFieldsStage addFields : components.addFieldsStages) {
      computedFieldNames.addAll(addFields.getFields().keySet());
    }
    for (SetWindowFieldsStage swf : components.setWindowFieldsStages) {
      computedFieldNames.addAll(swf.getOutput().keySet());
    }
    // Also add $graphLookup output field names - these are rendered separately below
    for (GraphLookupStage graphLookup : components.graphLookupStages) {
      computedFieldNames.add(graphLookup.getAs());
      if (graphLookup.getDepthField() != null) {
        computedFieldNames.add(graphLookup.getDepthField());
      }
    }

    // Track which computed fields $project transformed (so $addFields skips them)
    Set<String> projectTransformedFields = new HashSet<>();
    // Track which computed fields are included in $project (simple inclusion or transformed)
    Set<String> projectIncludedComputedFields = new HashSet<>();

    boolean first = true;
    for (var entry : project.getProjections().entrySet()) {
      final String alias = entry.getKey();
      ProjectStage.ProjectionField field = entry.getValue();

      if (field.isExcluded()) {
        continue;
      }

      // Skip fields computed by $addFields/$setWindowFields if just passing through
      if (computedFieldNames.contains(alias) && isSimpleFieldInclusion(field, alias)) {
        // Track this field as included so it gets rendered by renderAddFieldsExcluding
        projectIncludedComputedFields.add(alias);
        continue;
      }

      // Track transformed computed fields so $addFields doesn't re-render them
      if (computedFieldNames.contains(alias)) {
        projectTransformedFields.add(alias);
        projectIncludedComputedFields.add(alias);
      }

      if (!first) {
        ctx.sql(", ");
      }

      // Render the expression (JSON_QUERY for field paths in JSON output mode)
      if (field.getExpression() != null) {
        ctx.visit(field.getExpression());
      }
      ctx.sql(" AS ");
      ctx.identifier(alias);
      first = false;
    }

    if (first) {
      ctx.sql("NULL AS dummy");
    }

    ctx.setJsonOutputMode(false);

    // Render $addFields computed columns that are included in $project but not transformed
    // (i.e., simple inclusions like "nameParts: 1")
    for (AddFieldsStage addFields : components.addFieldsStages) {
      renderAddFieldsIncluding(
          addFields, projectIncludedComputedFields, projectTransformedFields, ctx);
    }

    // Render $setWindowFields window function columns
    for (SetWindowFieldsStage setWindowFields : components.setWindowFieldsStages) {
      ctx.sql(", ");
      ctx.visit(setWindowFields);
    }

    // Render $graphLookup result columns only if included in $project
    for (GraphLookupStage graphLookup : components.graphLookupStages) {
      String asField = graphLookup.getAs();
      // Only include if project actually wants this field (direct inclusion or transformation)
      if (projectIncludedComputedFields.contains(asField)
          && !projectTransformedFields.contains(asField)) {
        ctx.sql(", ");
        ctx.identifier(asField + "_cte");
        ctx.sql(".");
        ctx.identifier(asField);
        ctx.sql(" AS ");
        ctx.identifier(asField);
      }
      if (graphLookup.getDepthField() != null) {
        String depthField = graphLookup.getDepthField();
        if (projectIncludedComputedFields.contains(depthField)
            && !projectTransformedFields.contains(depthField)) {
          ctx.sql(", ");
          ctx.identifier(asField + "_cte");
          ctx.sql(".");
          ctx.identifier(depthField);
          ctx.sql(" AS ");
          ctx.identifier(depthField);
        }
      }
    }

    // FROM clause
    renderFromClause(components, ctx);

    // JOIN clauses ($lookup stages)
    renderJoinClauses(components, ctx);

    // $graphLookup joins
    renderGraphLookupJoins(components, ctx);

    // WHERE clause
    renderWhereClause(components, ctx);

    // ORDER BY clause
    renderOrderByClause(components, ctx);

    // OFFSET clause
    renderOffsetClause(components, ctx);

    // FETCH clause
    renderFetchClause(components, ctx);

    // Close subquery
    ctx.sql(") sub");
  }

  /**
   * Renders a $count stage using a subquery pattern to avoid Oracle's nested aggregate error
   * (ORA-00978). The pattern wraps the COUNT(*) in a subquery and aggregates the result:
   *
   * <pre>
   * SELECT JSON_ARRAYAGG(JSON_OBJECT('fieldName' VALUE cnt) RETURNING CLOB)
   * FROM (
   *   SELECT COUNT(*) AS cnt
   *   FROM table base
   *   WHERE ...
   * )
   * </pre>
   */
  private void renderCountWithSubquery(PipelineComponents components, SqlGenerationContext ctx) {
    CountStage count = components.countStage;

    // Outer query: wrap count result with JSON_ARRAYAGG
    ctx.sql("SELECT JSON_ARRAYAGG(JSON_OBJECT('");
    ctx.sql(count.getFieldName());
    ctx.sql("' VALUE cnt) RETURNING CLOB) FROM (");

    // Inner query: SELECT COUNT(*)
    ctx.sql("SELECT COUNT(*) AS cnt");

    // FROM clause
    renderFromClause(components, ctx);

    // JOIN clauses ($lookup stages) - rare but possible before $count
    renderJoinClauses(components, ctx);

    // WHERE clause (combined $match stages)
    renderWhereClause(components, ctx);

    // Close subquery (no alias needed)
    ctx.sql(")");
  }

  /**
   * Renders a query where $match follows $setWindowFields and filters on window results. Wraps the
   * window query as a subquery so we can filter on the computed window columns.
   */
  private void renderWithPostWindowMatch(PipelineComponents components, SqlGenerationContext ctx) {
    // Collect window field names for the project rendering
    Set<String> windowFields = new HashSet<>();
    for (SetWindowFieldsStage setWindowFields : components.setWindowFieldsStages) {
      windowFields.addAll(setWindowFields.getOutput().keySet());
    }

    // Outer SELECT: select columns that the outer query needs
    // If there's a $project, use those fields; otherwise select all
    if (components.projectStage != null) {
      // Render project fields accessing data from inner query (no table alias needed)
      renderPostWindowProjectSelect(components.projectStage, windowFields, ctx);
    } else {
      ctx.sql("SELECT *");
    }

    // FROM subquery containing the window functions
    ctx.sql(" FROM (SELECT ");

    // Inner query: id and data columns (without alias prefix so they're available to outer query)
    ctx.sql("id, ");
    ctx.sql(config.dataColumnName());

    // Add window function columns
    for (SetWindowFieldsStage setWindowFields : components.setWindowFieldsStages) {
      ctx.sql(", ");
      ctx.visit(setWindowFields);
    }

    // FROM clause
    ctx.sql(" FROM ");
    ctx.tableName(components.collectionName);
    String baseAlias = ctx.getBaseTableAlias();
    if (baseAlias != null && !baseAlias.isEmpty()) {
      ctx.sql(" ");
      ctx.sql(baseAlias);
    }

    // Pre-window WHERE clause (if any)
    if (!components.matchStages.isEmpty()) {
      ctx.sql(" WHERE ");
      boolean first = true;
      for (MatchStage match : components.matchStages) {
        if (!first) {
          ctx.sql(" AND ");
        }
        ctx.visit(match.getFilter());
        first = false;
      }
    }

    ctx.sql(") w"); // Close subquery with alias for dot notation

    // Post-window WHERE clause (filter on window results)
    if (!components.postWindowMatchStages.isEmpty()) {
      ctx.sql(" WHERE ");
      boolean first = true;
      for (MatchStage match : components.postWindowMatchStages) {
        if (!first) {
          ctx.sql(" AND ");
        }
        renderPostWindowMatchExpression(match.getFilter(), ctx);
        first = false;
      }
    }

    // ORDER BY clause - use no table alias since we're querying the subquery
    renderPostWindowOrderByClause(components, windowFields, ctx);

    // OFFSET clause
    renderOffsetClause(components, ctx);

    // FETCH clause
    renderFetchClause(components, ctx);
  }

  /**
   * Renders the outer SELECT for a post-window match query based on the $project stage. Uses dot
   * notation to access fields from the subquery result for type preservation.
   */
  private void renderPostWindowProjectSelect(
      ProjectStage project, Set<String> windowFields, SqlGenerationContext ctx) {
    ctx.sql("SELECT ");
    boolean first = true;
    for (Map.Entry<String, ProjectStage.ProjectionField> entry :
        project.getProjections().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      String fieldAlias = entry.getKey();

      // Handle _id specially - reference the id column directly
      if ("_id".equals(fieldAlias)) {
        ctx.sql("id");
      } else if (windowFields.contains(fieldAlias)) {
        // Window field - reference the column directly from the subquery
        ctx.sql(fieldAlias);
      } else {
        // Regular data field - use dot notation with subquery alias for type preservation
        ctx.sql("w.");
        ctx.sql(config.dataColumnName());
        ctx.sql(".");
        ctx.sql(quotePath(fieldAlias));
      }
      ctx.sql(" AS ");
      ctx.identifier(fieldAlias);
      first = false;
    }
  }

  /**
   * Renders ORDER BY for post-window match queries. Window fields are referenced directly, while
   * data fields use dot notation for type preservation.
   */
  private void renderPostWindowOrderByClause(
      PipelineComponents components, Set<String> windowFields, SqlGenerationContext ctx) {
    if (components.sortStage == null || components.sortStage.getSortFields().isEmpty()) {
      return;
    }

    ctx.sql(" ORDER BY ");
    boolean first = true;
    for (SortStage.SortField sortField : components.sortStage.getSortFields()) {
      if (!first) {
        ctx.sql(", ");
      }
      String fieldPath = sortField.getFieldPath().getPath();

      if (windowFields.contains(fieldPath)) {
        // Window field - reference the column directly
        ctx.sql(fieldPath);
      } else {
        // Data field - use dot notation with subquery alias for type preservation
        ctx.sql("w.");
        ctx.sql(config.dataColumnName());
        ctx.sql(".");
        ctx.sql(quotePath(fieldPath));
      }
      if (sortField.getDirection() == SortStage.SortDirection.DESC) {
        ctx.sql(" DESC");
      }
      first = false;
    }
  }

  /**
   * Renders a match expression for post-window filtering. Window field references become direct
   * column references (no JSON_VALUE needed).
   */
  private void renderPostWindowMatchExpression(Expression expr, SqlGenerationContext ctx) {
    if (expr instanceof ComparisonExpression comp) {
      Expression left = comp.getLeft();
      final Expression right = comp.getRight();

      // For window field references, just use the field name directly
      if (left instanceof FieldPathExpression fieldPath) {
        ctx.sql(fieldPath.getPath());
      } else {
        ctx.visit(left);
      }

      ctx.sql(" ");
      ctx.sql(comp.getOp().getSqlOperator());
      ctx.sql(" ");

      if (right instanceof LiteralExpression lit) {
        ctx.visit(lit);
      } else {
        // FieldPathExpression and other expression types
        ctx.visit(right);
      }
    } else {
      // For other expressions, visit normally
      ctx.visit(expr);
    }
  }

  /**
   * Renders a query where $addFields follows $group. Wraps the GROUP query as a subquery and
   * applies $addFields to the result.
   */
  private void renderWithPostGroupAddFields(
      PipelineComponents components, SqlGenerationContext ctx) {
    // Check if we need a 3-level query for $project after $addFields
    boolean needsThreeLevels = components.projectStage != null;

    if (needsThreeLevels) {
      // Level 3 (outer): $project transformations
      renderPostGroupProjectSelect(components.projectStage, components.groupStage, ctx);
      ctx.sql(" FROM (");
    }

    // Level 2 (middle) or Level 1 if no project: $addFields computed columns
    ctx.sql("SELECT inner_query.*");

    // Render post-group $addFields computed columns
    for (AddFieldsStage addFields : components.postGroupAddFieldsStages) {
      for (var entry : addFields.getFields().entrySet()) {
        ctx.sql(", ");
        // Render expression, but field references should resolve to inner query columns
        renderPostGroupExpression(entry.getValue(), ctx);
        ctx.sql(" AS ");
        ctx.identifier(entry.getKey());
      }
    }

    // FROM subquery
    ctx.sql(" FROM (");

    // Level 1 (inner): the GROUP BY query
    renderSelectClause(components, ctx);
    renderFromClause(components, ctx);
    renderJoinClauses(components, ctx);
    renderGraphLookupJoins(components, ctx);
    renderWhereClause(components, ctx);
    renderGroupByClause(components, ctx);

    ctx.sql(") inner_query");

    if (needsThreeLevels) {
      ctx.sql(") addfields_query");
    }

    // ORDER BY, OFFSET, FETCH apply to the outer query
    renderOrderByClauseForOuterQuery(components, ctx);
    renderOffsetClause(components, ctx);
    renderFetchClause(components, ctx);
  }

  /**
   * Renders a query with $setWindowFields after $group. Window functions cannot reference aliases
   * created in the same SELECT, so we need a two-level query pattern:
   *
   * <pre>
   * SELECT inner_query.*,
   *        RANK() OVER (ORDER BY totalSales DESC) AS salesRank
   * FROM (
   *   SELECT region AS "_id", SUM(amount) AS totalSales, COUNT(*) AS orderCount
   *   FROM sales GROUP BY region
   * ) inner_query
   * ORDER BY salesRank
   * </pre>
   */
  private void renderWithPostGroupSetWindowFields(
      PipelineComponents components, SqlGenerationContext ctx) {
    // If there are post-group $addFields that may reference window function results,
    // we need a 3-level query:
    // Level 3 (outer): $addFields expressions referencing window function results
    // Level 2 (middle): window functions
    // Level 1 (inner): GROUP BY query
    boolean needsThreeLevels = !components.postGroupAddFieldsStages.isEmpty();

    if (needsThreeLevels) {
      // Level 3: Outermost SELECT with $addFields expressions
      ctx.sql("SELECT window_query.*");

      // Render post-group $addFields computed columns
      for (AddFieldsStage addFields : components.postGroupAddFieldsStages) {
        for (var entry : addFields.getFields().entrySet()) {
          ctx.sql(", ");
          renderPostGroupExpression(entry.getValue(), ctx);
          ctx.sql(" AS ");
          ctx.identifier(entry.getKey());
        }
      }

      ctx.sql(" FROM (");

      // Level 2: Window functions query
      ctx.sql("SELECT inner_query.*");

      // Render window functions with direct column references
      for (SetWindowFieldsStage swf : components.postGroupSetWindowFieldsStages) {
        renderPostGroupWindowFunctions(swf, ctx);
      }

      ctx.sql(" FROM (");

      // Level 1: GROUP BY query
      renderSelectClause(components, ctx);
      renderFromClause(components, ctx);
      renderJoinClauses(components, ctx);
      renderGraphLookupJoins(components, ctx);
      renderWhereClause(components, ctx);
      renderGroupByClause(components, ctx);

      ctx.sql(") inner_query) window_query");
    } else {
      // Two-level query (no $addFields after window functions)
      ctx.sql("SELECT inner_query.*");

      // Render window functions with direct column references (not base.data.*)
      for (SetWindowFieldsStage swf : components.postGroupSetWindowFieldsStages) {
        renderPostGroupWindowFunctions(swf, ctx);
      }

      // FROM subquery
      ctx.sql(" FROM (");

      // Inner query: the GROUP BY query
      renderSelectClause(components, ctx);
      renderFromClause(components, ctx);
      renderJoinClauses(components, ctx);
      renderGraphLookupJoins(components, ctx);
      renderWhereClause(components, ctx);
      renderGroupByClause(components, ctx);

      ctx.sql(") inner_query");
    }

    // ORDER BY, OFFSET, FETCH apply to the outer query
    renderOrderByClauseForOuterQuery(components, ctx);
    renderOffsetClause(components, ctx);
    renderFetchClause(components, ctx);
  }

  /**
   * Renders window functions for $setWindowFields after $group, using direct column references
   * instead of base.data.* paths because the columns are computed by GROUP BY.
   */
  private void renderPostGroupWindowFunctions(
      SetWindowFieldsStage swf, SqlGenerationContext ctx) {
    for (Map.Entry<String, SetWindowFieldsStage.WindowField> entry : swf.getOutput().entrySet()) {
      ctx.sql(", ");
      renderPostGroupWindowFunction(entry.getValue(), swf.getSortBy(), ctx);
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
    }
  }

  /**
   * Renders a single window function with direct column references for ORDER BY and aggregate
   * arguments.
   */
  private void renderPostGroupWindowFunction(
      SetWindowFieldsStage.WindowField field,
      Map<String, Integer> sortBy,
      SqlGenerationContext ctx) {
    String op = field.operator();

    // Map MongoDB window operators to Oracle
    switch (op) {
      case "$sum" -> {
        ctx.sql("SUM(");
        renderPostGroupFieldPath(field.argument(), ctx);
        ctx.sql(")");
      }
      case "$avg" -> {
        ctx.sql("AVG(");
        renderPostGroupFieldPath(field.argument(), ctx);
        ctx.sql(")");
      }
      case "$min" -> {
        ctx.sql("MIN(");
        renderPostGroupFieldPath(field.argument(), ctx);
        ctx.sql(")");
      }
      case "$max" -> {
        ctx.sql("MAX(");
        renderPostGroupFieldPath(field.argument(), ctx);
        ctx.sql(")");
      }
      case "$count" -> ctx.sql("COUNT(*)");
      case "$rank" -> ctx.sql("RANK()");
      case "$denseRank" -> ctx.sql("DENSE_RANK()");
      case "$rowNumber", "$documentNumber" -> ctx.sql("ROW_NUMBER()");
      case "$first" -> {
        ctx.sql("FIRST_VALUE(");
        renderPostGroupFieldPath(field.argument(), ctx);
        ctx.sql(")");
      }
      case "$last" -> {
        ctx.sql("LAST_VALUE(");
        renderPostGroupFieldPath(field.argument(), ctx);
        ctx.sql(")");
      }
      default -> {
        ctx.sql("/* unsupported: ");
        ctx.sql(op);
        ctx.sql(" */ NULL");
      }
    }

    // Add OVER clause with direct column references
    ctx.sql(" OVER (");
    renderPostGroupOverClause(sortBy, field.window(), ctx);
    ctx.sql(")");
  }

  /** Renders field path for window functions after GROUP BY - uses direct column name. */
  private void renderPostGroupFieldPath(String fieldPath, SqlGenerationContext ctx) {
    if (fieldPath == null) {
      ctx.sql("1");
      return;
    }
    // Remove $ prefix and use direct column name
    String columnName = fieldPath.startsWith("$") ? fieldPath.substring(1) : fieldPath;
    ctx.sql(columnName);
  }

  /** Renders OVER clause with direct column references for post-group window functions. */
  private void renderPostGroupOverClause(
      Map<String, Integer> sortBy,
      SetWindowFieldsStage.WindowSpec window,
      SqlGenerationContext ctx) {
    boolean hasClause = false;

    // ORDER BY clause with direct column references
    if (sortBy != null && !sortBy.isEmpty()) {
      ctx.sql("ORDER BY ");
      boolean firstSort = true;
      for (Map.Entry<String, Integer> sortEntry : sortBy.entrySet()) {
        if (!firstSort) {
          ctx.sql(", ");
        }
        // Use direct column name, not JSON path
        ctx.sql(sortEntry.getKey());
        if (sortEntry.getValue() < 0) {
          ctx.sql(" DESC");
        }
        firstSort = false;
      }
      hasClause = true;
    }

    // Window frame clause
    if (window != null && window.bounds() != null && !window.bounds().isEmpty()) {
      if (hasClause) {
        ctx.sql(" ");
      }
      renderPostGroupWindowFrame(window, ctx);
    }
  }

  /** Renders window frame for post-group window functions. */
  private void renderPostGroupWindowFrame(SetWindowFieldsStage.WindowSpec window,
      SqlGenerationContext ctx) {
    String type = window.type();
    List<String> bounds = window.bounds();

    // Determine frame type (ROWS or RANGE)
    if ("documents".equals(type)) {
      ctx.sql("ROWS BETWEEN ");
    } else if ("range".equals(type)) {
      ctx.sql("RANGE BETWEEN ");
    } else {
      ctx.sql("ROWS BETWEEN ");
    }

    // Lower bound
    if (bounds.size() >= 1) {
      renderWindowBound(bounds.get(0), false, ctx);
    } else {
      ctx.sql("UNBOUNDED PRECEDING");
    }

    ctx.sql(" AND ");

    // Upper bound
    if (bounds.size() >= 2) {
      renderWindowBound(bounds.get(1), true, ctx);
    } else {
      ctx.sql("CURRENT ROW");
    }
  }

  /** Renders a window frame bound. */
  private void renderWindowBound(String bound, boolean isUpperBound, SqlGenerationContext ctx) {
    if (bound == null || "unbounded".equals(bound)) {
      ctx.sql(isUpperBound ? "UNBOUNDED FOLLOWING" : "UNBOUNDED PRECEDING");
    } else if ("current".equals(bound)) {
      ctx.sql("CURRENT ROW");
    } else {
      try {
        int value = Integer.parseInt(bound);
        if (value < 0) {
          ctx.sql(String.valueOf(-value));
          ctx.sql(" PRECEDING");
        } else if (value > 0) {
          ctx.sql(String.valueOf(value));
          ctx.sql(" FOLLOWING");
        } else {
          ctx.sql("CURRENT ROW");
        }
      } catch (NumberFormatException e) {
        ctx.sql(isUpperBound ? "UNBOUNDED FOLLOWING" : "UNBOUNDED PRECEDING");
      }
    }
  }

  /**
   * Renders a query with $bucketAuto. Uses NTILE to divide documents into N buckets, then computes
   * boundaries using LEAD() to match MongoDB's boundary semantics where max = next bucket's min.
   *
   * <p>Note: NTILE distributes rows evenly by position, which may differ from MongoDB's $bucketAuto
   * algorithm that considers value distribution. The bucket assignments may vary, but the boundary
   * semantics (max = next bucket's min) will match MongoDB's output format.
   *
   * <pre>
   * WITH buckets AS (
   *   SELECT bucket_id, MIN(val) AS bucket_min, MAX(val) AS bucket_max, COUNT(*), AVG(val)...
   *   FROM (SELECT field AS val, NTILE(n) OVER (ORDER BY field) AS bucket_id FROM table)
   *   GROUP BY bucket_id
   * )
   * SELECT JSON_OBJECT('min' VALUE bucket_min,
   *                    'max' VALUE NVL(LEAD(bucket_min) OVER (ORDER BY bucket_id), bucket_max))
   *        AS "_id", ...
   * FROM buckets ORDER BY bucket_id
   * </pre>
   */
  private void renderWithBucketAuto(PipelineComponents components, SqlGenerationContext ctx) {
    BucketAutoStage bucketAuto = components.bucketAutoStage;

    // CTE to compute bucket aggregations
    ctx.sql("WITH buckets AS (SELECT bucket_id, MIN(groupby_value) AS bucket_min, ");
    ctx.sql("MAX(groupby_value) AS bucket_max");

    // Render output accumulators in CTE
    for (Map.Entry<String, AccumulatorExpression> entry : bucketAuto.getOutput().entrySet()) {
      ctx.sql(", ");
      String alias = entry.getKey();
      AccumulatorExpression acc = entry.getValue();

      switch (acc.getOp()) {
        case SUM:
          if (acc.getArgument() instanceof LiteralExpression lit
              && lit.getValue() instanceof Number n
              && n.intValue() == 1) {
            ctx.sql("COUNT(*)");
          } else {
            ctx.sql("SUM(groupby_value)");
          }
          break;
        case AVG:
          ctx.sql("AVG(groupby_value)");
          break;
        case MIN:
          ctx.sql("MIN(groupby_value)");
          break;
        case MAX:
          ctx.sql("MAX(groupby_value)");
          break;
        case COUNT:
          ctx.sql("COUNT(*)");
          break;
        default:
          ctx.sql("/* unsupported: ");
          ctx.sql(acc.getOp().name());
          ctx.sql(" */ NULL");
      }
      ctx.sql(" AS ");
      ctx.identifier(alias);
    }

    // Inner subquery with NTILE
    ctx.sql(" FROM (SELECT ");
    ctx.visit(bucketAuto.getGroupBy());
    ctx.sql(" AS groupby_value, NTILE(");
    ctx.sql(String.valueOf(bucketAuto.getBuckets()));
    ctx.sql(") OVER (ORDER BY ");
    ctx.visit(bucketAuto.getGroupBy());
    ctx.sql(") AS bucket_id FROM ");
    ctx.tableName(components.collectionName);
    String baseAlias = ctx.getBaseTableAlias();
    if (baseAlias != null && !baseAlias.isEmpty()) {
      ctx.sql(" ");
      ctx.sql(baseAlias);
    }

    // Render WHERE clause if there are match stages
    if (!components.matchStages.isEmpty()) {
      ctx.sql(" WHERE ");
      boolean first = true;
      for (MatchStage match : components.matchStages) {
        if (!first) {
          ctx.sql(" AND ");
        }
        ctx.visit(match.getFilter());
        first = false;
      }
    }

    ctx.sql(") GROUP BY bucket_id) "); // Close CTE

    // Outer SELECT with LEAD() for proper boundary calculation
    // MongoDB's max = next bucket's min (or actual max for last bucket)
    ctx.sql("SELECT JSON_OBJECT('min' VALUE bucket_min, ");
    ctx.sql("'max' VALUE NVL(LEAD(bucket_min) OVER (ORDER BY bucket_id), bucket_max)) AS ");
    ctx.identifier("_id");

    // Project the accumulator columns from CTE
    for (Map.Entry<String, AccumulatorExpression> entry : bucketAuto.getOutput().entrySet()) {
      ctx.sql(", ");
      ctx.identifier(entry.getKey());
    }

    ctx.sql(" FROM buckets ORDER BY bucket_id");
  }

  /**
   * Renders a pipeline with multiple $group stages using CTEs (Common Table Expressions). Each
   * group stage before the last becomes a CTE, allowing subsequent stages to reference the
   * grouped results.
   *
   * <pre>
   * WITH cte_group_0 AS (
   *   SELECT category AS "_id", SUM(amount) AS totalRevenue
   *   FROM orders base
   *   GROUP BY category
   * )
   * SELECT region AS "_id", SUM(totalRevenue) AS grandTotal
   * FROM cte_group_0
   * GROUP BY region
   * </pre>
   */
  private void renderWithMultiGroupCtes(
      Pipeline pipeline, PipelineComponents components, SqlGenerationContext ctx) {
    PipelineStageSequence sequence = components.stageSequence;
    List<PipelineStageSequence.StageGroup> stageGroups = sequence.getStageGroups();

    // Render WITH clause
    ctx.sql("WITH ");

    // Track the source table for each CTE (starts with base collection, then CTE names)
    String currentSource = components.collectionName;
    String currentAlias = "base";
    // Track compound _id fields from previous group for field path resolution
    GroupStage previousGroupStage = null;

    // Render each CTE (all groups except the last one)
    for (int i = 0; i < stageGroups.size() - 1; i++) {
      PipelineStageSequence.StageGroup group = stageGroups.get(i);
      if (i > 0) {
        ctx.sql(", ");
      }

      String cteName = group.getCteName();
      ctx.sql(cteName);
      ctx.sql(" AS (");

      // Create a fresh context for this CTE
      // Only the first CTE (i=0) queries from the base table with JOINs
      // Subsequent CTEs query from previous CTEs and use plain column names
      // Propagate inlineValues setting from parent context
      DefaultSqlGenerationContext cteCtx =
          new DefaultSqlGenerationContext(ctx.inline(), null, currentAlias);
      cteCtx.setInCteContext(i > 0); // Only true when referencing previous CTE
      cteCtx.setCteSourceTable(currentSource);

      // If referencing previous CTE with compound _id, register those fields
      if (i > 0 && previousGroupStage != null) {
        cteCtx.registerCompoundIdFields(getCompoundIdFields(previousGroupStage));
      }

      // Extract stages from this group: GroupStage, LookupStage, UnwindStage, and MatchStage
      GroupStage groupStage = null;
      List<LookupStage> lookupStages = new ArrayList<>();
      List<UnwindStage> unwindStages = new ArrayList<>();
      List<MatchStage> matchStages = new ArrayList<>();
      for (Stage stage : group.getStages()) {
        if (stage instanceof GroupStage gs) {
          groupStage = gs;
        } else if (stage instanceof LookupStage ls) {
          lookupStages.add(ls);
        } else if (stage instanceof UnwindStage us) {
          unwindStages.add(us);
        } else if (stage instanceof MatchStage ms) {
          matchStages.add(ms);
        }
      }

      // Register unwind paths with context for field path resolution
      for (UnwindStage unwind : unwindStages) {
        String alias = cteCtx.generateTableAlias("unwind");
        cteCtx.registerUnwoundPath(unwind.getPath(), alias);
      }

      // Register lookup fields with context for field path resolution
      for (LookupStage lookup : lookupStages) {
        if (!lookup.isPipelineForm()) {
          cteCtx.registerLookupField(
              lookup.getAs(),
              lookup.getFrom(),
              lookup.getLocalField(),
              lookup.getForeignField());
          // Generate table alias and register for field path resolution
          String alias = cteCtx.generateTableAlias(lookup.getFrom());
          cteCtx.registerLookupTableAlias(lookup.getAs(), alias);
        }
      }

      if (groupStage != null) {
        // Render SELECT clause for this group
        cteCtx.sql("SELECT ");
        renderGroupSelectClause(groupStage, cteCtx);

        // Render FROM clause
        cteCtx.sql(" FROM ");
        cteCtx.sql(currentSource);
        cteCtx.sql(" ");
        cteCtx.sql(currentAlias);

        // Render CROSS APPLY for any $unwind stages in this group
        // Skip unwinds on lookup aliases - they don't need JSON_TABLE
        for (UnwindStage unwind : unwindStages) {
          // Check if this unwind path matches a lookup alias
          boolean isLookupAlias = false;
          for (LookupStage lookup : lookupStages) {
            if (unwind.getPath().equals(lookup.getAs())) {
              isLookupAlias = true;
              break;
            }
          }
          if (isLookupAlias) {
            // Skip - $lookup JOIN already provides the data
            continue;
          }
          if (unwind.isPreserveNullAndEmptyArrays()) {
            cteCtx.sql(" LEFT OUTER JOIN LATERAL ");
          } else {
            cteCtx.sql(" CROSS APPLY ");
          }
          cteCtx.visit(unwind);
        }

        // Render JOIN clauses for any $lookup stages in this group
        for (LookupStage lookup : lookupStages) {
          cteCtx.sql(" ");
          cteCtx.visit(lookup);
        }

        // Render WHERE clause for any $match stages in this group
        if (!matchStages.isEmpty()) {
          cteCtx.sql(" WHERE ");
          boolean firstMatch = true;
          for (MatchStage match : matchStages) {
            if (!firstMatch) {
              cteCtx.sql(" AND ");
            }
            match.getFilter().render(cteCtx);
            firstMatch = false;
          }
        }

        // Render GROUP BY clause
        renderCteGroupByClause(groupStage, cteCtx);
      }

      ctx.sql(cteCtx.toSql());
      ctx.sql(")");

      // Update source for next iteration
      currentSource = cteName;
      currentAlias = cteName;
      // Save group stage for compound _id field resolution in next CTE
      previousGroupStage = groupStage;
    }

    // Render final query that references the last CTE
    ctx.sql(" ");

    // Get the final stage group
    PipelineStageSequence.StageGroup finalGroup = stageGroups.get(stageGroups.size() - 1);

    // Extract the GroupStage and ProjectStage from the final group
    GroupStage finalGroupStage = null;
    ProjectStage finalProjectStage = null;
    for (Stage stage : finalGroup.getStages()) {
      if (stage instanceof GroupStage gs) {
        finalGroupStage = gs;
      } else if (stage instanceof ProjectStage ps) {
        finalProjectStage = ps;
      }
    }

    // Create context for final query - this references CTE columns, not JSON paths
    // Propagate inlineValues setting from parent context
    DefaultSqlGenerationContext finalCtx =
        new DefaultSqlGenerationContext(ctx.inline(), null, currentAlias);
    finalCtx.setInCteContext(true);
    finalCtx.setCteSourceTable(currentSource);

    // Register compound _id fields from last CTE's group stage for field path resolution
    if (previousGroupStage != null) {
      finalCtx.registerCompoundIdFields(getCompoundIdFields(previousGroupStage));
    }

    if (finalGroupStage != null) {
      if (finalProjectStage != null) {
        // $group followed by $project: render $project fields from CTE/subquery
        // We need a subquery: SELECT project_fields FROM (SELECT group_fields ... GROUP BY ...)
        renderGroupWithProject(
            finalGroupStage, finalProjectStage, currentSource, currentAlias, finalCtx);
      } else {
        // Just $group: render as before
        finalCtx.sql("SELECT ");
        renderGroupSelectClause(finalGroupStage, finalCtx);
        finalCtx.sql(" FROM ");
        finalCtx.sql(currentSource);
        renderCteGroupByClause(finalGroupStage, finalCtx);
      }
    } else if (finalProjectStage != null) {
      // Just $project (no $group in final stage): render project fields from CTE
      finalCtx.sql("SELECT ");
      renderCteProjectSelectClause(finalProjectStage, finalCtx);
      finalCtx.sql(" FROM ");
      finalCtx.sql(currentSource);
    }

    ctx.sql(finalCtx.toSql());
  }

  /**
   * Renders a $group followed by $project in the final query.
   * Creates a subquery for the $group and applies $project on top.
   */
  private void renderGroupWithProject(
      GroupStage group,
      ProjectStage project,
      String sourceTable,
      String sourceAlias,
      SqlGenerationContext ctx) {
    // SELECT project_fields FROM (SELECT group_fields FROM source GROUP BY ...) inner_query
    ctx.sql("SELECT ");
    renderCteProjectSelectClause(project, ctx);
    ctx.sql(" FROM (SELECT ");
    renderGroupSelectClause(group, ctx);
    ctx.sql(" FROM ");
    ctx.sql(sourceTable);
    ctx.sql(" ");
    ctx.sql(sourceAlias);
    renderCteGroupByClause(group, ctx);
    ctx.sql(") inner_query");
  }

  /**
   * Renders the SELECT clause fields for a project stage in CTE context.
   * Note: Does NOT output "SELECT " - that's handled by the caller.
   */
  private void renderCteProjectSelectClause(ProjectStage project, SqlGenerationContext ctx) {
    boolean first = true;
    for (Map.Entry<String, ProjectStage.ProjectionField> entry :
        project.getProjections().entrySet()) {
      String fieldName = entry.getKey();
      ProjectStage.ProjectionField field = entry.getValue();

      // Skip excluded fields (_id: 0)
      if (field.isExcluded()) {
        continue;
      }

      if (!first) {
        ctx.sql(", ");
      }

      Expression expr = field.getExpression();
      if (expr != null) {
        expr.render(ctx);
      } else {
        // Simple field inclusion - reference from CTE
        ctx.sql(fieldName);
      }
      ctx.sql(" AS ");
      ctx.sql(fieldName);
      first = false;
    }

    // If no fields were rendered (all excluded), output a dummy
    if (first) {
      ctx.sql("NULL AS dummy");
    }
  }

  /**
   * Renders the SELECT clause fields for a group stage.
   * Note: Does NOT output "SELECT " - that's handled by the caller.
   */
  private void renderGroupSelectClause(GroupStage group, SqlGenerationContext ctx) {
    boolean first = true;

    // Render _id field
    Expression idExpr = group.getIdExpression();
    if (idExpr != null && !(idExpr instanceof LiteralExpression lit && lit.getValue() == null)) {
      // Compound _id: use renderWithAliases() to get "expr AS field1, expr AS field2, ..."
      // Simple _id: use render() and add AS "_id" suffix
      if (idExpr instanceof CompoundIdExpression compound) {
        compound.renderWithAliases(ctx);
      } else {
        idExpr.render(ctx);
        ctx.sql(" AS \"_id\"");
      }
      first = false;
    }

    // Render accumulator fields
    // Only quote aliases when this is the final output (isJsonOutputMode = true)
    // because intermediate GROUP BY aliases shouldn't be quoted (breaks outer references)
    for (Map.Entry<String, AccumulatorExpression> entry : group.getAccumulators().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      entry.getValue().render(ctx);
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
      first = false;
    }

    // If no _id and no accumulators, output a dummy column (Oracle needs at least one column)
    if (first) {
      ctx.sql("NULL AS dummy");
    }
  }

  /**
   * Renders the GROUP BY clause for a group stage in CTE context.
   */
  private void renderCteGroupByClause(GroupStage group, SqlGenerationContext ctx) {
    Expression idExpr = group.getIdExpression();
    if (idExpr != null && !(idExpr instanceof LiteralExpression lit && lit.getValue() == null)) {
      ctx.sql(" GROUP BY ");
      idExpr.render(ctx);
    }
  }

  /**
   * Renders a query with $out stage. The $out stage changes the query from SELECT to INSERT INTO
   * ... SELECT pattern, writing the aggregation results to the target collection.
   *
   * <pre>
   * INSERT INTO targetCollection (data)
   * SELECT ... FROM sourceCollection WHERE ... GROUP BY ... ORDER BY ...
   * </pre>
   */
  private void renderWithOutStage(
      Pipeline pipeline, PipelineComponents components, SqlGenerationContext ctx) {
    OutStage outStage = components.outStage;

    // Validate target table name
    FieldNameValidator.validateTableName(outStage.getTargetCollection());

    // Render INSERT INTO clause
    ctx.sql("INSERT INTO ");
    if (outStage.hasTargetDatabase()) {
      FieldNameValidator.validateTableName(outStage.getTargetDatabase());
      ctx.sql(outStage.getTargetDatabase());
      ctx.sql(".");
    }
    ctx.sql(outStage.getTargetCollection());
    ctx.sql(" (");
    ctx.sql(config.dataColumnName());
    ctx.sql(") ");

    // Clear the $out from components so it doesn't interfere with SELECT rendering
    components.outStage = null;

    // Now render the SELECT query using the standard flow
    // We need to re-register virtual fields and aliases since the context is shared
    for (AddFieldsStage addFields : components.addFieldsStages) {
      for (var entry : addFields.getFields().entrySet()) {
        ctx.registerVirtualField(entry.getKey(), entry.getValue());
      }
    }

    for (LookupStage lookup : components.lookupStages) {
      if (!lookup.isPipelineForm()) {
        ctx.registerLookupField(
            lookup.getAs(), lookup.getFrom(), lookup.getLocalField(), lookup.getForeignField());
        String alias = ctx.generateTableAlias(lookup.getFrom());
        ctx.registerLookupTableAlias(lookup.getAs(), alias);
      }
    }

    for (UnwindStage unwind : components.unwindStages) {
      if (!isUnwindOnLookupField(unwind.getPath(), components)) {
        String alias = ctx.generateTableAlias("unwind");
        ctx.registerUnwoundPath(unwind.getPath(), alias);

        // Register includeArrayIndex field as virtual field
        String indexField = unwind.getIncludeArrayIndex();
        if (indexField != null) {
          final String unwindAlias = alias;
          final String indexName = indexField;
          ctx.registerVirtualField(
              indexField,
              new Expression() {
                @Override
                public void render(SqlGenerationContext renderCtx) {
                  renderCtx.sql("(");
                  renderCtx.sql(unwindAlias);
                  renderCtx.sql(".");
                  renderCtx.sql(indexName);
                  renderCtx.sql(" - 1)");
                }
              });
        }
      }
    }

    // Render the SELECT part of the query
    if (components.hasPostUnionGroup) {
      renderWithPostUnionGroup(components, ctx);
    } else if (components.hasPostWindowMatch) {
      renderWithPostWindowMatch(components, ctx);
    } else if (components.hasPostGroupAddFields) {
      renderWithPostGroupAddFields(components, ctx);
    } else if (components.bucketAutoStage != null) {
      renderWithBucketAuto(components, ctx);
    } else {
      renderStandardQuery(components, ctx);
    }

    // Render UNION ALL clauses if present
    if (!components.hasPostUnionGroup) {
      renderUnionWithClauses(components, ctx);
      renderPostUnionSortAndLimit(components, ctx);
    }
  }

  /**
   * Renders a query with $merge stage. The $merge stage generates an Oracle MERGE statement that
   * matches documents based on the ON fields and applies whenMatched/whenNotMatched actions.
   *
   * <pre>
   * MERGE INTO targetCollection t
   * USING (SELECT ... FROM sourceCollection WHERE ...) s
   * ON (t."_id" = s."_id")
   * WHEN MATCHED THEN UPDATE SET t.data = s.data
   * WHEN NOT MATCHED THEN INSERT (data) VALUES (s.data)
   * </pre>
   */
  private void renderWithMergeStage(
      Pipeline pipeline, PipelineComponents components, SqlGenerationContext ctx) {
    MergeStage mergeStage = components.mergeStage;

    // Validate target table name
    FieldNameValidator.validateTableName(mergeStage.getTargetCollection());

    // Render MERGE INTO clause
    ctx.sql("MERGE INTO ");
    ctx.sql(mergeStage.getTargetCollection());
    ctx.sql(" t ");

    // Render USING subquery
    ctx.sql("USING (");

    // Clear the $merge from components so it doesn't interfere with SELECT rendering
    components.mergeStage = null;

    // Register virtual fields and aliases for the subquery
    for (AddFieldsStage addFields : components.addFieldsStages) {
      for (var entry : addFields.getFields().entrySet()) {
        ctx.registerVirtualField(entry.getKey(), entry.getValue());
      }
    }

    for (LookupStage lookup : components.lookupStages) {
      if (!lookup.isPipelineForm()) {
        ctx.registerLookupField(
            lookup.getAs(), lookup.getFrom(), lookup.getLocalField(), lookup.getForeignField());
        String alias = ctx.generateTableAlias(lookup.getFrom());
        ctx.registerLookupTableAlias(lookup.getAs(), alias);
      }
    }

    for (UnwindStage unwind : components.unwindStages) {
      if (!isUnwindOnLookupField(unwind.getPath(), components)) {
        String alias = ctx.generateTableAlias("unwind");
        ctx.registerUnwoundPath(unwind.getPath(), alias);

        // Register includeArrayIndex field as virtual field
        String indexField = unwind.getIncludeArrayIndex();
        if (indexField != null) {
          final String unwindAlias = alias;
          final String indexName = indexField;
          ctx.registerVirtualField(
              indexField,
              new Expression() {
                @Override
                public void render(SqlGenerationContext renderCtx) {
                  renderCtx.sql("(");
                  renderCtx.sql(unwindAlias);
                  renderCtx.sql(".");
                  renderCtx.sql(indexName);
                  renderCtx.sql(" - 1)");
                }
              });
        }
      }
    }

    // Render the SELECT part of the subquery
    if (components.hasPostUnionGroup) {
      renderWithPostUnionGroup(components, ctx);
    } else if (components.hasPostWindowMatch) {
      renderWithPostWindowMatch(components, ctx);
    } else if (components.hasPostGroupAddFields) {
      renderWithPostGroupAddFields(components, ctx);
    } else if (components.bucketAutoStage != null) {
      renderWithBucketAuto(components, ctx);
    } else {
      renderStandardQuery(components, ctx);
    }

    // Render UNION ALL clauses if present
    if (!components.hasPostUnionGroup) {
      renderUnionWithClauses(components, ctx);
      renderPostUnionSortAndLimit(components, ctx);
    }

    ctx.sql(") s ");

    // Render ON clause with matching fields
    ctx.sql("ON (");
    List<String> onFields = mergeStage.getOnFields();
    for (int i = 0; i < onFields.size(); i++) {
      if (i > 0) {
        ctx.sql(" AND ");
      }
      String field = onFields.get(i);
      // Use dot notation for type preservation in the ON clause
      ctx.sql("t.");
      ctx.sql(config.dataColumnName());
      ctx.sql(".");
      ctx.sql(quotePath(field));
      ctx.sql(" = s.");
      ctx.sql(config.dataColumnName());
      ctx.sql(".");
      ctx.sql(quotePath(field));
    }
    ctx.sql(") ");

    // Render WHEN MATCHED clause based on whenMatched option
    MergeStage.WhenMatched whenMatched = mergeStage.getWhenMatched();
    if (whenMatched != MergeStage.WhenMatched.KEEP_EXISTING
        && whenMatched != MergeStage.WhenMatched.FAIL) {
      ctx.sql("WHEN MATCHED THEN UPDATE SET t.");
      ctx.sql(config.dataColumnName());
      if (whenMatched == MergeStage.WhenMatched.MERGE) {
        // Use JSON_MERGEPATCH to merge the documents
        ctx.sql(" = JSON_MERGEPATCH(t.");
        ctx.sql(config.dataColumnName());
        ctx.sql(", s.");
        ctx.sql(config.dataColumnName());
        ctx.sql(")");
      } else {
        // REPLACE: Simply replace the whole document
        ctx.sql(" = s.");
        ctx.sql(config.dataColumnName());
      }
      ctx.sql(" ");
    }

    // Render WHEN NOT MATCHED clause based on whenNotMatched option
    MergeStage.WhenNotMatched whenNotMatched = mergeStage.getWhenNotMatched();
    if (whenNotMatched == MergeStage.WhenNotMatched.INSERT) {
      ctx.sql("WHEN NOT MATCHED THEN INSERT (");
      ctx.sql(config.dataColumnName());
      ctx.sql(") VALUES (s.");
      ctx.sql(config.dataColumnName());
      ctx.sql(")");
    }
  }

  /**
   * Renders a query where $group follows $unionWith. Wraps the entire UNION in a subquery so the
   * GROUP BY can aggregate over the complete union result.
   *
   * <pre>
   * SELECT aggregates FROM (
   *   SELECT ... FROM t1 WHERE ...
   *   UNION ALL
   *   SELECT ... FROM t2 WHERE ...
   * )
   * [GROUP BY ...]
   * </pre>
   */
  private void renderWithPostUnionGroup(PipelineComponents components, SqlGenerationContext ctx) {
    GroupStage group = components.postUnionGroupStage;

    // Outer SELECT: render the group accumulators
    ctx.sql("SELECT ");
    boolean first = true;

    // Render _id expression if present
    if (group.getIdExpression() != null) {
      renderPostUnionGroupIdExpression(group.getIdExpression(), ctx);
      ctx.sql(" AS ");
      ctx.identifier("_id");
      first = false;
    }

    // Render accumulators
    for (var entry : group.getAccumulators().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      renderPostUnionAccumulator(entry.getValue(), ctx);
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
      first = false;
    }

    // If nothing was rendered, select a placeholder
    if (first) {
      ctx.sql("NULL AS dummy");
    }

    // FROM subquery containing the UNION
    ctx.sql(" FROM (");

    // Render the pre-union query (the base query without the group)
    renderPreUnionSelectClause(components, ctx);
    renderFromClause(components, ctx);
    renderJoinClauses(components, ctx);
    renderWhereClause(components, ctx);

    // Render UNION ALL clauses
    renderUnionWithClauses(components, ctx);

    ctx.sql(")");

    // GROUP BY clause (if _id is not null)
    if (group.getIdExpression() != null) {
      ctx.sql(" GROUP BY ");
      renderPostUnionGroupIdExpression(group.getIdExpression(), ctx);
    }
  }

  /**
   * Renders the SELECT clause for the pre-union part when building a post-union group subquery. We
   * need to select the fields that the outer GROUP BY will reference.
   */
  private void renderPreUnionSelectClause(PipelineComponents components, SqlGenerationContext ctx) {
    ctx.sql("SELECT ");

    // If there's a project stage, use it
    if (components.projectStage != null) {
      boolean first = true;
      for (var entry : components.projectStage.getProjections().entrySet()) {
        final String alias = entry.getKey();
        ProjectStage.ProjectionField field = entry.getValue();

        if (field.isExcluded()) {
          continue;
        }

        if (!first) {
          ctx.sql(", ");
        }

        if (field.getExpression() != null) {
          ctx.visit(field.getExpression());
        }
        ctx.sql(" AS ");
        ctx.identifier(alias);
        first = false;
      }
      if (first) {
        ctx.sql("NULL AS dummy");
      }
    } else {
      // Default: select data column
      String baseAlias = ctx.getBaseTableAlias();
      if (baseAlias != null && !baseAlias.isEmpty()) {
        ctx.sql(baseAlias);
        ctx.sql(".");
      }
      ctx.sql(config.dataColumnName());
    }
  }

  /**
   * Renders the _id expression for post-union GROUP BY. Field paths should reference the column
   * aliases from the subquery.
   */
  private void renderPostUnionGroupIdExpression(Expression expr, SqlGenerationContext ctx) {
    if (expr instanceof FieldPathExpression fieldPath) {
      ctx.identifier(fieldPath.getPath());
    } else {
      ctx.visit(expr);
    }
  }

  /**
   * Renders an accumulator for post-union GROUP. Field paths reference column aliases from the
   * subquery.
   */
  private void renderPostUnionAccumulator(AccumulatorExpression acc, SqlGenerationContext ctx) {
    switch (acc.getOp()) {
      case SUM:
        if (acc.getArgument() instanceof LiteralExpression lit
            && lit.getValue() instanceof Number n
            && n.intValue() == 1) {
          ctx.sql("COUNT(*)");
        } else if (acc.getArgument() instanceof FieldPathExpression fieldPath) {
          ctx.sql("SUM(");
          ctx.identifier(fieldPath.getPath());
          ctx.sql(")");
        } else {
          ctx.sql("SUM(");
          ctx.visit(acc.getArgument());
          ctx.sql(")");
        }
        break;
      case AVG:
        if (acc.getArgument() instanceof FieldPathExpression fieldPath) {
          ctx.sql("AVG(");
          ctx.identifier(fieldPath.getPath());
          ctx.sql(")");
        } else {
          ctx.sql("AVG(");
          ctx.visit(acc.getArgument());
          ctx.sql(")");
        }
        break;
      case MIN:
        if (acc.getArgument() instanceof FieldPathExpression fieldPath) {
          ctx.sql("MIN(");
          ctx.identifier(fieldPath.getPath());
          ctx.sql(")");
        } else {
          ctx.sql("MIN(");
          ctx.visit(acc.getArgument());
          ctx.sql(")");
        }
        break;
      case MAX:
        if (acc.getArgument() instanceof FieldPathExpression fieldPath) {
          ctx.sql("MAX(");
          ctx.identifier(fieldPath.getPath());
          ctx.sql(")");
        } else {
          ctx.sql("MAX(");
          ctx.visit(acc.getArgument());
          ctx.sql(")");
        }
        break;
      case COUNT:
        ctx.sql("COUNT(*)");
        break;
      default:
        ctx.sql("/* unsupported: ");
        ctx.sql(acc.getOp().name());
        ctx.sql(" */ NULL");
    }
  }

  /**
   * Renders the SELECT clause for a $project stage after post-group $addFields. Field references
   * resolve to column names from the subquery, and expressions are rendered using the post-group
   * expression context.
   *
   * @param project the project stage to render
   * @param groupStage the group stage that precedes this project (for compound _id resolution)
   * @param ctx the SQL generation context
   */
  private void renderPostGroupProjectSelect(
      ProjectStage project, GroupStage groupStage, SqlGenerationContext ctx) {
    // Extract compound _id field names from the GroupStage for field path resolution
    Set<String> compoundIdFields = getCompoundIdFields(groupStage);

    ctx.sql("SELECT ");
    boolean first = true;
    for (Map.Entry<String, ProjectStage.ProjectionField> entry :
        project.getProjections().entrySet()) {
      String fieldAlias = entry.getKey();
      ProjectStage.ProjectionField projection = entry.getValue();

      // Skip excluded fields (_id: 0)
      if (projection.isExcluded()) {
        continue;
      }

      if (!first) {
        ctx.sql(", ");
      }

      Expression expr = projection.getExpression();
      if (isSimpleInclusion(expr)) {
        // Simple inclusion: "sizeCategory: 1" -> "sizeCategory AS sizeCategory"
        ctx.identifier(fieldAlias);
      } else if (expr instanceof FieldPathExpression fieldPath) {
        // Field rename: "eventCount: '$count'" -> "count AS eventCount"
        // Handle compound _id paths: "$_id.category" -> "category" column
        // In MongoDB, $group with compound _id produces _id.field, but our SQL
        // translation produces columns named directly (category, priority, etc.)
        String sourcePath = fieldPath.getPath();
        if (sourcePath.startsWith("_id.")) {
          String fieldAfterPrefix = sourcePath.substring(4);
          // Only strip _id. prefix if the field is actually from a compound _id
          if (compoundIdFields.contains(fieldAfterPrefix)) {
            sourcePath = fieldAfterPrefix;
          }
        }
        ctx.identifier(sourcePath);
      } else if (expr instanceof ArrayExpression arrayExpr) {
        // Array operations like $size, $round need special handling
        renderPostGroupArrayExpression(arrayExpr, ctx);
      } else if (expr != null) {
        // Computed expression: render using post-group expression context
        renderPostGroupExpression(expr, ctx);
      } else {
        // Fallback: just reference the column
        ctx.identifier(fieldAlias);
      }

      ctx.sql(" AS ");
      ctx.identifier(fieldAlias);
      first = false;
    }
  }

  /**
   * Extracts the field names from a compound _id expression in a GroupStage.
   *
   * @param groupStage the group stage to extract from
   * @return set of field names if the group has a compound _id, empty set otherwise
   */
  private Set<String> getCompoundIdFields(GroupStage groupStage) {
    if (groupStage == null) {
      return Collections.emptySet();
    }
    Expression idExpr = groupStage.getIdExpression();
    if (idExpr instanceof CompoundIdExpression compound) {
      return compound.getFields().keySet();
    }
    return Collections.emptySet();
  }

  /**
   * Checks if an expression represents a simple inclusion (i.e., {field: 1} or {field: true}).
   */
  private boolean isSimpleInclusion(Expression expr) {
    if (expr instanceof LiteralExpression lit) {
      Object value = lit.getValue();
      return (value instanceof Number && ((Number) value).intValue() == 1)
          || (value instanceof Boolean && (Boolean) value);
    }
    return false;
  }

  /**
   * Renders an ArrayExpression in post-group context, resolving field paths to column names.
   */
  private void renderPostGroupArrayExpression(ArrayExpression arrayExpr, SqlGenerationContext ctx) {
    ArrayOp op = arrayExpr.getOp();
    Expression arrayPathExpr = arrayExpr.getArrayExpression();

    // Extract the path if the array expression is a FieldPathExpression
    String path = null;
    if (arrayPathExpr instanceof FieldPathExpression fieldPath) {
      path = fieldPath.getPath();
    }

    switch (op) {
      case SIZE -> {
        if (path != null) {
          // $size on a column: JSON_VALUE(columnName, '$.size()' RETURNING NUMBER)
          ctx.sql("JSON_VALUE(");
          ctx.identifier(path);
          ctx.sql(", '$.size()' RETURNING NUMBER)");
        } else {
          // For computed arrays, fall back to standard rendering
          ctx.visit(arrayExpr);
        }
      }
      default -> {
        // For other array operations, use standard rendering
        ctx.visit(arrayExpr);
      }
    }
  }

  /**
   * Renders an expression in post-group $addFields context. Field paths resolve to column names,
   * not JSON paths.
   */
  private void renderPostGroupExpression(Expression expr, SqlGenerationContext ctx) {
    if (expr instanceof FieldPathExpression fieldPath) {
      // In post-group context, field paths are column names
      ctx.identifier(fieldPath.getPath());
    } else if (expr instanceof ArithmeticExpression arith) {
      renderPostGroupArithmetic(arith, ctx);
    } else if (expr instanceof ConditionalExpression cond) {
      renderPostGroupConditional(cond, ctx);
    } else if (expr instanceof ComparisonExpression comp) {
      renderPostGroupComparison(comp, ctx);
    } else if (expr instanceof SwitchExpression sw) {
      renderPostGroupSwitch(sw, ctx);
    } else {
      // For LiteralExpression and other expressions, use standard rendering
      // This may not always be correct but handles simple cases
      ctx.visit(expr);
    }
  }

  private void renderPostGroupArithmetic(ArithmeticExpression arith, SqlGenerationContext ctx) {
    ArithmeticOp aop = arith.getOp();

    // Function-style operators
    if (aop.requiresFunctionCall()) {
      ctx.sql(aop.getSqlOperator());
      ctx.sql("(");
      boolean first = true;
      for (Expression operand : arith.getOperands()) {
        if (!first) {
          ctx.sql(", ");
        }
        renderPostGroupExpression(operand, ctx);
        first = false;
      }
      ctx.sql(")");
      return;
    }

    // Infix operators (+, -, *, /)
    String op;
    switch (aop) {
      case ADD:
        op = " + ";
        break;
      case SUBTRACT:
        op = " - ";
        break;
      case MULTIPLY:
        op = " * ";
        break;
      case DIVIDE:
        op = " / ";
        break;
      default:
        op = " " + aop.getSqlOperator() + " ";
        break;
    }

    ctx.sql("(");
    boolean first = true;
    for (Expression operand : arith.getOperands()) {
      if (!first) {
        ctx.sql(op);
      }
      renderPostGroupExpression(operand, ctx);
      first = false;
    }
    ctx.sql(")");
  }

  private void renderPostGroupConditional(ConditionalExpression cond, SqlGenerationContext ctx) {
    if (cond.getType() == ConditionalExpression.ConditionalType.IF_NULL) {
      // $ifNull: [expr, replacement] -> NVL(expr, replacement)
      ctx.sql("NVL(");
      renderPostGroupExpression(cond.getThenExpr(), ctx);
      ctx.sql(", ");
      renderPostGroupExpression(cond.getElseExpr(), ctx);
      ctx.sql(")");
    } else {
      // $cond: [if, then, else] -> CASE WHEN ... THEN ... ELSE ... END
      ctx.sql("CASE WHEN ");
      renderPostGroupExpression(cond.getCondition(), ctx);
      ctx.sql(" THEN ");
      renderPostGroupExpression(cond.getThenExpr(), ctx);
      ctx.sql(" ELSE ");
      renderPostGroupExpression(cond.getElseExpr(), ctx);
      ctx.sql(" END");
    }
  }

  private void renderPostGroupComparison(ComparisonExpression comp, SqlGenerationContext ctx) {
    // Handle null comparisons specially - Oracle requires IS NULL / IS NOT NULL
    // and these must be wrapped in CASE WHEN to be used as value expressions in SELECT
    if (comp.getRight() instanceof LiteralExpression lit && lit.isNull()) {
      ctx.sql("CASE WHEN ");
      renderPostGroupExpression(comp.getLeft(), ctx);
      if (comp.getOp() == ComparisonOp.EQ) {
        ctx.sql(" IS NULL THEN 1 ELSE 0 END");
      } else if (comp.getOp() == ComparisonOp.NE) {
        ctx.sql(" IS NOT NULL THEN 1 ELSE 0 END");
      } else {
        throw new IllegalStateException("Invalid NULL comparison with operator: " + comp.getOp());
      }
      return;
    }

    renderPostGroupExpression(comp.getLeft(), ctx);
    ctx.sql(" ");
    ctx.sql(comp.getOp().getSqlOperator());
    ctx.sql(" ");
    renderPostGroupExpression(comp.getRight(), ctx);
  }

  private void renderPostGroupSwitch(SwitchExpression sw, SqlGenerationContext ctx) {
    // Render $switch as CASE WHEN ... THEN ... ELSE ... END
    // using post-group expression rendering for column references
    ctx.sql("CASE");
    for (SwitchExpression.SwitchBranch branch : sw.getBranches()) {
      ctx.sql(" WHEN ");
      renderPostGroupExpression(branch.caseExpr(), ctx);
      ctx.sql(" THEN ");
      renderPostGroupExpression(branch.thenExpr(), ctx);
    }
    if (sw.getDefaultExpr() != null) {
      ctx.sql(" ELSE ");
      renderPostGroupExpression(sw.getDefaultExpr(), ctx);
    }
    ctx.sql(" END");
  }

  /** Renders ORDER BY for outer query - field paths should be column names. */
  private void renderOrderByClauseForOuterQuery(
      PipelineComponents components, SqlGenerationContext ctx) {
    if (components.sortStage == null || components.sortStage.getSortFields().isEmpty()) {
      return;
    }

    ctx.sql(" ORDER BY ");

    boolean first = true;
    for (SortStage.SortField field : components.sortStage.getSortFields()) {
      if (!first) {
        ctx.sql(", ");
      }
      // In outer query context, use column names directly
      ctx.identifier(field.getFieldPath().getPath());
      if (field.getDirection() == SortStage.SortDirection.DESC) {
        ctx.sql(" DESC");
      }
      first = false;
    }
  }

  private void renderCteClause(PipelineComponents components, SqlGenerationContext ctx) {
    // Recursive $graphLookup stages need top-level CTEs with start_id tracking
    // This avoids the ORA-00904 limitation of recursive CTEs inside LATERAL
    List<GraphLookupStage> recursiveGraphLookups = components.graphLookupStages.stream()
        .filter(gl -> gl.getMaxDepth() == null || gl.getMaxDepth() > 0)
        .toList();

    if (recursiveGraphLookups.isEmpty()) {
      return;
    }

    ctx.sql("WITH ");
    boolean first = true;

    for (GraphLookupStage graphLookup : recursiveGraphLookups) {
      if (!first) {
        ctx.sql(", ");
      }
      first = false;

      renderRecursiveGraphLookupCte(graphLookup, components, ctx);
    }

    ctx.sql(" ");
  }

  /**
   * Renders a recursive $graphLookup as a top-level CTE with start_id tracking.
   * This generates two CTEs:
   * 1. graph_paths_{as} - recursive CTE that tracks start_id through the traversal
   * 2. graph_{as} - aggregation CTE that groups results by start_id
   */
  private void renderRecursiveGraphLookupCte(
      GraphLookupStage graphLookup,
      PipelineComponents components,
      SqlGenerationContext ctx) {
    String as = graphLookup.getAs();
    String from = graphLookup.getFrom();
    String startWith = FieldNameValidator.validateAndNormalizeFieldPath(
        graphLookup.getStartWith().startsWith("$")
            ? graphLookup.getStartWith().substring(1)
            : graphLookup.getStartWith());
    final String connectFromField = FieldNameValidator.validateAndNormalizeFieldPath(
        graphLookup.getConnectFromField());
    final String connectToField = FieldNameValidator.validateAndNormalizeFieldPath(
        graphLookup.getConnectToField());

    // CTE 1: Recursive path traversal with start_id tracking
    ctx.sql("graph_paths_");
    ctx.sql(as);
    ctx.sql(" (start_id, id, data, graph_depth) AS (");

    // Base case: each row in the source starts its own traversal
    ctx.sql("SELECT ");
    ctx.sql(ctx.getBaseTableAlias());
    ctx.sql(".data.");
    ctx.sql(quotePath(startWith));
    ctx.sql(" AS start_id, g.id, g.data, 0 AS graph_depth FROM ");
    ctx.tableName(from);
    ctx.sql(" g, ");
    ctx.tableName(components.collectionName);
    ctx.sql(" ");
    ctx.sql(ctx.getBaseTableAlias());
    ctx.sql(" WHERE g.data.");
    ctx.sql(quotePath(connectToField));
    ctx.sql(" = ");
    ctx.sql(ctx.getBaseTableAlias());
    ctx.sql(".data.");
    ctx.sql(quotePath(startWith));

    // Add restrictSearchWithMatch filter if specified
    if (graphLookup.getRestrictSearchWithMatch() != null
        && !graphLookup.getRestrictSearchWithMatch().isEmpty()) {
      renderRestrictMatchConditions(graphLookup, ctx, "g");
    }

    ctx.sql(" UNION ALL ");

    // Recursive case: follow connections
    // NOTE: For CTE columns, we must use JSON_VALUE() since Oracle's dot notation
    // doesn't work on CLOB columns in CTE results
    ctx.sql("SELECT p.start_id, c.id, c.data, p.graph_depth + 1 FROM ");
    ctx.tableName(from);
    ctx.sql(" c JOIN graph_paths_");
    ctx.sql(as);
    ctx.sql(" p ON c.data.");
    ctx.sql(quotePath(connectToField));
    ctx.sql(" = JSON_VALUE(p.data, '$.");
    ctx.sql(connectFromField);
    ctx.sql("')");

    // Add depth limit if specified
    if (graphLookup.getMaxDepth() != null) {
      ctx.sql(" WHERE p.graph_depth < ");
      ctx.sql(String.valueOf(graphLookup.getMaxDepth()));
    }

    // Add restrictSearchWithMatch filter to recursive case
    if (graphLookup.getRestrictSearchWithMatch() != null
        && !graphLookup.getRestrictSearchWithMatch().isEmpty()) {
      if (graphLookup.getMaxDepth() != null) {
        renderRestrictMatchConditions(graphLookup, ctx, "c");
      } else {
        ctx.sql(" WHERE 1=1");
        renderRestrictMatchConditions(graphLookup, ctx, "c");
      }
    }

    ctx.sql("), ");

    // CTE 2: Aggregate by start_id
    ctx.sql("graph_");
    ctx.sql(as);
    ctx.sql(" AS (SELECT start_id, JSON_ARRAYAGG(data RETURNING CLOB) AS ");
    ctx.identifier(as);
    if (graphLookup.getDepthField() != null) {
      ctx.sql(", MAX(graph_depth) AS ");
      ctx.identifier(graphLookup.getDepthField());
    }
    ctx.sql(" FROM graph_paths_");
    ctx.sql(as);
    ctx.sql(" GROUP BY start_id)");
  }

  /** Renders restrictSearchWithMatch conditions for $graphLookup. */
  private void renderRestrictMatchConditions(
      GraphLookupStage graphLookup, SqlGenerationContext ctx, String alias) {
    for (Map.Entry<String, Object> entry :
        graphLookup.getRestrictSearchWithMatch().entrySet()) {
      final String field = FieldNameValidator.validateAndNormalizeFieldPath(entry.getKey());
      final Object value = entry.getValue();

      ctx.sql(" AND ");
      ctx.sql(alias);
      ctx.sql(".data.");
      ctx.sql(quotePath(field));
      ctx.sql(" = ");
      renderLiteralValueForGraphLookup(ctx, value);
    }
  }

  /** Renders a literal value for $graphLookup restrict conditions. */
  private void renderLiteralValueForGraphLookup(SqlGenerationContext ctx, Object value) {
    if (value == null) {
      ctx.sql("NULL");
    } else if (value instanceof String str) {
      ctx.sql("'");
      ctx.sql(str.replace("'", "''"));
      ctx.sql("'");
    } else if (value instanceof Boolean bool) {
      ctx.sql(bool ? "'true'" : "'false'");
    } else {
      ctx.sql(String.valueOf(value));
    }
  }

  private void renderGraphLookupJoins(PipelineComponents components, SqlGenerationContext ctx) {
    for (GraphLookupStage graphLookup : components.graphLookupStages) {
      // Validate table name and field names to prevent injection
      FieldNameValidator.validateTableName(graphLookup.getFrom());
      String validConnectToField =
          FieldNameValidator.validateAndNormalizeFieldPath(graphLookup.getConnectToField());
      String validConnectFromField =
          FieldNameValidator.validateAndNormalizeFieldPath(graphLookup.getConnectFromField());
      String validStartField =
          FieldNameValidator.validateAndNormalizeFieldPath(graphLookup.getStartWith());

      // Check if this is a recursive traversal (maxDepth > 0 or null for unlimited)
      boolean isRecursive =
          graphLookup.getMaxDepth() == null || graphLookup.getMaxDepth() > 0;

      if (isRecursive) {
        renderRecursiveGraphLookup(
            graphLookup,
            validConnectToField,
            validConnectFromField,
            validStartField,
            ctx);
      } else {
        renderSimpleGraphLookup(graphLookup, validConnectToField, validStartField, ctx);
      }
    }
  }

  /**
   * Renders a recursive $graphLookup using the top-level CTE approach.
   * The CTE (graph_{as}) was rendered in renderCteClause with start_id tracking.
   * Now we just need a simple LEFT JOIN to that CTE by matching start_id to the base data.
   */
  private void renderRecursiveGraphLookup(
      GraphLookupStage graphLookup,
      String connectToField,
      String connectFromField,
      String startField,
      SqlGenerationContext ctx) {
    // The recursive CTE was rendered in renderCteClause
    // Now join to the aggregated results by start_id
    ctx.sql(" LEFT JOIN graph_");
    ctx.sql(graphLookup.getAs());
    ctx.sql(" ");
    ctx.identifier(graphLookup.getAs() + "_cte");
    ctx.sql(" ON ");
    ctx.identifier(graphLookup.getAs() + "_cte");
    ctx.sql(".start_id = ");
    ctx.sql(ctx.getBaseTableAlias());
    ctx.sql(".data.");
    ctx.sql(quotePath(startField));
  }

  /** Renders a simple (non-recursive) $graphLookup using a direct LATERAL join. */
  private void renderSimpleGraphLookup(
      GraphLookupStage graphLookup,
      String connectToField,
      String startField,
      SqlGenerationContext ctx) {
    // Use LATERAL join (CROSS APPLY) which allows referencing outer query columns
    ctx.sql(" LEFT OUTER JOIN LATERAL (SELECT JSON_ARRAYAGG(g.data) AS ");
    ctx.identifier(graphLookup.getAs());

    ctx.sql(" FROM ");
    ctx.tableName(graphLookup.getFrom());
    // Use dot notation for type preservation in the WHERE clause
    ctx.sql(" g WHERE g.data.");
    ctx.sql(quotePath(connectToField));
    ctx.sql(" = ");
    ctx.sql(ctx.getBaseTableAlias());
    ctx.sql(".data.");
    ctx.sql(quotePath(startField));

    // Add restrictSearchWithMatch filter if specified
    renderRestrictSearchFilter(graphLookup, "g", ctx);

    ctx.sql(") ");
    ctx.identifier(graphLookup.getAs() + "_cte");
    ctx.sql(" ON 1=1");
  }

  /** Renders restrictSearchWithMatch conditions as AND clauses using dot notation. */
  private void renderRestrictSearchFilter(
      GraphLookupStage graphLookup, String tableAlias, SqlGenerationContext ctx) {
    if (graphLookup.getRestrictSearchWithMatch() == null
        || graphLookup.getRestrictSearchWithMatch().isEmpty()) {
      return;
    }

    for (var entry : graphLookup.getRestrictSearchWithMatch().entrySet()) {
      String validField = FieldNameValidator.validateAndNormalizeFieldPath(entry.getKey());
      final Object value = entry.getValue();
      // Use dot notation for type preservation
      ctx.sql(" AND ");
      ctx.sql(tableAlias);
      ctx.sql(".data.");
      ctx.sql(quotePath(validField));
      ctx.sql(" = ");
      if (value instanceof String) {
        ctx.sql("'");
        ctx.sql(((String) value).replace("'", "''"));
        ctx.sql("'");
      } else if (value instanceof Boolean) {
        ctx.sql(value.toString());
      } else {
        ctx.sql(String.valueOf(value));
      }
    }
  }

  private PipelineComponents analyzePipeline(Pipeline pipeline) {
    PipelineComponents components = new PipelineComponents();
    components.collectionName = pipeline.getCollection();
    boolean sawGroupStage = false;
    boolean sawSetWindowFields = false;
    boolean sawUnionWith = false;
    boolean sawFacetStage = false;

    for (Stage stage : pipeline.getStages()) {
      if (stage instanceof MatchStage match) {
        // Check if this $match follows $setWindowFields and references window output fields
        if (sawSetWindowFields && matchReferencesWindowFields(match, components)) {
          components.postWindowMatchStages.add(match);
          components.hasPostWindowMatch = true;
        } else {
          components.matchStages.add(match);
        }
      } else if (stage instanceof GroupStage group) {
        if (sawUnionWith) {
          // $group after $unionWith aggregates the whole union result
          components.postUnionGroupStage = group;
          components.hasPostUnionGroup = true;
        } else {
          components.allGroupStages.add(group);
          components.groupStage = group; // Keep last group for backward compat
        }
        sawGroupStage = true;
      } else if (stage instanceof ProjectStage project) {
        if (sawFacetStage) {
          // $project after $facet reshapes facet output
          components.postFacetProjectStage = project;
        } else {
          components.projectStage = project;
        }
      } else if (stage instanceof SortStage sort) {
        if (sawUnionWith) {
          // $sort after $unionWith applies to the whole union result
          components.postUnionSortStage = sort;
          components.hasPostUnionSortOrLimit = true;
        } else {
          components.sortStage = sort;
        }
      } else if (stage instanceof SkipStage skip) {
        components.skipStage = skip;
      } else if (stage instanceof LimitStage limit) {
        if (sawUnionWith) {
          // $limit after $unionWith applies to the whole union result
          components.postUnionLimitStage = limit;
          components.hasPostUnionSortOrLimit = true;
        } else {
          components.limitStage = limit;
        }
      } else if (stage instanceof LookupStage lookup) {
        components.lookupStages.add(lookup);
      } else if (stage instanceof UnwindStage unwind) {
        components.unwindStages.add(unwind);
      } else if (stage instanceof AddFieldsStage addFields) {
        if (sawGroupStage) {
          // $addFields after $group needs special handling
          components.postGroupAddFieldsStages.add(addFields);
          components.hasPostGroupAddFields = true;
        } else {
          components.addFieldsStages.add(addFields);
        }
      } else if (stage instanceof UnionWithStage unionWith) {
        components.unionWithStages.add(unionWith);
        sawUnionWith = true;
      } else if (stage instanceof BucketStage bucket) {
        components.bucketStage = bucket;
        sawGroupStage = true; // $bucket also produces grouped results
      } else if (stage instanceof BucketAutoStage bucketAuto) {
        components.bucketAutoStage = bucketAuto;
        sawGroupStage = true; // $bucketAuto also produces grouped results
      } else if (stage instanceof FacetStage facet) {
        components.facetStage = facet;
        sawFacetStage = true;
      } else if (stage instanceof GraphLookupStage graphLookup) {
        components.graphLookupStages.add(graphLookup);
      } else if (stage instanceof SetWindowFieldsStage setWindowFields) {
        if (sawGroupStage) {
          // $setWindowFields after $group needs special handling
          components.postGroupSetWindowFieldsStages.add(setWindowFields);
          components.hasPostGroupSetWindowFields = true;
        } else {
          components.setWindowFieldsStages.add(setWindowFields);
        }
        sawSetWindowFields = true;
      } else if (stage instanceof CountStage count) {
        components.countStage = count;
      } else if (stage instanceof SampleStage sample) {
        components.sampleStage = sample;
      } else if (stage instanceof RedactStage redact) {
        components.redactStages.add(redact);
      } else if (stage instanceof ReplaceRootStage replaceRoot) {
        components.replaceRootStage = replaceRoot;
      } else if (stage instanceof OutStage out) {
        components.outStage = out;
      } else if (stage instanceof MergeStage merge) {
        components.mergeStage = merge;
      }
      // For unknown stages, we skip them (they won't be rendered)
    }

    // Analyze pipeline for CTE requirements (multiple $group stages, etc.)
    components.stageSequence = PipelineStageSequence.analyze(pipeline);

    return components;
  }

  private void renderSelectClause(PipelineComponents components, SqlGenerationContext ctx) {
    ctx.sql("SELECT ");

    if (components.countStage != null) {
      // $count returns a single document with the count
      // The actual JSON_ARRAYAGG wrapping is done by renderCountWithSubquery
      // This just outputs the field reference for the inner subquery result
      ctx.sql("JSON_ARRAYAGG(JSON_OBJECT('");
      ctx.sql(components.countStage.getFieldName());
      ctx.sql("' VALUE cnt) RETURNING CLOB)");
      return; // $count replaces the entire query
    } else if (components.facetStage != null) {
      // $facet creates a JSON object with multiple subquery results
      renderFacetSelectClause(components, ctx);
      return; // $facet replaces the entire query
    } else if (components.replaceRootStage != null) {
      // $replaceRoot restructures the document
      renderReplaceRootSelectClause(components.replaceRootStage, ctx);
      return; // $replaceRoot replaces the entire SELECT
    } else if (components.groupStage != null) {
      // $group determines the SELECT clause
      renderGroupSelectClause(components.groupStage, ctx);
    } else if (components.bucketStage != null) {
      // $bucket determines the SELECT clause
      ctx.visit(components.bucketStage);
    } else if (components.bucketAutoStage != null) {
      // $bucketAuto determines the SELECT clause
      ctx.visit(components.bucketAutoStage);
    } else if (components.projectStage != null) {
      // $project determines the SELECT clause
      renderProjectSelectClause(components.projectStage, components, ctx);
    } else {
      // Default: select all data
      // Use table alias if present
      String baseAlias = ctx.getBaseTableAlias();
      if (baseAlias != null && !baseAlias.isEmpty()) {
        ctx.sql(baseAlias);
        ctx.sql(".");
      }
      ctx.sql(config.dataColumnName());
    }

    // $addFields adds computed columns to the existing SELECT
    renderAddFieldsClauses(components, ctx);
  }

  private void renderAddFieldsClauses(PipelineComponents components, SqlGenerationContext ctx) {
    // Only render pre-group $addFields when there's no GROUP BY
    // When there's a $group, pre-group addFields are only used as virtual fields
    // for expression substitution, not as standalone columns
    if (components.groupStage == null) {
      for (AddFieldsStage addFields : components.addFieldsStages) {
        if (!addFields.getFields().isEmpty()) {
          ctx.sql(", ");
          ctx.visit(addFields);
        }
      }
    }

    // Render $setWindowFields window function columns
    for (SetWindowFieldsStage setWindowFields : components.setWindowFieldsStages) {
      ctx.sql(", ");
      ctx.visit(setWindowFields);
    }

    // Render $graphLookup result columns
    for (GraphLookupStage graphLookup : components.graphLookupStages) {
      ctx.sql(", ");
      ctx.identifier(graphLookup.getAs() + "_cte");
      ctx.sql(".");
      ctx.identifier(graphLookup.getAs());
      ctx.sql(" AS ");
      ctx.identifier(graphLookup.getAs());
      if (graphLookup.getDepthField() != null) {
        ctx.sql(", ");
        ctx.identifier(graphLookup.getAs() + "_cte");
        ctx.sql(".");
        ctx.identifier(graphLookup.getDepthField());
        ctx.sql(" AS ");
        ctx.identifier(graphLookup.getDepthField());
      }
    }
  }

  private void renderProjectSelectClause(
      ProjectStage project, PipelineComponents components, SqlGenerationContext ctx) {
    // Collect computed field names from $addFields and $setWindowFields
    // These will be rendered separately by renderAddFieldsClauses, so skip them in $project
    Set<String> computedFieldNames = new HashSet<>();
    for (AddFieldsStage addFields : components.addFieldsStages) {
      computedFieldNames.addAll(addFields.getFields().keySet());
    }
    for (SetWindowFieldsStage swf : components.setWindowFieldsStages) {
      computedFieldNames.addAll(swf.getOutput().keySet());
    }
    // Also add $graphLookup output field names - these are rendered by renderAddFieldsClauses
    for (GraphLookupStage graphLookup : components.graphLookupStages) {
      computedFieldNames.add(graphLookup.getAs());
      if (graphLookup.getDepthField() != null) {
        computedFieldNames.add(graphLookup.getDepthField());
      }
    }

    // Determine if we need row-by-row output (for UNIONs or nested pipelines)
    boolean needsRowByRow = ctx.isNestedPipeline() || !components.unionWithStages.isEmpty();

    if (needsRowByRow) {
      // Row-by-row output: SELECT col1 AS alias1, col2 AS alias2, ...
      renderProjectSelectClauseRowByRow(project, computedFieldNames, ctx);
    } else {
      // Aggregated JSON output: JSON_ARRAYAGG(JSON_OBJECT(...))
      renderProjectSelectClauseJsonAgg(project, computedFieldNames, ctx);
    }
  }

  private void renderProjectSelectClauseRowByRow(
      ProjectStage project, Set<String> computedFieldNames, SqlGenerationContext ctx) {
    // Use JSON output mode to preserve native JSON types when columns are wrapped
    // in JSON_OBJECT(*) by the outer query (e.g., for UNION or nested pipelines)
    ctx.setJsonOutputMode(true);

    boolean first = true;
    for (var entry : project.getProjections().entrySet()) {
      final String alias = entry.getKey();
      ProjectStage.ProjectionField field = entry.getValue();

      if (field.isExcluded()) {
        continue;
      }

      // Skip fields computed by $addFields/$setWindowFields
      if (computedFieldNames.contains(alias) && isSimpleFieldInclusion(field, alias)) {
        continue;
      }

      if (!first) {
        ctx.sql(", ");
      }

      // Render expression with alias
      if (field.getExpression() != null) {
        ctx.visit(field.getExpression());
      }
      ctx.sql(" AS ");
      ctx.identifier(alias);
      first = false;
    }

    if (first) {
      ctx.sql("NULL AS dummy");
    }

    ctx.setJsonOutputMode(false);
  }

  private void renderProjectSelectClauseJsonAgg(
      ProjectStage project, Set<String> computedFieldNames, SqlGenerationContext ctx) {
    // Start JSON_ARRAYAGG wrapper for type-preserving output
    ctx.sql("JSON_ARRAYAGG(JSON_OBJECT(");
    ctx.setJsonOutputMode(true);

    boolean first = true;
    for (var entry : project.getProjections().entrySet()) {
      final String alias = entry.getKey();
      ProjectStage.ProjectionField field = entry.getValue();

      if (field.isExcluded()) {
        continue;
      }

      // Skip fields that are computed by $addFields or $setWindowFields
      if (computedFieldNames.contains(alias) && isSimpleFieldInclusion(field, alias)) {
        continue;
      }

      if (!first) {
        ctx.sql(", ");
      }

      // Render as KEY 'alias' VALUE expr for JSON_OBJECT
      ctx.sql("KEY '");
      ctx.sql(alias);
      ctx.sql("' VALUE ");

      if (field.getExpression() != null) {
        ctx.visit(field.getExpression());
      }
      first = false;
    }

    if (first) {
      ctx.sql("KEY 'dummy' VALUE NULL");
    }

    ctx.sql(") RETURNING CLOB)");
    ctx.setJsonOutputMode(false);
  }

  /**
   * Checks if a projection field is a simple field inclusion (just referencing the field, not
   * transforming it). This is true when: - the expression is a FieldPathExpression pointing to the
   * same field name as the alias - or when expression is null (implicit inclusion)
   */
  private boolean isSimpleFieldInclusion(ProjectStage.ProjectionField field, String alias) {
    Expression expr = field.getExpression();
    if (expr == null) {
      return true; // Implicit inclusion like {fieldName: 1}
    }
    if (expr instanceof FieldPathExpression fieldPath) {
      // Check if it's just referencing the same field
      // (e.g., {totalCompensation: "$totalCompensation"})
      return fieldPath.getPath().equals(alias);
    }
    return false;
  }

  /**
   * Renders $addFields computed columns, skipping fields that were already transformed by $project.
   * This prevents duplicate column names in the SQL output.
   */
  private void renderAddFieldsExcluding(
      AddFieldsStage addFields, Set<String> excludeFields, SqlGenerationContext ctx) {
    for (Map.Entry<String, Expression> entry : addFields.getFields().entrySet()) {
      String fieldName = entry.getKey();

      // Skip fields already transformed by $project
      if (excludeFields.contains(fieldName)) {
        continue;
      }

      ctx.sql(", ");

      Expression expr = entry.getValue();
      // Oracle doesn't support boolean as column value, wrap in CASE WHEN
      if (expr.isBooleanExpression()) {
        ctx.sql("CASE WHEN ");
        ctx.visit(expr);
        ctx.sql(" THEN 1 ELSE 0 END");
      } else {
        ctx.visit(expr);
      }
      ctx.sql(" AS ");
      ctx.identifier(fieldName);
    }
  }

  /**
   * Renders $addFields computed columns that are included in $project but not transformed.
   * Only fields in includeFields and NOT in excludeFields are rendered.
   */
  private void renderAddFieldsIncluding(
      AddFieldsStage addFields,
      Set<String> includeFields,
      Set<String> excludeFields,
      SqlGenerationContext ctx) {
    for (Map.Entry<String, Expression> entry : addFields.getFields().entrySet()) {
      String fieldName = entry.getKey();

      // Only render fields that are included in $project
      if (!includeFields.contains(fieldName)) {
        continue;
      }

      // Skip fields already transformed by $project
      if (excludeFields.contains(fieldName)) {
        continue;
      }

      ctx.sql(", ");

      Expression expr = entry.getValue();
      // Oracle doesn't support boolean as column value, wrap in CASE WHEN
      if (expr.isBooleanExpression()) {
        ctx.sql("CASE WHEN ");
        ctx.visit(expr);
        ctx.sql(" THEN 1 ELSE 0 END");
      } else {
        ctx.visit(expr);
      }
      ctx.sql(" AS ");
      // Enable JSON output mode to quote the alias and preserve case for JSON_OBJECT(*)
      boolean wasJsonMode = ctx.isJsonOutputMode();
      ctx.setJsonOutputMode(true);
      ctx.identifier(fieldName);
      ctx.setJsonOutputMode(wasJsonMode);
    }
  }

  /**
   * Renders $replaceRoot SELECT clause. When newRoot is an InlineObjectExpression (document with
   * field-to-expression mappings), each field becomes a separate column. When it's a
   * FieldPathExpression (subdocument promotion), the subdocument becomes the data column.
   */
  private void renderReplaceRootSelectClause(
      ReplaceRootStage replaceRoot, SqlGenerationContext ctx) {
    Expression newRoot = replaceRoot.getNewRoot();

    if (newRoot instanceof InlineObjectExpression inlineObj) {
      // Document with explicit field mappings: {field1: "$expr1", field2: "$expr2"}
      // Render as: expr1 AS field1, expr2 AS field2
      // Use JSON output mode to preserve native JSON types when wrapped in JSON_OBJECT(*)
      ctx.setJsonOutputMode(true);

      boolean first = true;
      for (Map.Entry<String, Expression> entry : inlineObj.getFields().entrySet()) {
        if (!first) {
          ctx.sql(", ");
        }
        ctx.visit(entry.getValue());
        ctx.sql(" AS ");
        ctx.identifier(entry.getKey());
        first = false;
      }
      if (first) {
        ctx.sql("NULL AS dummy");
      }

      ctx.setJsonOutputMode(false);
    } else if (newRoot instanceof FieldPathExpression fieldPath) {
      // Subdocument promotion: {newRoot: "$subdocument"}
      // Render as: JSON_QUERY(data, '$.subdocument') AS data
      ctx.sql("JSON_QUERY(");
      String alias = ctx.getBaseTableAlias();
      if (alias != null && !alias.isEmpty()) {
        ctx.sql(alias);
        ctx.sql(".");
      }
      ctx.sql("data, '$.");
      ctx.sql(fieldPath.getPath());
      ctx.sql("') AS ");
      ctx.sql(config.dataColumnName());
    } else {
      // Other expressions (e.g., $mergeObjects) - render as data column
      ctx.visit(newRoot);
      ctx.sql(" AS ");
      ctx.sql(config.dataColumnName());
    }
  }

  /**
   * Renders a $facet stage as a complete query selecting JSON_OBJECT from DUAL. Each facet pipeline
   * becomes a scalar subquery within the JSON_OBJECT.
   *
   * <pre>
   * SELECT JSON_OBJECT(
   *   'facetName1' VALUE (SELECT JSON_ARRAYAGG(...) FROM (subquery1)),
   *   'facetName2' VALUE (SELECT JSON_ARRAYAGG(...) FROM (subquery2))
   * ) AS result FROM DUAL
   * </pre>
   */
  private void renderFacetSelectClause(PipelineComponents components, SqlGenerationContext ctx) {
    FacetStage facet = components.facetStage;
    String collectionName = components.collectionName;

    // If there's a post-facet $project, use its field names and transformations
    if (components.postFacetProjectStage != null) {
      renderPostFacetProjectSelectClause(components, ctx);
      return;
    }

    // Wrap in JSON_ARRAYAGG so the result is a JSON array (matches other pipeline outputs)
    // This ensures the test harness uses the SQL directly without wrapping in JSON_OBJECT(*)
    ctx.sql("JSON_ARRAYAGG(JSON_OBJECT(");
    boolean first = true;

    for (Map.Entry<String, List<Stage>> entry : facet.getFacets().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      String facetName = entry.getKey();
      final List<Stage> pipeline = entry.getValue();

      ctx.sql("'");
      ctx.sql(facetName);
      ctx.sql("' VALUE (");
      renderFacetPipeline(collectionName, pipeline, components, ctx);
      ctx.sql(")");
      first = false;
    }

    ctx.sql(")) AS ");
    ctx.sql(config.dataColumnName());
  }

  /**
   * Renders a facet SELECT clause with post-facet $project transformations.
   * This handles patterns like:
   * - {"$arrayElemAt": ["$summary.count", 0]} → extracts scalar from count facet
   * - "$results" → renames facet output
   */
  private void renderPostFacetProjectSelectClause(
      PipelineComponents components, SqlGenerationContext ctx) {
    FacetStage facet = components.facetStage;
    ProjectStage postProject = components.postFacetProjectStage;
    String collectionName = components.collectionName;

    // Wrap in JSON_ARRAYAGG so the result is a JSON array (matches other pipeline outputs)
    ctx.sql("JSON_ARRAYAGG(JSON_OBJECT(");
    boolean first = true;

    for (var entry : postProject.getProjections().entrySet()) {
      final String outputFieldName = entry.getKey();
      ProjectStage.ProjectionField projField = entry.getValue();

      if (projField.isExcluded()) {
        continue;
      }

      if (!first) {
        ctx.sql(", ");
      }

      ctx.sql("'");
      ctx.sql(outputFieldName);
      ctx.sql("' VALUE ");

      Expression expr = projField.getExpression();
      renderPostFacetFieldExpression(expr, facet, collectionName, components, ctx);

      first = false;
    }

    ctx.sql(")) AS ");
    ctx.sql(config.dataColumnName());
  }

  /**
   * Renders a post-facet field expression, handling:
   * - FieldPathExpression (e.g., "$results") → render the referenced facet pipeline
   * - ArrayExpression with ARRAY_ELEM_AT (e.g., {"$arrayElemAt": ["$summary.count", 0]})
   *   → extract scalar value from facet result
   */
  private void renderPostFacetFieldExpression(
      Expression expr,
      FacetStage facet,
      String collectionName,
      PipelineComponents components,
      SqlGenerationContext ctx) {

    if (expr instanceof FieldPathExpression fieldPath) {
      String fullPath = fieldPath.getPath();
      List<Stage> pipeline = facet.getFacets().get(fullPath);

      if (pipeline != null) {
        // Simple facet reference like "$results" - render the facet pipeline
        ctx.sql("(");
        renderFacetPipeline(collectionName, pipeline, components, ctx);
        ctx.sql(")");
      } else if (fullPath.contains(".")) {
        // Nested field access like "$data._id" - facet="data", field="_id"
        // This extracts a field from each element in the facet array
        String[] parts = fullPath.split("\\.", 2);
        String facetName = parts[0];
        String nestedField = parts[1];
        List<Stage> facetPipeline = facet.getFacets().get(facetName);

        if (facetPipeline != null) {
          renderFacetNestedFieldExtraction(
              facetPipeline, nestedField, collectionName, components, ctx);
        } else {
          // Fallback: render the expression normally
          ctx.sql("(");
          ctx.visit(expr);
          ctx.sql(")");
        }
      } else {
        // Fallback: render the expression normally
        ctx.sql("(");
        ctx.visit(expr);
        ctx.sql(")");
      }
    } else if (expr instanceof ArrayExpression arrExpr
        && arrExpr.getOp() == ArrayOp.ARRAY_ELEM_AT) {
      // $arrayElemAt expression like {"$arrayElemAt": ["$summary.count", 0]}
      renderArrayElemAtFacetExtraction(arrExpr, facet, collectionName, components, ctx);
    } else {
      // Fallback: render the expression normally
      ctx.sql("(");
      ctx.visit(expr);
      ctx.sql(")");
    }
  }

  /**
   * Renders extraction of a nested field from each element in a facet array.
   * Pattern: "$data._id" → extracts _id from each element in data facet array
   *
   * <p>This produces SQL like:
   * <pre>
   * SELECT JSON_ARRAYAGG(jt.field_val FORMAT JSON)
   * FROM JSON_TABLE((facet_subquery), '$[*]'
   *   COLUMNS (field_val VARCHAR2(4000) FORMAT JSON PATH '$._id')) jt
   * </pre>
   */
  private void renderFacetNestedFieldExtraction(
      List<Stage> facetPipeline,
      String nestedField,
      String collectionName,
      PipelineComponents components,
      SqlGenerationContext ctx) {

    // Build the JSON path for the nested field
    final String jsonPath = "$." + nestedField;

    ctx.sql("(SELECT JSON_ARRAYAGG(jt_nested.field_val FORMAT JSON) FROM JSON_TABLE((");
    renderFacetPipeline(collectionName, facetPipeline, components, ctx);
    ctx.sql("), '$[*]' COLUMNS (field_val VARCHAR2(4000) FORMAT JSON PATH '");
    ctx.sql(jsonPath);
    ctx.sql("')) jt_nested)");
  }

  /**
   * Renders an $arrayElemAt extraction from a facet result.
   * Pattern: {"$arrayElemAt": ["$summary.count", 0]}
   * → JSON_VALUE((SELECT ... FROM summary facet), '$[0].count')
   */
  private void renderArrayElemAtFacetExtraction(
      ArrayExpression arrExpr,
      FacetStage facet,
      String collectionName,
      PipelineComponents components,
      SqlGenerationContext ctx) {

    Expression arrayExpr = arrExpr.getArrayExpression();
    Expression indexExpr = arrExpr.getIndexExpression();

    // Determine index value (typically 0 for first element)
    int index = 0;
    if (indexExpr instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
      index = num.intValue();
    }

    if (arrayExpr instanceof FieldPathExpression fieldPath) {
      // Parse the field path: e.g., "summary.count" → facet="summary", field="count"
      String fullPath = fieldPath.getPath();
      String[] parts = fullPath.split("\\.", 2);
      String facetName = parts[0];
      String fieldName = parts.length > 1 ? parts[1] : null;

      List<Stage> pipeline = facet.getFacets().get(facetName);
      if (pipeline != null) {
        // Check if this is a count facet (contains $count stage)
        boolean isCountFacet = pipeline.stream().anyMatch(s -> s instanceof CountStage);

        if (isCountFacet && fieldName != null) {
          // For count facet: extract the scalar value directly
          // The count subquery returns JSON_ARRAYAGG([{count: N}])
          // We need JSON_VALUE(..., '$[0].count' RETURNING NUMBER)
          ctx.sql("JSON_VALUE((");
          renderFacetPipeline(collectionName, pipeline, components, ctx);
          ctx.sql("), '$[");
          ctx.sql(String.valueOf(index));
          ctx.sql("].");
          ctx.sql(fieldName);
          ctx.sql("' RETURNING NUMBER)");
        } else {
          // Generic case: extract element at index
          ctx.sql("JSON_QUERY((");
          renderFacetPipeline(collectionName, pipeline, components, ctx);
          ctx.sql("), '$[");
          ctx.sql(String.valueOf(index));
          ctx.sql("]')");
        }
        return;
      }
    }

    // Fallback: render the expression normally
    ctx.sql("(");
    ctx.visit(arrExpr);
    ctx.sql(")");
  }

  /**
   * Renders a single facet pipeline as a scalar subquery that returns JSON_ARRAYAGG of results. The
   * output structure depends on what the pipeline produces.
   */
  private void renderFacetPipeline(
      String collectionName,
      List<Stage> pipeline,
      PipelineComponents parentComponents,
      SqlGenerationContext ctx) {
    // Analyze the facet pipeline to understand its structure
    GroupStage groupStage = null;
    SortStage sortStage = null;
    LimitStage limitStage = null;
    SkipStage skipStage = null;
    ProjectStage projectStage = null;
    CountStage countStage = null;
    List<MatchStage> matchStages = new ArrayList<>();

    for (Stage stage : pipeline) {
      if (stage instanceof GroupStage g) {
        groupStage = g;
      } else if (stage instanceof SortStage s) {
        sortStage = s;
      } else if (stage instanceof LimitStage l) {
        limitStage = l;
      } else if (stage instanceof SkipStage sk) {
        skipStage = sk;
      } else if (stage instanceof ProjectStage p) {
        projectStage = p;
      } else if (stage instanceof MatchStage m) {
        matchStages.add(m);
      } else if (stage instanceof CountStage c) {
        countStage = c;
      }
    }

    // Special case: $count stage in facet sub-pipeline (e.g., recordCount: [{$count: "count"}])
    if (countStage != null) {
      renderFacetCountQuery(
          collectionName, countStage, matchStages, parentComponents, ctx);
      return;
    }

    // Special case: pagination over pre-facet grouped data
    // When parent has $group and facet just has $skip/$limit, paginate the grouped results
    if (parentComponents.groupStage != null
        && groupStage == null
        && projectStage == null
        && (skipStage != null || limitStage != null)) {
      renderFacetPaginationQuery(
          collectionName, skipStage, limitStage, sortStage, matchStages, parentComponents, ctx);
      return;
    }

    // Outer query: JSON_ARRAYAGG around inner subquery
    ctx.sql("SELECT JSON_ARRAYAGG(");
    renderFacetJsonObject(groupStage, projectStage, sortStage, ctx);

    // Add ORDER BY inside JSON_ARRAYAGG if there's a sort
    if (sortStage != null && !sortStage.getSortFields().isEmpty()) {
      ctx.sql(" ORDER BY ");
      boolean firstSort = true;
      for (SortStage.SortField field : sortStage.getSortFields()) {
        if (!firstSort) {
          ctx.sql(", ");
        }
        ctx.identifier(field.getFieldPath().getPath());
        if (field.getDirection() == SortStage.SortDirection.DESC) {
          ctx.sql(" DESC");
        }
        firstSort = false;
      }
    }

    ctx.sql(") FROM (");

    // Inner query: the actual aggregation/selection
    if (groupStage != null) {
      renderFacetGroupQuery(collectionName, groupStage, matchStages, parentComponents, ctx);
    } else if (projectStage != null) {
      renderFacetProjectQuery(
          collectionName, projectStage, matchStages, sortStage, limitStage, parentComponents, ctx);
    } else {
      // Simple select - include parent's group if present
      if (parentComponents.groupStage != null) {
        renderPreFacetGroupQuery(collectionName, parentComponents, ctx);
      } else {
        ctx.sql("SELECT * FROM ");
        ctx.tableName(collectionName);
      }
    }

    ctx.sql(")");
  }

  /**
   * Renders the JSON_OBJECT for each row in a facet result. The structure depends on the group or
   * project stage.
   */
  private void renderFacetJsonObject(
      GroupStage groupStage,
      ProjectStage projectStage,
      SortStage sortStage,
      SqlGenerationContext ctx) {
    ctx.sql("JSON_OBJECT(");

    if (groupStage != null) {
      boolean first = true;
      // Include _id if present
      if (groupStage.getIdExpression() != null) {
        ctx.sql("'_id' VALUE ");
        ctx.identifier("_id");
        first = false;
      } else {
        ctx.sql("'_id' VALUE NULL");
        first = false;
      }
      // Include all accumulators
      for (String accName : groupStage.getAccumulators().keySet()) {
        if (!first) {
          ctx.sql(", ");
        }
        ctx.sql("'");
        ctx.sql(accName);
        ctx.sql("' VALUE ");
        ctx.identifier(accName);
        first = false;
      }
    } else if (projectStage != null) {
      boolean first = true;
      for (String fieldName : projectStage.getProjections().keySet()) {
        if (!first) {
          ctx.sql(", ");
        }
        ctx.sql("'");
        ctx.sql(fieldName);
        ctx.sql("' VALUE ");
        ctx.identifier(fieldName);
        first = false;
      }
    } else {
      ctx.sql("'data' VALUE data");
    }

    ctx.sql(")");
  }

  /** Renders a facet pipeline that contains a $group stage. */
  private void renderFacetGroupQuery(
      String collectionName,
      GroupStage groupStage,
      List<MatchStage> matchStages,
      PipelineComponents parentComponents,
      SqlGenerationContext ctx) {
    ctx.sql("SELECT ");

    // Render _id if present
    boolean first = true;
    if (groupStage.getIdExpression() != null) {
      ctx.visit(groupStage.getIdExpression());
      ctx.sql(" AS ");
      ctx.identifier("_id");
      first = false;
    }

    // Render accumulators
    for (var entry : groupStage.getAccumulators().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.visit(entry.getValue());
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
      first = false;
    }

    ctx.sql(" FROM ");
    ctx.tableName(collectionName);
    ctx.sql(" ");
    ctx.sql(ctx.getBaseTableAlias());

    // Include match conditions from parent pipeline (pre-facet filters)
    List<MatchStage> allMatches = new ArrayList<>(parentComponents.matchStages);
    allMatches.addAll(matchStages);

    if (!allMatches.isEmpty()) {
      ctx.sql(" WHERE ");
      boolean firstMatch = true;
      for (MatchStage match : allMatches) {
        if (!firstMatch) {
          ctx.sql(" AND ");
        }
        ctx.visit(match.getFilter());
        firstMatch = false;
      }
    }

    // GROUP BY clause
    if (groupStage.getIdExpression() != null) {
      ctx.sql(" GROUP BY ");
      ctx.visit(groupStage.getIdExpression());
    }
  }

  /** Renders a facet pipeline that contains a $project stage (without $group). */
  private void renderFacetProjectQuery(
      String collectionName,
      ProjectStage projectStage,
      List<MatchStage> matchStages,
      SortStage sortStage,
      LimitStage limitStage,
      PipelineComponents parentComponents,
      SqlGenerationContext ctx) {
    ctx.sql("SELECT ");

    boolean first = true;
    for (var entry : projectStage.getProjections().entrySet()) {
      String alias = entry.getKey();
      ProjectStage.ProjectionField field = entry.getValue();

      if (field.isExcluded()) {
        continue;
      }

      if (!first) {
        ctx.sql(", ");
      }

      // Handle _id specially - use the id column
      if ("_id".equals(alias)) {
        ctx.sql("id");
      } else if (field.getExpression() != null) {
        ctx.visit(field.getExpression());
      }
      ctx.sql(" AS ");
      ctx.identifier(alias);
      first = false;
    }

    ctx.sql(" FROM ");
    ctx.tableName(collectionName);
    ctx.sql(" ");
    ctx.sql(ctx.getBaseTableAlias());

    // Include match conditions from parent pipeline (pre-facet filters)
    List<MatchStage> allMatches = new ArrayList<>(parentComponents.matchStages);
    allMatches.addAll(matchStages);

    if (!allMatches.isEmpty()) {
      ctx.sql(" WHERE ");
      boolean firstMatch = true;
      for (MatchStage match : allMatches) {
        if (!firstMatch) {
          ctx.sql(" AND ");
        }
        ctx.visit(match.getFilter());
        firstMatch = false;
      }
    }

    // ORDER BY
    if (sortStage != null && !sortStage.getSortFields().isEmpty()) {
      ctx.sql(" ORDER BY ");
      boolean firstSort = true;
      for (SortStage.SortField field : sortStage.getSortFields()) {
        if (!firstSort) {
          ctx.sql(", ");
        }
        ctx.visit(field.getFieldPath());
        if (field.getDirection() == SortStage.SortDirection.DESC) {
          ctx.sql(" DESC");
        }
        firstSort = false;
      }
    }

    // LIMIT
    if (limitStage != null) {
      ctx.sql(" FETCH FIRST ");
      ctx.sql(String.valueOf(limitStage.getLimit()));
      ctx.sql(" ROWS ONLY");
    }
  }

  /**
   * Renders a facet sub-pipeline that contains a $count stage.
   * Used for patterns like: recordCount: [{$count: "count"}]
   * When there's a pre-facet $group, this counts the grouped rows.
   */
  private void renderFacetCountQuery(
      String collectionName,
      CountStage countStage,
      List<MatchStage> matchStages,
      PipelineComponents parentComponents,
      SqlGenerationContext ctx) {
    // Return JSON_ARRAYAGG with a single JSON_OBJECT containing the count
    ctx.sql("SELECT JSON_ARRAYAGG(JSON_OBJECT('");
    ctx.sql(countStage.getFieldName());
    ctx.sql("' VALUE cnt)) FROM (SELECT COUNT(*) AS cnt FROM (");

    // Inner query: the data to count
    if (parentComponents.groupStage != null) {
      // Count the grouped rows
      renderPreFacetGroupQuery(collectionName, parentComponents, ctx);
    } else {
      // Count raw collection rows (with any match filters)
      ctx.sql("SELECT 1 FROM ");
      ctx.tableName(collectionName);
      ctx.sql(" ");
      ctx.sql(ctx.getBaseTableAlias());

      // Apply parent match stages
      List<MatchStage> allMatches = new ArrayList<>(parentComponents.matchStages);
      allMatches.addAll(matchStages);

      if (!allMatches.isEmpty()) {
        ctx.sql(" WHERE ");
        boolean firstMatch = true;
        for (MatchStage match : allMatches) {
          if (!firstMatch) {
            ctx.sql(" AND ");
          }
          ctx.visit(match.getFilter());
          firstMatch = false;
        }
      }
    }

    ctx.sql("))");
  }

  /**
   * Renders a facet sub-pipeline that paginates over pre-facet grouped data.
   * Used for patterns like: data: [{$skip: 0}, {$limit: 5}]
   * when there's a $group stage before the $facet.
   */
  private void renderFacetPaginationQuery(
      String collectionName,
      SkipStage skipStage,
      LimitStage limitStage,
      SortStage sortStage,
      List<MatchStage> matchStages,
      PipelineComponents parentComponents,
      SqlGenerationContext ctx) {
    GroupStage parentGroup = parentComponents.groupStage;

    // Return JSON_ARRAYAGG of JSON_OBJECT with all fields from the grouped data
    ctx.sql("SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE \"_id\"");

    // Include all accumulator fields in the JSON_OBJECT
    if (parentGroup != null) {
      for (String accName : parentGroup.getAccumulators().keySet()) {
        ctx.sql(", '");
        ctx.sql(accName);
        ctx.sql("' VALUE ");
        ctx.identifier(accName);
      }
    }

    // Close JSON_OBJECT before ORDER BY
    ctx.sql(")");

    // Add ORDER BY inside JSON_ARRAYAGG if there's a sort
    if (sortStage != null && !sortStage.getSortFields().isEmpty()) {
      ctx.sql(" ORDER BY ");
      boolean firstSort = true;
      for (SortStage.SortField field : sortStage.getSortFields()) {
        if (!firstSort) {
          ctx.sql(", ");
        }
        // Reference by column identifier, not base.data path
        ctx.identifier(field.getFieldPath().getPath());
        if (field.getDirection() == SortStage.SortDirection.DESC) {
          ctx.sql(" DESC");
        }
        firstSort = false;
      }
    }

    // Close JSON_ARRAYAGG and start FROM clause
    ctx.sql(") FROM (");

    // Inner query: the grouped data with pagination
    renderPreFacetGroupQuery(collectionName, parentComponents, ctx);

    // Add ORDER BY for pagination if specified - use column identifiers
    if (sortStage != null && !sortStage.getSortFields().isEmpty()) {
      ctx.sql(" ORDER BY ");
      boolean firstSort = true;
      for (SortStage.SortField field : sortStage.getSortFields()) {
        if (!firstSort) {
          ctx.sql(", ");
        }
        // Reference by column identifier, not base.data path
        ctx.identifier(field.getFieldPath().getPath());
        if (field.getDirection() == SortStage.SortDirection.DESC) {
          ctx.sql(" DESC");
        }
        firstSort = false;
      }
    }

    // Apply OFFSET/FETCH
    if (skipStage != null && skipStage.getSkip() > 0) {
      ctx.sql(" OFFSET ");
      ctx.sql(String.valueOf(skipStage.getSkip()));
      ctx.sql(" ROWS");
    }
    if (limitStage != null) {
      ctx.sql(" FETCH FIRST ");
      ctx.sql(String.valueOf(limitStage.getLimit()));
      ctx.sql(" ROWS ONLY");
    }

    ctx.sql(")");
  }

  /**
   * Renders the pre-facet grouped data as a subquery.
   * This represents the result of applying $match and $group before $facet.
   */
  private void renderPreFacetGroupQuery(
      String collectionName,
      PipelineComponents parentComponents,
      SqlGenerationContext ctx) {
    GroupStage groupStage = parentComponents.groupStage;

    ctx.sql("SELECT ");

    // Render the _id expression
    Expression idExpr = groupStage.getIdExpression();
    if (idExpr != null) {
      if (idExpr instanceof CompoundIdExpression compoundId) {
        // Compound _id: render as JSON_OBJECT to preserve structure
        // e.g., JSON_OBJECT('field1' VALUE expr1, 'field2' VALUE expr2)
        ctx.sql("JSON_OBJECT(");
        boolean first = true;
        for (var entry : compoundId.getFields().entrySet()) {
          if (!first) {
            ctx.sql(", ");
          }
          ctx.sql("'");
          ctx.sql(entry.getKey());
          ctx.sql("' VALUE ");
          ctx.visit(entry.getValue());
          first = false;
        }
        ctx.sql(")");
      } else {
        ctx.visit(idExpr);
      }
      ctx.sql(" AS ");
      ctx.identifier("_id");
    } else {
      ctx.sql("NULL AS ");
      ctx.identifier("_id");
    }

    // Render any accumulators from the group stage
    for (var entry : groupStage.getAccumulators().entrySet()) {
      ctx.sql(", ");
      ctx.visit(entry.getValue());
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
    }

    ctx.sql(" FROM ");
    ctx.tableName(collectionName);
    ctx.sql(" ");
    ctx.sql(ctx.getBaseTableAlias());

    // Apply parent match stages
    if (!parentComponents.matchStages.isEmpty()) {
      ctx.sql(" WHERE ");
      boolean firstMatch = true;
      for (MatchStage match : parentComponents.matchStages) {
        if (!firstMatch) {
          ctx.sql(" AND ");
        }
        ctx.visit(match.getFilter());
        firstMatch = false;
      }
    }

    // GROUP BY clause
    if (idExpr != null) {
      ctx.sql(" GROUP BY ");
      ctx.visit(idExpr);
    }
  }

  private void renderFromClause(PipelineComponents components, SqlGenerationContext ctx) {
    ctx.sql(" FROM ");

    // $facet uses FROM DUAL since all data comes from subqueries
    if (components.facetStage != null) {
      ctx.sql("DUAL");
      return;
    }

    // Use collection name from pipeline if different from config (e.g., for $unionWith subqueries),
    // otherwise use the config's qualified table name (which includes schema if configured)
    if (components.collectionName != null
        && !components.collectionName.equals(config.collectionName())) {
      // Different collection (e.g., union subquery) - use just the collection name
      ctx.tableName(components.collectionName);
    } else {
      // Same collection or no collection specified - use config's qualified name
      ctx.sql(config.qualifiedTableName());
    }

    // Always add alias to disambiguate table references
    // This ensures JSON_VALUE(base.data, ...) works consistently
    ctx.sql(" ");
    ctx.sql(ctx.getBaseTableAlias());

    // Render unwind stages as joins with JSON_TABLE
    // When preserveNullAndEmptyArrays is true, use LEFT OUTER JOIN
    // to preserve rows with null/empty arrays
    // Skip unwinds that are on $lookup result fields - the JOIN handles them
    for (UnwindStage unwind : components.unwindStages) {
      // Check if this unwind is on a $lookup result field
      if (isUnwindOnLookupField(unwind.getPath(), components)) {
        // Skip - the $lookup JOIN already produces the correct rows
        continue;
      }
      if (unwind.isPreserveNullAndEmptyArrays()) {
        ctx.sql(" LEFT OUTER JOIN ");
        ctx.visit(unwind);
        ctx.sql(" ON 1=1");
      } else {
        ctx.sql(", ");
        ctx.visit(unwind);
      }
    }
  }

  /** Checks if an $unwind path refers to a $lookup result field. */
  private boolean isUnwindOnLookupField(String unwindPath, PipelineComponents components) {
    for (LookupStage lookup : components.lookupStages) {
      // $unwind: "$customer" matches $lookup { as: "customer" }
      if (unwindPath.equals(lookup.getAs()) || unwindPath.startsWith(lookup.getAs() + ".")) {
        return true;
      }
    }
    return false;
  }

  private void renderJoinClauses(PipelineComponents components, SqlGenerationContext ctx) {
    for (LookupStage lookup : components.lookupStages) {
      // Skip lookups that were fully consumed by $size (use correlated subquery instead)
      if (ctx.isLookupConsumedBySize(lookup.getAs())) {
        continue;
      }
      ctx.sql(" ");
      ctx.visit(lookup);
    }
  }

  private void renderWhereClause(PipelineComponents components, SqlGenerationContext ctx) {
    List<Expression> allFilters = new ArrayList<>();

    // Collect all match filters
    for (MatchStage match : components.matchStages) {
      allFilters.add(match.getFilter());
    }

    // Collect redact filters - $redact filters based on PRUNE/KEEP/DESCEND
    // The condition that returns PRUNE should exclude the document
    for (RedactStage redact : components.redactStages) {
      // Redact expression evaluates to $$PRUNE, $$KEEP, or $$DESCEND
      // We filter where the result != '$$PRUNE'
      Expression redactFilter =
          new ComparisonExpression(
              ComparisonOp.NE, redact.getExpression(), LiteralExpression.of("$$PRUNE"));
      allFilters.add(redactFilter);
    }

    if (allFilters.isEmpty()) {
      return;
    }

    ctx.sql(" WHERE ");

    if (allFilters.size() == 1) {
      ctx.visit(allFilters.get(0));
    } else {
      LogicalExpression combined = new LogicalExpression(LogicalOp.AND, allFilters);
      ctx.visit(combined);
    }
  }

  private void renderGroupByClause(PipelineComponents components, SqlGenerationContext ctx) {
    if (components.groupStage != null && components.groupStage.getIdExpression() != null) {
      ctx.sql(" GROUP BY ");
      ctx.visit(components.groupStage.getIdExpression());
    } else if (components.bucketStage != null) {
      // For $bucket, GROUP BY the CASE expression
      ctx.sql(" GROUP BY ");
      renderBucketCaseExpression(components.bucketStage, ctx);
    } else if (components.bucketAutoStage != null) {
      // For $bucketAuto, GROUP BY the NTILE result
      ctx.sql(" GROUP BY ");
      ctx.sql("NTILE(");
      ctx.sql(String.valueOf(components.bucketAutoStage.getBuckets()));
      ctx.sql(") OVER (ORDER BY ");
      ctx.visit(components.bucketAutoStage.getGroupBy());
      ctx.sql(")");
    }
  }

  private void renderBucketCaseExpression(BucketStage bucket, SqlGenerationContext ctx) {
    // Check if we have mixed types (boundaries are numbers but default is string)
    // Oracle's CASE requires all branches to return compatible types
    boolean needsStringCast = bucketHasMixedTypes(bucket);

    ctx.sql("CASE");
    var boundaries = bucket.getBoundaries();
    for (int i = 0; i < boundaries.size() - 1; i++) {
      final Object lower = boundaries.get(i);
      final Object upper = boundaries.get(i + 1);
      ctx.sql(" WHEN ");
      ctx.visit(bucket.getGroupBy());
      ctx.sql(" >= ");
      renderBucketLiteral(ctx, lower, false);
      ctx.sql(" AND ");
      ctx.visit(bucket.getGroupBy());
      ctx.sql(" < ");
      renderBucketLiteral(ctx, upper, false);
      ctx.sql(" THEN ");
      renderBucketLiteral(ctx, lower, needsStringCast);
    }
    if (bucket.hasDefault()) {
      ctx.sql(" ELSE ");
      renderBucketLiteral(ctx, bucket.getDefaultBucket(), needsStringCast);
    }
    ctx.sql(" END");
  }

  /**
   * Checks if bucket stage has mixed types (numeric boundaries with string default or vice versa).
   */
  private boolean bucketHasMixedTypes(BucketStage bucket) {
    if (!bucket.hasDefault() || bucket.getBoundaries().isEmpty()) {
      return false;
    }
    boolean boundariesAreNumeric = bucket.getBoundaries().get(0) instanceof Number;
    boolean defaultIsNumeric = bucket.getDefaultBucket() instanceof Number;
    return boundariesAreNumeric != defaultIsNumeric;
  }

  private void renderBucketLiteral(SqlGenerationContext ctx, Object value, boolean forceString) {
    if (value instanceof String) {
      ctx.sql("'");
      ctx.sql(((String) value).replace("'", "''"));
      ctx.sql("'");
    } else if (value instanceof Number) {
      if (forceString) {
        ctx.sql("'");
        ctx.sql(value.toString());
        ctx.sql("'");
      } else {
        ctx.sql(value.toString());
      }
    } else if (value == null) {
      ctx.sql("NULL");
    } else {
      ctx.sql("'");
      ctx.sql(value.toString().replace("'", "''"));
      ctx.sql("'");
    }
  }

  private void renderOrderByClause(PipelineComponents components, SqlGenerationContext ctx) {
    // $sample uses random ordering
    if (components.sampleStage != null) {
      ctx.sql(" ORDER BY DBMS_RANDOM.VALUE");
      return;
    }

    if (components.sortStage == null || components.sortStage.getSortFields().isEmpty()) {
      return;
    }

    ctx.sql(" ORDER BY ");

    boolean first = true;
    for (SortStage.SortField field : components.sortStage.getSortFields()) {
      if (!first) {
        ctx.sql(", ");
      }
      // After GROUP BY, sort fields refer to aliases, not JSON paths
      if (components.groupStage != null
          || components.bucketStage != null
          || components.bucketAutoStage != null) {
        ctx.identifier(field.getFieldPath().getPath());
      } else {
        // Check if this is a computed field from $project
        Expression computedExpr = getComputedProjectExpression(components, field.getFieldPath());
        if (computedExpr != null) {
          // Use the computed expression for sorting
          ctx.visit(computedExpr);
        } else {
          ctx.visit(field.getFieldPath());
        }
      }
      if (field.getDirection() == SortStage.SortDirection.DESC) {
        ctx.sql(" DESC");
      }
      first = false;
    }
  }

  /**
   * Returns the computed expression from $project if the sort field is a computed field (not a
   * simple field reference). Returns null if there's no $project, the field isn't in the project,
   * or the field is a simple field reference (not computed).
   */
  private Expression getComputedProjectExpression(
      PipelineComponents components, FieldPathExpression sortField) {
    if (components.projectStage == null) {
      return null;
    }

    String fieldName = sortField.getPath();
    ProjectStage.ProjectionField projection =
        components.projectStage.getProjections().get(fieldName);

    if (projection == null || projection.isExcluded()) {
      return null;
    }

    Expression projExpr = projection.getExpression();

    // Check if it's a simple field reference (not computed)
    // A simple reference is like { name: 1 } which translates to FieldPathExpression("name")
    if (projExpr instanceof FieldPathExpression fieldPathExpr) {
      // If the projection just references the same field, it's not computed
      if (fieldPathExpr.getPath().equals(fieldName)) {
        return null;
      }
    }

    // This is a computed field - return the expression
    return projExpr;
  }

  private void renderOffsetClause(PipelineComponents components, SqlGenerationContext ctx) {
    if (components.skipStage == null) {
      return;
    }

    ctx.sql(" OFFSET ");
    ctx.sql(String.valueOf(components.skipStage.getSkip()));
    ctx.sql(" ROWS");
  }

  private void renderFetchClause(PipelineComponents components, SqlGenerationContext ctx) {
    // $sample limits result count
    if (components.sampleStage != null) {
      ctx.sql(" FETCH FIRST ");
      ctx.sql(String.valueOf(components.sampleStage.getSize()));
      ctx.sql(" ROWS ONLY");
      return;
    }

    if (components.limitStage == null) {
      return;
    }

    ctx.sql(" FETCH FIRST ");
    ctx.sql(String.valueOf(components.limitStage.getLimit()));
    ctx.sql(" ROWS ONLY");
  }

  private void renderUnionWithClauses(PipelineComponents components, SqlGenerationContext ctx) {
    for (UnionWithStage unionWith : components.unionWithStages) {
      // Validate table name to prevent injection
      FieldNameValidator.validateTableName(unionWith.getCollection());

      ctx.sql(" UNION ALL ");

      // If the unionWith has a pipeline, we need to render it as a subquery
      if (unionWith.hasPipeline()) {
        // Create a new Pipeline for the union collection and render it
        // Mark as nested pipeline so it doesn't use JSON_ARRAYAGG pattern
        Pipeline unionPipeline = Pipeline.of(unionWith.getCollection(), unionWith.getPipeline());
        boolean wasNested = ctx.isNestedPipeline();
        ctx.setNestedPipeline(true);
        render(unionPipeline, ctx);
        ctx.setNestedPipeline(wasNested);
      } else {
        // Simple union - just select from the collection
        ctx.sql("SELECT ");
        ctx.sql(config.dataColumnName());
        ctx.sql(" FROM ");
        ctx.tableName(unionWith.getCollection());
      }
    }
  }

  /**
   * Renders ORDER BY and FETCH FIRST clauses that apply to the whole union result. These are
   * $sort/$limit stages that came AFTER $unionWith in the original pipeline.
   */
  private void renderPostUnionSortAndLimit(
      PipelineComponents components, SqlGenerationContext ctx) {
    if (!components.hasPostUnionSortOrLimit) {
      return;
    }

    // Render ORDER BY for post-union sort
    if (components.postUnionSortStage != null) {
      ctx.sql(" ORDER BY ");
      // Enable JSON output mode to quote identifiers consistently with UNION SELECT aliases
      // UNION queries use quoted column aliases, so ORDER BY must also use quoted identifiers
      boolean wasJsonMode = ctx.isJsonOutputMode();
      ctx.setJsonOutputMode(true);
      boolean first = true;
      for (SortStage.SortField sortField : components.postUnionSortStage.getSortFields()) {
        if (!first) {
          ctx.sql(", ");
        }
        // For union results, we reference the column aliases directly
        ctx.identifier(sortField.getFieldPath().getPath());
        ctx.sql(sortField.getDirection() == SortStage.SortDirection.ASC ? " ASC" : " DESC");
        first = false;
      }
      ctx.setJsonOutputMode(wasJsonMode);
    }

    // Render FETCH FIRST for post-union limit
    if (components.postUnionLimitStage != null) {
      ctx.sql(" FETCH FIRST ");
      ctx.sql(String.valueOf(components.postUnionLimitStage.getLimit()));
      ctx.sql(" ROWS ONLY");
    }
  }

  /**
   * Checks if a $match stage references any window output fields.
   *
   * @param match the match stage to check
   * @param components the pipeline components containing window field definitions
   * @return true if the match references window output fields
   */
  private boolean matchReferencesWindowFields(MatchStage match, PipelineComponents components) {
    // Get all window output field names
    java.util.Set<String> windowFieldNames = new java.util.HashSet<>();
    for (SetWindowFieldsStage swf : components.setWindowFieldsStages) {
      windowFieldNames.addAll(swf.getOutput().keySet());
    }

    if (windowFieldNames.isEmpty()) {
      return false;
    }

    // Check if the match expression references any of these fields
    return expressionReferencesFields(match.getFilter(), windowFieldNames);
  }

  /** Recursively checks if an expression references any of the given field names. */
  private boolean expressionReferencesFields(Expression expr, java.util.Set<String> fieldNames) {
    if (expr instanceof FieldPathExpression fieldPath) {
      return fieldNames.contains(fieldPath.getPath());
    } else if (expr instanceof ComparisonExpression comp) {
      return expressionReferencesFields(comp.getLeft(), fieldNames)
          || expressionReferencesFields(comp.getRight(), fieldNames);
    } else if (expr instanceof LogicalExpression logical) {
      for (Expression operand : logical.getOperands()) {
        if (expressionReferencesFields(operand, fieldNames)) {
          return true;
        }
      }
      return false;
    }
    // For other expression types, assume they might reference fields
    // This is conservative but safe
    return false;
  }

  /**
   * Quotes a field path for Oracle dot notation. Segments that start with underscore or digit need
   * quoting since Oracle identifiers must start with a letter when unquoted.
   */
  private static String quotePath(String path) {
    String[] segments = path.split("\\.");
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        result.append(".");
      }
      String segment = segments[i];
      if (!segment.isEmpty() && !Character.isLetter(segment.charAt(0))) {
        result.append("\"").append(segment).append("\"");
      } else {
        result.append(segment);
      }
    }
    return result.toString();
  }

  /** Holds the decomposed components of a pipeline. */
  private static class PipelineComponents {
    String collectionName; // The collection name from the pipeline
    List<MatchStage> matchStages = new ArrayList<>();
    List<MatchStage> postWindowMatchStages = new ArrayList<>(); // $match after $setWindowFields
    List<LookupStage> lookupStages = new ArrayList<>();
    List<UnwindStage> unwindStages = new ArrayList<>();
    List<AddFieldsStage> addFieldsStages = new ArrayList<>();
    List<AddFieldsStage> postGroupAddFieldsStages = new ArrayList<>(); // $addFields after $group
    List<UnionWithStage> unionWithStages = new ArrayList<>();
    List<GraphLookupStage> graphLookupStages = new ArrayList<>();
    List<SetWindowFieldsStage> setWindowFieldsStages = new ArrayList<>();
    List<SetWindowFieldsStage> postGroupSetWindowFieldsStages =
        new ArrayList<>(); // $setWindowFields after $group
    List<RedactStage> redactStages = new ArrayList<>();
    List<GroupStage> allGroupStages = new ArrayList<>(); // All group stages for CTE detection
    GroupStage groupStage; // Last/primary group stage for backward compatibility
    PipelineStageSequence stageSequence; // Stage sequence analysis for CTE generation
    ProjectStage projectStage;
    BucketStage bucketStage;
    BucketAutoStage bucketAutoStage;
    FacetStage facetStage;
    SortStage sortStage;
    SkipStage skipStage;
    LimitStage limitStage;
    CountStage countStage;
    SampleStage sampleStage;
    ReplaceRootStage replaceRootStage;
    OutStage outStage; // Terminal $out stage that writes results to another collection
    MergeStage mergeStage; // Terminal $merge stage that merges results into another collection
    ProjectStage postFacetProjectStage; // $project after $facet that reshapes facet output
    boolean hasPostGroupAddFields = false; // Track if $addFields comes after $group
    boolean hasPostGroupSetWindowFields = false; // Track if $setWindowFields comes after $group
    boolean hasPostWindowMatch = false; // Track if $match comes after $setWindowFields
    boolean hasPostUnionSortOrLimit = false; // Track if $sort/$limit come after $unionWith
    boolean hasPostUnionGroup = false; // Track if $group comes after $unionWith
    SortStage postUnionSortStage; // $sort after $unionWith (applied to whole union result)
    LimitStage postUnionLimitStage; // $limit after $unionWith (applied to whole union result)
    GroupStage postUnionGroupStage; // $group after $unionWith (aggregates whole union result)
  }
}
