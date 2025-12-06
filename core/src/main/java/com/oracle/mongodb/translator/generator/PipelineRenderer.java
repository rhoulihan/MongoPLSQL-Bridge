/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.ArrayExpression;
import com.oracle.mongodb.translator.ast.expression.ArrayOp;
import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticOp;
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
      }
    }

    // Pre-register unwind paths so field paths can resolve correctly
    // After $unwind: "$items", references like "$items.product" should access the JSON_TABLE column
    for (UnwindStage unwind : components.unwindStages) {
      // Skip unwinds on lookup fields - they don't produce separate JSON_TABLE joins
      if (!isUnwindOnLookupField(unwind.getPath(), components)) {
        String alias = ctx.generateTableAlias("unwind");
        ctx.registerUnwoundPath(unwind.getPath(), alias);
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

    // Collect computed field names from $addFields and $setWindowFields
    Set<String> computedFieldNames = new HashSet<>();
    for (AddFieldsStage addFields : components.addFieldsStages) {
      computedFieldNames.addAll(addFields.getFields().keySet());
    }
    for (SetWindowFieldsStage swf : components.setWindowFieldsStages) {
      computedFieldNames.addAll(swf.getOutput().keySet());
    }

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

    // Render $addFields computed columns
    for (AddFieldsStage addFields : components.addFieldsStages) {
      if (!addFields.getFields().isEmpty()) {
        ctx.sql(", ");
        ctx.visit(addFields);
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
    // Outer SELECT: columns from inner query + computed fields from $addFields
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

    // Inner query: the GROUP BY query
    renderSelectClause(components, ctx);
    renderFromClause(components, ctx);
    renderJoinClauses(components, ctx);
    renderGraphLookupJoins(components, ctx);
    renderWhereClause(components, ctx);
    renderGroupByClause(components, ctx);

    ctx.sql(") inner_query");

    // ORDER BY, OFFSET, FETCH apply to the outer query
    renderOrderByClauseForOuterQuery(components, ctx);
    renderOffsetClause(components, ctx);
    renderFetchClause(components, ctx);
  }

  /**
   * Renders a query with $bucketAuto. Since NTILE is a window function and cannot be used directly
   * in GROUP BY, we use a subquery pattern:
   *
   * <pre>
   * SELECT bucket_id, COUNT(*) AS cnt, aggregations...
   * FROM (
   *   SELECT field AS field_alias, NTILE(n) OVER (ORDER BY field) AS bucket_id
   *   FROM table WHERE ...
   * )
   * GROUP BY bucket_id
   * ORDER BY bucket_id
   * </pre>
   */
  private void renderWithBucketAuto(PipelineComponents components, SqlGenerationContext ctx) {
    BucketAutoStage bucketAuto = components.bucketAutoStage;

    // Outer SELECT: bucket_id and aggregations
    ctx.sql("SELECT bucket_id");

    // Render output accumulators
    for (Map.Entry<String, AccumulatorExpression> entry : bucketAuto.getOutput().entrySet()) {
      ctx.sql(", ");
      String alias = entry.getKey();
      AccumulatorExpression acc = entry.getValue();

      // Render the accumulator with the correct field reference
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

    // FROM subquery
    ctx.sql(" FROM (SELECT ");

    // Inner query: the groupBy field value and NTILE
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

    ctx.sql(")"); // Close subquery

    // GROUP BY bucket_id
    ctx.sql(" GROUP BY bucket_id");

    // ORDER BY bucket_id
    ctx.sql(" ORDER BY bucket_id");
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
    renderPostGroupExpression(comp.getLeft(), ctx);
    ctx.sql(" ");
    ctx.sql(comp.getOp().getSqlOperator());
    ctx.sql(" ");
    renderPostGroupExpression(comp.getRight(), ctx);
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
    // GraphLookup uses LATERAL joins, not CTEs (since CTEs can't reference outer query columns)
    // So we don't render CTE clause for graphLookup stages
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
   * Renders a recursive $graphLookup. Note: Recursive graph lookups (maxDepth > 0) have Oracle
   * limitations:
   *
   * <ul>
   *   <li>Recursive CTEs inside LATERAL cannot reference outer table columns (ORA-00904)
   *   <li>CONNECT BY with PRIOR doesn't work with JSON dot notation (ORA-19200)
   * </ul>
   *
   * <p>As a result, recursive $graphLookup produces a placeholder query that returns empty results.
   * Tests using recursive $graphLookup should be marked as skipped.
   */
  private void renderRecursiveGraphLookup(
      GraphLookupStage graphLookup,
      String connectToField,
      String connectFromField,
      String startField,
      SqlGenerationContext ctx) {
    // Due to Oracle limitations, recursive $graphLookup returns empty results:
    // 1. Recursive CTEs inside LATERAL can't reference outer columns (ORA-00904)
    // 2. CONNECT BY with PRIOR doesn't work with JSON dot notation (ORA-19200)
    // Using a LATERAL subquery that returns an empty array as a placeholder
    ctx.sql(" LEFT OUTER JOIN LATERAL (SELECT ");
    ctx.sql("CAST(NULL AS JSON) AS ");
    ctx.identifier(graphLookup.getAs());

    // Add depth field if specified
    if (graphLookup.getDepthField() != null) {
      ctx.sql(", NULL AS ");
      ctx.identifier(graphLookup.getDepthField());
    }

    ctx.sql(" FROM DUAL WHERE 1=0) ");
    ctx.identifier(graphLookup.getAs() + "_cte");
    ctx.sql(" ON 1=1");
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
          components.groupStage = group;
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
        components.setWindowFieldsStages.add(setWindowFields);
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

    return components;
  }

  private void renderSelectClause(PipelineComponents components, SqlGenerationContext ctx) {
    ctx.sql("SELECT ");

    if (components.countStage != null) {
      // $count returns a single document with the count
      ctx.sql("JSON_OBJECT('");
      ctx.sql(components.countStage.getFieldName());
      ctx.sql("' VALUE COUNT(*)) AS ");
      ctx.sql(config.dataColumnName());
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
    for (AddFieldsStage addFields : components.addFieldsStages) {
      if (!addFields.getFields().isEmpty()) {
        ctx.sql(", ");
        ctx.visit(addFields);
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

  private void renderGroupSelectClause(GroupStage group, SqlGenerationContext ctx) {
    boolean first = true;

    // Render _id expression if present
    if (group.getIdExpression() != null) {
      var idExpr = group.getIdExpression();
      if (idExpr instanceof CompoundIdExpression compound) {
        // For compound _id, render each field with its alias
        compound.renderWithAliases(ctx);
      } else {
        ctx.visit(idExpr);
        ctx.sql(" AS ");
        ctx.identifier("_id");
      }
      first = false;
    }

    // Render accumulators
    for (var entry : group.getAccumulators().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.visit(entry.getValue());
      ctx.sql(" AS ");
      ctx.identifier(entry.getKey());
      first = false;
    }

    // If nothing was rendered, select a placeholder
    if (first) {
      ctx.sql("NULL AS dummy");
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

    ctx.sql("JSON_OBJECT(");
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

    ctx.sql(") AS ");
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

    ctx.sql("JSON_OBJECT(");
    boolean first = true;

    for (var entry : postProject.getProjections().entrySet()) {
      String outputFieldName = entry.getKey();
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

    ctx.sql(") AS ");
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
    String jsonPath = "$." + nestedField;

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
          // We need JSON_VALUE(..., '$[0].count')
          ctx.sql("JSON_VALUE((");
          renderFacetPipeline(collectionName, pipeline, components, ctx);
          ctx.sql("), '$[");
          ctx.sql(String.valueOf(index));
          ctx.sql("].");
          ctx.sql(fieldName);
          ctx.sql("')");
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
    if (groupStage.getIdExpression() != null) {
      ctx.visit(groupStage.getIdExpression());
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
    if (groupStage.getIdExpression() != null) {
      ctx.sql(" GROUP BY ");
      ctx.visit(groupStage.getIdExpression());
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
    ctx.sql("CASE");
    var boundaries = bucket.getBoundaries();
    for (int i = 0; i < boundaries.size() - 1; i++) {
      final Object lower = boundaries.get(i);
      final Object upper = boundaries.get(i + 1);
      ctx.sql(" WHEN ");
      ctx.visit(bucket.getGroupBy());
      ctx.sql(" >= ");
      renderBucketLiteral(ctx, lower);
      ctx.sql(" AND ");
      ctx.visit(bucket.getGroupBy());
      ctx.sql(" < ");
      renderBucketLiteral(ctx, upper);
      ctx.sql(" THEN ");
      renderBucketLiteral(ctx, lower);
    }
    if (bucket.hasDefault()) {
      ctx.sql(" ELSE ");
      renderBucketLiteral(ctx, bucket.getDefaultBucket());
    }
    ctx.sql(" END");
  }

  private void renderBucketLiteral(SqlGenerationContext ctx, Object value) {
    if (value instanceof String) {
      ctx.sql("'");
      ctx.sql(((String) value).replace("'", "''"));
      ctx.sql("'");
    } else if (value instanceof Number) {
      ctx.sql(value.toString());
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
        ctx.visit(field.getFieldPath());
      }
      if (field.getDirection() == SortStage.SortDirection.DESC) {
        ctx.sql(" DESC");
      }
      first = false;
    }
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
    List<RedactStage> redactStages = new ArrayList<>();
    GroupStage groupStage;
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
    boolean hasPostWindowMatch = false; // Track if $match comes after $setWindowFields
    boolean hasPostUnionSortOrLimit = false; // Track if $sort/$limit come after $unionWith
    boolean hasPostUnionGroup = false; // Track if $group comes after $unionWith
    SortStage postUnionSortStage; // $sort after $unionWith (applied to whole union result)
    LimitStage postUnionLimitStage; // $limit after $unionWith (applied to whole union result)
    GroupStage postUnionGroupStage; // $group after $unionWith (aggregates whole union result)
  }
}
