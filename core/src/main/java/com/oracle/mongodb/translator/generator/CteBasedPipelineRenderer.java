/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.AccumulatorOp;
import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticOp;
import com.oracle.mongodb.translator.ast.expression.ArrayExpression;
import com.oracle.mongodb.translator.ast.expression.ArrayOp;
import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.CompoundIdExpression;
import com.oracle.mongodb.translator.ast.expression.ConditionalExpression;
import com.oracle.mongodb.translator.ast.expression.DateExpression;
import com.oracle.mongodb.translator.ast.expression.DateOp;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.InExpression;
import com.oracle.mongodb.translator.ast.expression.InlineObjectExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.expression.ObjectExpression;
import com.oracle.mongodb.translator.ast.expression.ObjectOp;
import com.oracle.mongodb.translator.ast.expression.StringExpression;
import com.oracle.mongodb.translator.ast.expression.StringOp;
import com.oracle.mongodb.translator.ast.expression.SwitchExpression;
import com.oracle.mongodb.translator.ast.expression.TypeConversionExpression;
import com.oracle.mongodb.translator.ast.expression.TypeConversionOp;
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
import com.oracle.mongodb.translator.ast.stage.RedactStage;
import com.oracle.mongodb.translator.ast.stage.ReplaceRootStage;
import com.oracle.mongodb.translator.ast.stage.SampleStage;
import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.ast.stage.UnionWithStage;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Renders a MongoDB aggregation pipeline using CTE-based SQL following Oracle MongoDB API patterns.
 *
 * <p>This renderer generates SQL using:
 * <ul>
 *   <li>WITH clause where each pipeline stage becomes a CTE (Q1, Q2, Q3, etc.)</li>
 *   <li>JSON_EXISTS with type-safe predicates (stringOnly, numberOnly, etc.)</li>
 *   <li>json_transform for projections (KEEP, SET, REMOVE)</li>
 *   <li>DATA column preservation (returns JSON, not extracted columns)</li>
 * </ul>
 *
 * <p>Example output:
 * <pre>
 * WITH "Q1" ("ID", "DATA") AS (
 *   SELECT "ID", "DATA" FROM "orders"
 * ), "Q2" ("DATA") AS (
 *   SELECT "DATA" FROM "Q1" q
 *   WHERE JSON_EXISTS("DATA", '$?(@.status.stringOnly() == $B0)'
 *         PASSING :1 AS "B0" TYPE(strict))
 * )
 * SELECT "DATA" FROM "Q2"
 * </pre>
 */
public final class CteBasedPipelineRenderer {

  private final OracleConfiguration config;

  /**
   * Tracks field names that were set to numeric expressions in previous $addFields stages.
   * Used to determine whether to use RETURNING NUMBER when referencing these fields
   * in compound _id expressions.
   */
  private final java.util.Set<String> knownNumericFields = new java.util.HashSet<>();

  /**
   * Tracks the facet names from the most recent $facet stage.
   * Used to determine when a dotted field path like "$data._id" refers to a facet array
   * and should use [*] syntax to extract nested fields from each array element.
   * Cleared when we move past post-facet stages.
   */
  private final java.util.Set<String> activeFacetNames = new java.util.HashSet<>();

  /**
   * Tracks whether we're currently rendering a value for json_transform SET.
   * When true, we can use PATH 'expr' syntax for type-preserving operations like .size()
   * instead of JSON_VALUE which would require type coercion.
   */
  @SuppressFBWarnings(
      value = "URF_UNREAD_FIELD",
      justification = "Field is set but read logic will be added for JSON_TRANSFORM optimization")
  private boolean inJsonTransformSet = false;

  public CteBasedPipelineRenderer(OracleConfiguration config) {
    this.config = config;
  }

  /**
   * Renders the pipeline to the given context using CTE-based SQL generation.
   */
  public void render(Pipeline pipeline, SqlGenerationContext ctx) {
    // Set CTE context so expressions know to use "DATA" column references
    ctx.setInCteContext(true);
    ctx.setUsesCteDataColumn(true);

    List<Stage> stages = pipeline.getStages();
    String tableName = pipeline.getCollectionName();

    if (stages.isEmpty()) {
      // Empty pipeline: simple base CTE and select with JSON_ARRAYAGG
      renderBaseCte(ctx, tableName, 1);
      ctx.sql(" SELECT JSON_ARRAYAGG(\"DATA\" RETURNING CLOB) FROM \"Q1\"");
      return;
    }

    // Build list of CTEs
    List<CteDefinition> ctes = new ArrayList<>();

    // Q1 is always the base table
    ctes.add(new CteDefinition("Q1", CteType.BASE, null));

    // Process each stage - use index-based loop to allow look-ahead for sort+pagination
    int cteIndex = 2;
    for (int stageIdx = 0; stageIdx < stages.size(); stageIdx++) {
      Stage stage = stages.get(stageIdx);

      if (stage instanceof MatchStage matchStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.MATCH, matchStage));
        cteIndex++;
      } else if (stage instanceof ProjectStage projectStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.PROJECT, projectStage));
        cteIndex++;
      } else if (stage instanceof GroupStage groupStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.GROUP, groupStage));
        cteIndex++;
      } else if (stage instanceof SortStage sortStage) {
        // Look ahead for skip/limit to combine with sort (CTEs don't preserve ordering)
        SkipStage skipStage = null;
        LimitStage limitStage = null;
        int consumed = 0;

        // Check next stage for skip or limit
        if (stageIdx + 1 < stages.size()) {
          Stage next = stages.get(stageIdx + 1);
          if (next instanceof SkipStage ss) {
            skipStage = ss;
            consumed++;
          } else if (next instanceof LimitStage ls) {
            limitStage = ls;
            consumed++;
          }
        }
        // Check stage after that for the other (skip after limit or limit after skip)
        if (stageIdx + 1 + consumed < stages.size()) {
          Stage next = stages.get(stageIdx + 1 + consumed);
          if (skipStage == null && next instanceof SkipStage ss) {
            skipStage = ss;
            consumed++;
          } else if (limitStage == null && next instanceof LimitStage ls) {
            limitStage = ls;
            consumed++;
          }
        }

        if (skipStage != null || limitStage != null) {
          // Combine sort with pagination into single CTE
          var combined = new SortPaginationStages(sortStage, skipStage, limitStage);
          ctes.add(new CteDefinition("Q" + cteIndex, CteType.SORT_PAGINATION, combined));
          stageIdx += consumed;  // Skip the consumed stages
        } else {
          // Check if followed by $group with $first/$last accumulators
          if (stageIdx + 1 < stages.size()
              && stages.get(stageIdx + 1) instanceof GroupStage gs
              && hasFirstOrLastAccumulator(gs)) {
            // Combine sort with group for proper $first/$last with KEEP clause
            var combined = new SortGroupStages(sortStage, gs);
            ctes.add(new CteDefinition("Q" + cteIndex, CteType.SORT_GROUP, combined));
            stageIdx++;  // Skip the group stage (we consumed it)
          } else {
            // Standalone sort (no following skip/limit/group)
            ctes.add(new CteDefinition("Q" + cteIndex, CteType.SORT, sortStage));
          }
        }
        cteIndex++;
      } else if (stage instanceof LimitStage limitStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.LIMIT, limitStage));
        cteIndex++;
      } else if (stage instanceof SkipStage skipStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.SKIP, skipStage));
        cteIndex++;
      } else if (stage instanceof CountStage countStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.COUNT, countStage));
        cteIndex++;
      } else if (stage instanceof SampleStage sampleStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.SAMPLE, sampleStage));
        cteIndex++;
      } else if (stage instanceof AddFieldsStage addFieldsStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.ADDFIELDS, addFieldsStage));
        cteIndex++;
      } else if (stage instanceof UnwindStage unwindStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.UNWIND, unwindStage));
        cteIndex++;
      } else if (stage instanceof LookupStage lookupStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.LOOKUP, lookupStage));
        cteIndex++;
      } else if (stage instanceof UnionWithStage unionWithStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.UNIONWITH, unionWithStage));
        cteIndex++;
      } else if (stage instanceof BucketStage bucketStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.BUCKET, bucketStage));
        cteIndex++;
      } else if (stage instanceof BucketAutoStage bucketAutoStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.BUCKETAUTO, bucketAutoStage));
        cteIndex++;
      } else if (stage instanceof FacetStage facetStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.FACET, facetStage));
        cteIndex++;
      } else if (stage instanceof RedactStage redactStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.REDACT, redactStage));
        cteIndex++;
      } else if (stage instanceof SetWindowFieldsStage windowStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.SETWINDOWFIELDS, windowStage));
        cteIndex++;
      } else if (stage instanceof ReplaceRootStage replaceRootStage) {
        ctes.add(new CteDefinition("Q" + cteIndex, CteType.REPLACEROOT, replaceRootStage));
        cteIndex++;
      } else if (stage instanceof GraphLookupStage graphLookupStage) {
        // Check if this is a recursive graphLookup (maxDepth > 0)
        Integer maxDepth = graphLookupStage.getMaxDepth();
        if (maxDepth != null && maxDepth > 0) {
          // Recursive graphLookup - need 3 CTEs: paths, aggregation, and join
          String asField = graphLookupStage.getAs();
          String pathsCte = "graph_paths_" + asField;
          String aggCte = "graph_" + asField;
          String sourceCte = "Q" + (cteIndex - 1);

          RecursiveGraphLookupContext context = new RecursiveGraphLookupContext(
              graphLookupStage, sourceCte, pathsCte, aggCte);

          // Add the 3 CTEs for recursive graphLookup
          ctes.add(new CteDefinition(pathsCte, CteType.GRAPHLOOKUP_PATHS, context));
          ctes.add(new CteDefinition(aggCte, CteType.GRAPHLOOKUP_AGG, context));
          ctes.add(new CteDefinition("Q" + cteIndex, CteType.GRAPHLOOKUP_JOIN, context));
          cteIndex++;
        } else {
          // Non-recursive graphLookup (maxDepth=0 or null) - single-level lookup
          ctes.add(new CteDefinition("Q" + cteIndex, CteType.GRAPHLOOKUP, graphLookupStage));
          cteIndex++;
        }
      }
      // TODO: Add support for other stage types
    }

    // Render WITH clause
    ctx.sql("WITH ");

    for (int i = 0; i < ctes.size(); i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      renderCte(ctx, ctes.get(i), tableName, i > 0 ? ctes.get(i - 1).name : null);
    }

    // Render final SELECT with JSON_ARRAYAGG for proper array output
    String lastCte = ctes.get(ctes.size() - 1).name;
    ctx.sql(" SELECT JSON_ARRAYAGG(\"DATA\" RETURNING CLOB) FROM \"" + lastCte + "\"");
  }

  private void renderBaseCte(SqlGenerationContext ctx, String tableName, int cteNumber) {
    // Uppercase table name for Oracle (quoted identifiers are case-sensitive)
    String upperTableName = tableName.toUpperCase(java.util.Locale.ROOT);
    ctx.sql("WITH \"Q" + cteNumber + "\" (\"ID\", \"DATA\") AS (");
    ctx.sql("SELECT \"ID\", \"DATA\" FROM \"" + upperTableName + "\"");
    ctx.sql(")");
  }

  private void renderCte(
      SqlGenerationContext ctx, CteDefinition cte, String tableName, String previousCte) {

    switch (cte.type) {
      case BASE -> {
        String upperTableName = tableName.toUpperCase(java.util.Locale.ROOT);
        ctx.sql("\"Q1\" (\"ID\", \"DATA\") AS (");
        ctx.sql("SELECT \"ID\", \"DATA\" FROM \"" + upperTableName + "\"");
        ctx.sql(")");
      }
      case MATCH -> {
        MatchStage matchStage = (MatchStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT \"DATA\" FROM \"" + previousCte + "\" q WHERE ");
        renderJsonExistsPredicate(ctx, matchStage.getFilter());
        ctx.sql(")");
      }
      case PROJECT -> {
        ProjectStage projectStage = (ProjectStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT ");
        renderJsonTransformProjection(ctx, projectStage);
        ctx.sql(" FROM \"" + previousCte + "\" q");
        ctx.sql(")");
      }
      case GROUP -> {
        GroupStage groupStage = (GroupStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT ");
        renderJsonObjectGroup(ctx, groupStage);
        // Use inline view wrapper to enable dot notation for type preservation
        ctx.sql(" AS \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q");
        renderGroupByClause(ctx, groupStage);
        ctx.sql(")");
      }
      case SORT -> {
        SortStage sortStage = (SortStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        // Use inline view wrapper to enable dot notation for type-preserving sort
        ctx.sql("SELECT \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q");
        renderOrderByClause(ctx, sortStage);
        ctx.sql(")");
      }
      case SORT_PAGINATION -> {
        // Combined sort + skip/limit for order preservation across CTEs
        SortPaginationStages sp = cte.sortPagination();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        // Use inline view wrapper to enable dot notation for type-preserving sort
        ctx.sql("SELECT \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q");
        renderOrderByClause(ctx, sp.sort());
        if (sp.skip() != null) {
          ctx.sql(" OFFSET " + sp.skip().getSkip() + " ROWS");
        }
        if (sp.limit() != null) {
          ctx.sql(" FETCH FIRST " + sp.limit().getLimit() + " ROWS ONLY");
        }
        ctx.sql(")");
      }
      case SORT_GROUP -> {
        // Combined sort + group for proper $first/$last with KEEP clause
        SortGroupStages sg = cte.sortGroup();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT ");
        renderJsonObjectGroupWithSort(ctx, sg.group(), sg.sort());
        // Use inline view wrapper to enable dot notation for type preservation
        ctx.sql(" AS \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q");
        renderGroupByClause(ctx, sg.group());
        ctx.sql(")");
      }
      case LIMIT -> {
        LimitStage limitStage = (LimitStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT \"DATA\" FROM \"" + previousCte + "\" q");
        ctx.sql(" FETCH FIRST " + limitStage.getLimit() + " ROWS ONLY");
        ctx.sql(")");
      }
      case SKIP -> {
        SkipStage skipStage = (SkipStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT \"DATA\" FROM \"" + previousCte + "\" q");
        ctx.sql(" OFFSET " + skipStage.getSkip() + " ROWS");
        ctx.sql(")");
      }
      case COUNT -> {
        CountStage countStage = (CountStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT JSON_OBJECT('");
        ctx.sql(countStage.getFieldName());
        ctx.sql("' VALUE COUNT(*)) AS \"DATA\" FROM \"" + previousCte + "\" q");
        ctx.sql(")");
      }
      case SAMPLE -> {
        SampleStage sampleStage = (SampleStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT \"DATA\" FROM \"" + previousCte + "\" q");
        ctx.sql(" ORDER BY DBMS_RANDOM.VALUE FETCH FIRST " + sampleStage.getSize() + " ROWS ONLY");
        ctx.sql(")");
      }
      case ADDFIELDS -> {
        AddFieldsStage addFieldsStage = (AddFieldsStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT ");
        renderJsonTransformAddFields(ctx, addFieldsStage);
        ctx.sql(" FROM \"" + previousCte + "\" q");
        ctx.sql(")");
      }
      case UNWIND -> {
        UnwindStage unwindStage = (UnwindStage) cte.stage();
        String path = unwindStage.getPath();
        final String indexField = unwindStage.getIncludeArrayIndex();

        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        // Use json_transform to replace the array with each unwound element
        ctx.sql("SELECT json_transform(q.\"DATA\", SET '$.\"");
        ctx.sql(path);
        ctx.sql("\"' = jt.elem");
        // Add array index field if specified (0-based, so subtract 1 from Oracle's 1-based)
        if (indexField != null) {
          ctx.sql(", SET '$.\"" + indexField + "\"' = (jt.idx - 1)");
        }
        ctx.sql(") AS \"DATA\" FROM \"" + previousCte + "\" q");
        final boolean preserveNull = unwindStage.isPreserveNullAndEmptyArrays();
        if (preserveNull) {
          // LEFT OUTER JOIN preserves documents with null/empty arrays
          ctx.sql(" LEFT OUTER JOIN ");
        } else {
          ctx.sql(", ");
        }
        ctx.sql("JSON_TABLE(q.\"DATA\", '$." + path + "[*]' ");
        ctx.sql("COLUMNS (elem JSON PATH '$'");
        if (indexField != null) {
          ctx.sql(", idx FOR ORDINALITY");
        }
        ctx.sql(")) jt");
        if (preserveNull) {
          ctx.sql(" ON (1=1)");
        }
        ctx.sql(")");
      }
      case LOOKUP -> {
        LookupStage lookupStage = (LookupStage) cte.stage();
        String upperFromTable = lookupStage.getFrom().toUpperCase(java.util.Locale.ROOT);
        String asField = lookupStage.getAs();

        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT json_transform(q.\"DATA\", SET '$.\"" + asField + "\"' = ");

        if (lookupStage.isPipelineForm()) {
          // Pipeline form: use let variables and pipeline stages
          renderLookupPipelineSubquery(ctx, lookupStage, upperFromTable);
        } else {
          // Simple equality form - use dot notation for type preservation
          String localField = lookupStage.getLocalField();
          String foreignField = lookupStage.getForeignField();

          ctx.sql("COALESCE((SELECT JSON_ARRAYAGG(f.\"DATA\" RETURNING JSON) ");
          ctx.sql("FROM \"" + upperFromTable + "\" f ");
          // Use dot notation for both fields - inline view wrapper enables this for CTE access
          ctx.sql("WHERE f.\"DATA\"." + quoteDotNotationPath(foreignField) + " = ");
          ctx.sql("q.\"DATA\"." + quoteDotNotationPath(localField) + "), ");
          ctx.sql("JSON_ARRAY(RETURNING JSON))");
        }

        // Wrap CTE reference in inline view to enable dot notation
        // Oracle's parser requires this for simplified JSON syntax on CTE columns
        ctx.sql(") AS \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q");
        ctx.sql(")");
      }
      case UNIONWITH -> {
        UnionWithStage unionWithStage = (UnionWithStage) cte.stage();
        String upperCollection =
            unionWithStage.getCollection().toUpperCase(java.util.Locale.ROOT);
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT \"DATA\" FROM \"" + previousCte + "\" ");
        ctx.sql("UNION ALL ");
        if (unionWithStage.hasPipeline()) {
          renderUnionSubpipeline(ctx, upperCollection, unionWithStage.getPipeline());
        } else {
          ctx.sql("SELECT \"DATA\" FROM \"" + upperCollection + "\"");
        }
        ctx.sql(")");
      }
      case BUCKET -> {
        BucketStage bucketStage = (BucketStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT JSON_OBJECT('_id' VALUE ");
        renderBucketCaseExpression(ctx, bucketStage);
        // Render output accumulators (count, products, avgBonus, etc.)
        for (Map.Entry<String, AccumulatorExpression> entry
            : bucketStage.getOutput().entrySet()) {
          ctx.sql(", '" + entry.getKey() + "' VALUE ");
          renderAccumulator(ctx, entry.getValue());
        }
        // Use inline view wrapper to enable dot notation for type preservation
        ctx.sql(") AS \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q ");
        ctx.sql("GROUP BY ");
        renderBucketCaseExpression(ctx, bucketStage);
        ctx.sql(")");
      }
      case BUCKETAUTO -> {
        final BucketAutoStage bucketAutoStage = (BucketAutoStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        // MongoDB $bucketAuto returns _id as {min: X, max: Y}
        ctx.sql("SELECT JSON_OBJECT('_id' VALUE ");
        ctx.sql("JSON_OBJECT('min' VALUE MIN(group_val), 'max' VALUE MAX(group_val))");
        // Render output accumulators
        // Note: Use special BUCKETAUTO method since q alias is in subquery scope
        for (Map.Entry<String, AccumulatorExpression> entry
            : bucketAutoStage.getOutput().entrySet()) {
          ctx.sql(", '" + entry.getKey() + "' VALUE ");
          renderBucketAutoAccumulator(ctx, entry.getValue());
        }
        ctx.sql(") AS \"DATA\" FROM (");
        final int buckets = bucketAutoStage.getBuckets();
        ctx.sql("SELECT NTILE(" + buckets + ") OVER (ORDER BY ");
        // Use dot notation for type-preserving numeric ordering
        renderBucketAutoGroupByField(ctx, bucketAutoStage.getGroupBy());
        ctx.sql(") AS bucket_id, ");
        // Include the groupBy field value for MIN/MAX computation using dot notation
        renderBucketAutoGroupByField(ctx, bucketAutoStage.getGroupBy());
        // Use inline view wrapper to enable dot notation for type preservation
        ctx.sql(" AS group_val, \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q");
        ctx.sql(") GROUP BY bucket_id ORDER BY bucket_id");
        ctx.sql(")");
      }
      case FACET -> {
        FacetStage facetStage = (FacetStage) cte.stage();

        // First, render a CTE for each facet sub-pipeline
        for (Map.Entry<String, List<Stage>> facet : facetStage.getFacets().entrySet()) {
          String facetName = facet.getKey();
          List<Stage> subPipeline = facet.getValue();
          String facetCteName = cte.name + "_" + facetName;

          ctx.sql("\"" + facetCteName + "\" (\"DATA\") AS (");
          renderFacetSubPipeline(ctx, previousCte, subPipeline);
          ctx.sql("), ");
        }

        // Then render the combining CTE
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT JSON_OBJECT(");
        boolean first = true;
        for (String facetName : facetStage.getFacetNames()) {
          if (!first) {
            ctx.sql(", ");
          }
          String facetCteName = cte.name + "_" + facetName;
          ctx.sql("'" + facetName + "' VALUE ");
          ctx.sql("COALESCE((SELECT JSON_ARRAYAGG(\"DATA\" RETURNING JSON) FROM \"");
          ctx.sql(facetCteName);
          ctx.sql("\"), JSON_ARRAY(RETURNING JSON))");
          first = false;
        }
        ctx.sql(") AS \"DATA\" FROM DUAL");
        ctx.sql(")");

        // Track facet names for post-facet projections that reference facet arrays
        activeFacetNames.clear();
        activeFacetNames.addAll(facetStage.getFacetNames());
      }
      case REDACT -> {
        RedactStage redactStage = (RedactStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        // Use inline view wrapper to enable dot notation for type preservation
        ctx.sql("SELECT \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q WHERE ");
        renderRedactCondition(ctx, redactStage.getExpression());
        ctx.sql(")");
      }
      case SETWINDOWFIELDS -> {
        SetWindowFieldsStage windowStage = (SetWindowFieldsStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT json_transform(q.\"DATA\"");
        // Add each window function output field
        for (Map.Entry<String, SetWindowFieldsStage.WindowField> entry
            : windowStage.getOutput().entrySet()) {
          ctx.sql(", SET '$.\"" + entry.getKey() + "\"' = ");
          renderWindowFunctionCte(ctx, windowStage, entry.getValue());
        }
        ctx.sql(") AS \"DATA\" FROM \"" + previousCte + "\" q");
        ctx.sql(")");
      }
      case REPLACEROOT -> {
        ReplaceRootStage replaceRootStage = (ReplaceRootStage) cte.stage();
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT ");
        renderReplaceRootExpression(ctx, replaceRootStage.getNewRoot());
        ctx.sql(" AS \"DATA\" FROM \"" + previousCte + "\" q");
        ctx.sql(")");
      }
      case GRAPHLOOKUP -> {
        GraphLookupStage graphLookupStage = (GraphLookupStage) cte.stage();
        final String upperFromTable = graphLookupStage.getFrom().toUpperCase(java.util.Locale.ROOT);
        String asField = graphLookupStage.getAs();
        // Strip leading $ from field path (MongoDB uses $fieldName for field references)
        String startWith = graphLookupStage.getStartWith();
        if (startWith.startsWith("$")) {
          startWith = startWith.substring(1);
        }
        String connectToField = graphLookupStage.getConnectToField();

        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT json_transform(q.\"DATA\", SET '$.\"" + asField + "\"' = ");
        ctx.sql("COALESCE((SELECT JSON_ARRAYAGG(f.\"DATA\" RETURNING JSON) ");
        ctx.sql("FROM \"" + upperFromTable + "\" f ");
        // Use dot notation for type-preserving comparison
        ctx.sql("WHERE f.\"DATA\"." + quoteDotNotationPath(connectToField) + " = ");
        ctx.sql("q.\"DATA\"." + quoteDotNotationPath(startWith));

        // Add restrictSearchWithMatch filter if present
        var restrictMatch = graphLookupStage.getRestrictSearchWithMatch();
        if (restrictMatch != null && !restrictMatch.isEmpty()) {
          for (var entry : restrictMatch.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            // Use dot notation for type-preserving filter
            ctx.sql(" AND f.\"DATA\"." + quoteDotNotationPath(field));
            if (value instanceof Boolean boolVal) {
              ctx.sql(" = " + boolVal);
            } else if (value instanceof Number numVal) {
              ctx.sql(" = " + numVal);
            } else if (value instanceof String strVal) {
              ctx.sql(" = '" + strVal.replace("'", "''") + "'");
            } else {
              ctx.sql(" = '" + value + "'");
            }
          }
        }

        ctx.sql("), JSON_ARRAY(RETURNING JSON))");
        // Wrap CTE reference in inline view to enable dot notation
        ctx.sql(") AS \"DATA\" FROM (SELECT * FROM \"" + previousCte + "\") q");
        ctx.sql(")");
      }
      case GRAPHLOOKUP_PATHS -> {
        // Recursive CTE for $graphLookup with maxDepth > 0
        RecursiveGraphLookupContext context = cte.recursiveGraphLookup();
        GraphLookupStage glStage = context.stage();
        final String fromTable = glStage.getFrom().toUpperCase(java.util.Locale.ROOT);
        String startWith = glStage.getStartWith();
        if (startWith.startsWith("$")) {
          startWith = startWith.substring(1);
        }
        final String connectFromField = glStage.getConnectFromField();
        String connectToField = glStage.getConnectToField();

        // Store connectFromField value as a separate column to avoid JSON_VALUE on recursive ref
        ctx.sql("\"" + cte.name + "\" (start_id, id, connect_from_val, data, graph_depth) AS (");

        // Base case: each row in the source starts its own traversal
        // Use dot notation for type-preserving field access
        // Wrap source CTE in inline view to enable dot notation
        ctx.sql("SELECT s.\"DATA\"." + quoteDotNotationPath(startWith) + " AS start_id, ");
        ctx.sql("g.\"DATA\".\"_id\" AS id, ");
        ctx.sql("g.\"DATA\"." + quoteDotNotationPath(connectFromField) + " AS connect_from_val, ");
        ctx.sql("g.\"DATA\" AS data, 0 AS graph_depth ");
        ctx.sql("FROM \"" + fromTable + "\" g, (SELECT * FROM \"" + context.sourceCte() + "\") s ");
        ctx.sql("WHERE g.\"DATA\"." + quoteDotNotationPath(connectToField) + " = ");
        ctx.sql("s.\"DATA\"." + quoteDotNotationPath(startWith));

        // Add restrictSearchWithMatch filter if present
        var restrictMatch = glStage.getRestrictSearchWithMatch();
        if (restrictMatch != null && !restrictMatch.isEmpty()) {
          for (var entry : restrictMatch.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            ctx.sql(" AND g.\"DATA\"." + quoteDotNotationPath(field));
            if (value instanceof Boolean boolVal) {
              ctx.sql(" = " + boolVal);  // Native boolean, not string
            } else if (value instanceof Number numVal) {
              ctx.sql(" = " + numVal);
            } else if (value instanceof String strVal) {
              ctx.sql(" = '" + strVal.replace("'", "''") + "'");
            } else {
              ctx.sql(" = '" + value + "'");
            }
          }
        }

        ctx.sql(" UNION ALL ");

        // Recursive case: follow connections
        // Use dot notation for base table, and the extracted connect_from_val column for CTE ref
        ctx.sql("SELECT p.start_id, c.\"DATA\".\"_id\" AS id, ");
        ctx.sql("c.\"DATA\"." + quoteDotNotationPath(connectFromField) + " AS connect_from_val, ");
        ctx.sql("c.\"DATA\" AS data, ");
        ctx.sql("p.graph_depth + 1 AS graph_depth ");
        ctx.sql("FROM \"" + fromTable + "\" c ");
        ctx.sql("JOIN \"" + cte.name + "\" p ON ");
        // Use dot notation for base table, and extracted column for recursive ref
        ctx.sql("c.\"DATA\"." + quoteDotNotationPath(connectToField) + " = p.connect_from_val");

        // Add depth limit
        ctx.sql(" WHERE p.graph_depth < " + glStage.getMaxDepth());

        // Add restrictSearchWithMatch filter to recursive case
        if (restrictMatch != null && !restrictMatch.isEmpty()) {
          for (var entry : restrictMatch.entrySet()) {
            String field = entry.getKey();
            Object value = entry.getValue();
            ctx.sql(" AND c.\"DATA\"." + quoteDotNotationPath(field));
            if (value instanceof Boolean boolVal) {
              ctx.sql(" = " + boolVal);  // Native boolean, not string
            } else if (value instanceof Number numVal) {
              ctx.sql(" = " + numVal);
            } else if (value instanceof String strVal) {
              ctx.sql(" = '" + strVal.replace("'", "''") + "'");
            } else {
              ctx.sql(" = '" + value + "'");
            }
          }
        }

        ctx.sql(")");
      }
      case GRAPHLOOKUP_AGG -> {
        // Aggregation CTE for recursive $graphLookup
        RecursiveGraphLookupContext context = cte.recursiveGraphLookup();
        GraphLookupStage glStage = context.stage();
        String depthField = glStage.getDepthField();

        ctx.sql("\"" + cte.name + "\" AS (");
        ctx.sql("SELECT start_id, JSON_ARRAYAGG(");
        if (depthField != null) {
          // Merge depth into each document using JSON_MERGEPATCH
          ctx.sql("JSON_MERGEPATCH(data, JSON_OBJECT('" + depthField + "' VALUE graph_depth))");
        } else {
          ctx.sql("data");
        }
        ctx.sql(" RETURNING CLOB) AS \"" + glStage.getAs() + "\" ");
        ctx.sql("FROM \"" + context.pathsCte() + "\" GROUP BY start_id");
        ctx.sql(")");
      }
      case GRAPHLOOKUP_JOIN -> {
        // Join CTE to merge recursive results back to source
        RecursiveGraphLookupContext context = cte.recursiveGraphLookup();
        GraphLookupStage glStage = context.stage();
        String asField = glStage.getAs();
        String startWith = glStage.getStartWith();
        if (startWith.startsWith("$")) {
          startWith = startWith.substring(1);
        }

        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT json_transform(s.\"DATA\", SET '$.\"" + asField + "\"' = ");
        ctx.sql("COALESCE(g.\"" + asField + "\", JSON_ARRAY(RETURNING CLOB))");
        ctx.sql(") AS \"DATA\" ");
        // Wrap source CTE in inline view to enable dot notation
        ctx.sql("FROM (SELECT * FROM \"" + context.sourceCte() + "\") s ");
        ctx.sql("LEFT JOIN \"" + context.aggCte() + "\" g ON ");
        // Use dot notation for type-preserving field access
        ctx.sql("s.\"DATA\"." + quoteDotNotationPath(startWith) + " = g.start_id");
        ctx.sql(")");
      }
      default -> {
        // Unsupported stage type - pass through DATA
        ctx.sql("\"" + cte.name + "\" (\"DATA\") AS (");
        ctx.sql("SELECT \"DATA\" FROM \"" + previousCte + "\" q");
        ctx.sql(")");
      }
    }
  }

  /**
   * Renders a sub-pipeline for $unionWith as nested subqueries.
   * Supports $match and $project stages.
   */
  private void renderUnionSubpipeline(
      SqlGenerationContext ctx, String collection, List<Stage> stages) {
    // Separate stages by type
    MatchStage matchStage = null;
    ProjectStage projectStage = null;

    for (Stage stage : stages) {
      if (stage instanceof MatchStage ms) {
        matchStage = ms;
      } else if (stage instanceof ProjectStage ps) {
        projectStage = ps;
      }
    }

    // Render the sub-pipeline
    if (projectStage != null) {
      ctx.sql("SELECT ");
      renderJsonTransformProjection(ctx, projectStage);
      ctx.sql(" AS \"DATA\" FROM \"" + collection + "\" q");
      if (matchStage != null) {
        ctx.sql(" WHERE ");
        renderJsonExistsPredicate(ctx, matchStage.getFilter());
      }
    } else if (matchStage != null) {
      ctx.sql("SELECT \"DATA\" FROM \"" + collection + "\" WHERE ");
      renderJsonExistsPredicate(ctx, matchStage.getFilter());
    } else {
      // No recognizable stages, just select from collection
      ctx.sql("SELECT \"DATA\" FROM \"" + collection + "\"");
    }
  }

  /**
   * Renders a $lookup pipeline form as a correlated subquery.
   * Handles let variable substitution and pipeline stages ($match, $group, $unwind).
   */
  private void renderLookupPipelineSubquery(
      SqlGenerationContext ctx, LookupStage lookupStage, String fromTable) {
    var letVars = lookupStage.getLetVariables();
    var pipeline = lookupStage.getPipeline();

    // Collect all key stages in the pipeline
    List<MatchStage> matchStages = new ArrayList<>();
    GroupStage groupStage = null;
    UnwindStage unwindStage = null;
    for (Stage stage : pipeline) {
      if (stage instanceof MatchStage ms) {
        matchStages.add(ms);
      } else if (stage instanceof GroupStage gs) {
        groupStage = gs;
      } else if (stage instanceof UnwindStage us) {
        unwindStage = us;
      }
    }

    // Get unwind path if present (e.g., "items" from "$items")
    String unwindPath = null;
    if (unwindStage != null) {
      unwindPath = unwindStage.getPath();
    }

    ctx.sql("COALESCE((SELECT ");

    if (groupStage != null) {
      // Pipeline with $group: render JSON_ARRAY(JSON_OBJECT(...))
      ctx.sql("JSON_ARRAY(JSON_OBJECT(");
      ctx.sql("'_id' VALUE NULL");  // _id: null in the group

      // Render each accumulator
      for (var entry : groupStage.getAccumulators().entrySet()) {
        ctx.sql(", '" + entry.getKey() + "' VALUE ");
        renderLookupAccumulator(ctx, entry.getValue(), unwindPath);
      }

      ctx.sql(") RETURNING JSON)");
    } else {
      // No $group: collect matching documents
      ctx.sql("JSON_ARRAYAGG(f.\"DATA\" RETURNING JSON)");
    }

    ctx.sql(" FROM \"" + fromTable + "\" f");

    // If there's an $unwind, add JSON_TABLE to iterate over the array
    if (unwindPath != null) {
      ctx.sql(", JSON_TABLE(f.\"DATA\", '$." + unwindPath + "[*]' COLUMNS (");
      // Add columns based on fields used in match and group
      java.util.Set<String> neededFields = collectUnwindFields(matchStages, groupStage, unwindPath);
      boolean first = true;
      for (String field : neededFields) {
        if (!first) {
          ctx.sql(", ");
        }
        ctx.sql("\"" + field + "\" VARCHAR2(4000) PATH '$." + field + "'");
        first = false;
      }
      ctx.sql(")) jt");
    }

    // Render WHERE clause with let variable substitution - combine all match stages
    if (!matchStages.isEmpty()) {
      ctx.sql(" WHERE ");
      for (int i = 0; i < matchStages.size(); i++) {
        if (i > 0) {
          ctx.sql(" AND ");
        }
        renderLookupMatchCondition(ctx, matchStages.get(i).getFilter(), letVars, unwindPath);
      }
    }

    ctx.sql("), JSON_ARRAY(RETURNING JSON))");  // Empty array when no matches
  }

  /**
   * Collects field names used in match and group stages that come from the unwound array.
   */
  private java.util.Set<String> collectUnwindFields(
      List<MatchStage> matchStages, GroupStage groupStage, String unwindPath) {
    java.util.Set<String> fields = new java.util.LinkedHashSet<>();
    String prefix = unwindPath + ".";

    // Collect from match stages
    for (MatchStage ms : matchStages) {
      collectFieldsFromExpression(ms.getFilter(), prefix, fields);
    }

    // Collect from group stage accumulators
    if (groupStage != null) {
      for (var entry : groupStage.getAccumulators().entrySet()) {
        collectFieldsFromExpression(entry.getValue().getArgument(), prefix, fields);
      }
    }

    return fields;
  }

  /**
   * Recursively collects field paths starting with the given prefix.
   */
  private void collectFieldsFromExpression(
      Expression expr, String prefix, java.util.Set<String> fields) {
    if (expr instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      if (path.startsWith(prefix)) {
        // Extract the field name after the prefix (e.g., "items.product" -> "product")
        fields.add(path.substring(prefix.length()));
      }
    } else if (expr instanceof ComparisonExpression comp) {
      collectFieldsFromExpression(comp.getLeft(), prefix, fields);
      collectFieldsFromExpression(comp.getRight(), prefix, fields);
    } else if (expr instanceof LogicalExpression logical) {
      for (Expression operand : logical.getOperands()) {
        collectFieldsFromExpression(operand, prefix, fields);
      }
    } else if (expr instanceof AccumulatorExpression accum) {
      collectFieldsFromExpression(accum.getArgument(), prefix, fields);
    }
  }

  /**
   * Renders an accumulator expression for $lookup pipeline $group.
   * Handles SUM, COUNT, MIN, MAX, PUSH (with object expressions), etc.
   *
   * @param unwindPath if non-null, use JSON_TABLE columns (jt.field) instead of JSON_VALUE
   */
  private void renderLookupAccumulator(
      SqlGenerationContext ctx, AccumulatorExpression accum, String unwindPath) {
    var op = accum.getOp();
    Expression arg = accum.getArgument();

    switch (op) {
      case COUNT -> ctx.sql("COUNT(*)");
      case PUSH, ADD_TO_SET -> {
        // $push/$addToSet: JSON_ARRAYAGG(...)
        ctx.sql("COALESCE(JSON_ARRAYAGG(");
        renderLookupPushArgument(ctx, arg);
        ctx.sql(" RETURNING JSON), JSON_ARRAY(RETURNING JSON))");
      }
      case MAX, MIN -> {
        // MAX/MIN: use dot notation for type preservation (numbers, dates, strings)
        String func = op == AccumulatorOp.MAX ? "MAX" : "MIN";
        ctx.sql(func + "(");
        renderLookupFieldAccessDotNotation(ctx, arg, unwindPath);
        ctx.sql(")");
      }
      case SUM, AVG -> {
        // Numeric aggregations
        String func = op.getSqlFunction();
        ctx.sql("NVL(" + func + "(");
        renderLookupFieldAccess(ctx, arg, unwindPath, true);
        ctx.sql("), 0)");
      }
      default -> {
        // Fallback for other accumulators
        ctx.sql("NULL");
      }
    }
  }

  /**
   * Renders a field access in lookup context, using JSON_TABLE column if unwound.
   *
   * @param numeric if true, use RETURNING NUMBER for JSON_VALUE
   */
  private void renderLookupFieldAccess(
      SqlGenerationContext ctx, Expression arg, String unwindPath, boolean numeric) {
    if (arg instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      if (path.startsWith("$")) {
        path = path.substring(1);
      }

      // Check if this path starts with the unwind path
      if (unwindPath != null && path.startsWith(unwindPath + ".")) {
        // Use JSON_TABLE column (jt.fieldName)
        String fieldName = path.substring(unwindPath.length() + 1);
        if (numeric) {
          ctx.sql("TO_NUMBER(jt.\"" + fieldName + "\")");
        } else {
          ctx.sql("jt.\"" + fieldName + "\"");
        }
      } else {
        // Standard JSON_VALUE access
        if (numeric) {
          ctx.sql("JSON_VALUE(f.\"DATA\", '$." + path + "' RETURNING NUMBER)");
        } else {
          ctx.sql("JSON_VALUE(f.\"DATA\", '$." + path + "')");
        }
      }
    } else if (arg instanceof LiteralExpression lit) {
      // Handle literal values like $sum: 1
      Object value = lit.getValue();
      if (value instanceof Number num) {
        ctx.sql(String.valueOf(num));
      } else if (value instanceof String str) {
        ctx.sql("'" + str.replace("'", "''") + "'");
      } else if (value == null) {
        ctx.sql("NULL");
      } else {
        ctx.sql(String.valueOf(value));
      }
    } else {
      ctx.sql("NULL");
    }
  }

  /**
   * Renders a field access in lookup context using dot notation for type preservation.
   * Used by MAX/MIN accumulators to preserve native JSON types (numbers, dates, strings).
   */
  private void renderLookupFieldAccessDotNotation(
      SqlGenerationContext ctx, Expression arg, String unwindPath) {
    if (arg instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      if (path.startsWith("$")) {
        path = path.substring(1);
      }

      // Check if this path starts with the unwind path
      if (unwindPath != null && path.startsWith(unwindPath + ".")) {
        // Use JSON_TABLE column (jt.fieldName) - already type-preserving
        String fieldName = path.substring(unwindPath.length() + 1);
        ctx.sql("jt.\"" + fieldName + "\"");
      } else {
        // Use dot notation for type preservation: f."DATA".fieldPath
        ctx.sql("f.\"DATA\"." + quoteDotNotationPath(path));
      }
    } else if (arg instanceof LiteralExpression lit) {
      // Handle literal values
      Object value = lit.getValue();
      if (value instanceof Number num) {
        ctx.sql(String.valueOf(num));
      } else if (value instanceof String str) {
        ctx.sql("'" + str.replace("'", "''") + "'");
      } else if (value == null) {
        ctx.sql("NULL");
      } else {
        ctx.sql(String.valueOf(value));
      }
    } else {
      ctx.sql("NULL");
    }
  }

  /**
   * Renders a field reference in $lookup $match conditions.
   * Handles inner table fields (f.DATA), outer table variable references (q.DATA via letVars),
   * unwound array fields (jt.field), and literal values.
   *
   * @param ctx the SQL generation context
   * @param expr the expression to render
   * @param letVars mapping of $$variable names to their outer-table field paths
   * @param unwindPath if non-null, the array path being unwound (use jt. for fields under it)
   * @param allowVariable if true, check for $$variable references in letVars
   */
  private void renderLookupMatchField(
      SqlGenerationContext ctx, Expression expr, Map<String, String> letVars,
      String unwindPath, boolean allowVariable) {
    if (expr instanceof FieldPathExpression fp) {
      String path = fp.getPath();

      // Check for $$variable reference (outer table field via let clause)
      // Note: ExpressionParser strips one $ from $$varName, leaving $varName
      if (allowVariable && path.startsWith("$") && letVars != null) {
        String varName = path.substring(1); // Remove the remaining $
        String outerField = letVars.get(varName);
        if (outerField != null) {
          // Reference the outer table (q) via the let variable
          ctx.sql("JSON_VALUE(q.\"DATA\", '$." + outerField + "')");
          return;
        }
      }

      // Strip single $ prefix if present (inner table field reference)
      if (path.startsWith("$")) {
        path = path.substring(1);
      }

      // Check if this field is under the unwind path (use JSON_TABLE column)
      if (unwindPath != null && path.startsWith(unwindPath + ".")) {
        String fieldName = path.substring(unwindPath.length() + 1);
        ctx.sql("jt.\"" + fieldName + "\"");
      } else {
        // Standard inner table field reference
        ctx.sql("JSON_VALUE(f.\"DATA\", '$." + path + "')");
      }
    } else if (expr instanceof LiteralExpression lit) {
      Object val = lit.getValue();
      if (val instanceof String) {
        ctx.sql("'" + ((String) val).replace("'", "''") + "'");
      } else if (val instanceof Number) {
        ctx.sql(val.toString());
      } else if (val instanceof Boolean) {
        ctx.sql(((Boolean) val) ? "'true'" : "'false'");
      } else if (val == null) {
        ctx.sql("NULL");
      } else {
        ctx.sql("'" + val.toString().replace("'", "''") + "'");
      }
    } else {
      ctx.sql("NULL");
    }
  }

  /**
   * Renders the argument for a $push accumulator.
   * Handles object expressions like {wh: "$warehouse", qty: "$quantity"}.
   */
  private void renderLookupPushArgument(SqlGenerationContext ctx, Expression arg) {
    if (arg instanceof FieldPathExpression fp) {
      // Simple field path: push the field value
      String path = fp.getPath();
      if (path.startsWith("$")) {
        path = path.substring(1);
      }
      ctx.sql("JSON_QUERY(f.\"DATA\", '$." + path + "')");
    } else if (arg instanceof InlineObjectExpression objExpr) {
      // Object expression: {key1: expr1, key2: expr2}
      ctx.sql("JSON_OBJECT(");
      boolean first = true;
      for (var entry : objExpr.getFields().entrySet()) {
        if (!first) {
          ctx.sql(", ");
        }
        ctx.sql("'" + entry.getKey() + "' VALUE ");
        Expression fieldExpr = entry.getValue();
        if (fieldExpr instanceof FieldPathExpression fieldPath) {
          String path = fieldPath.getPath();
          if (path.startsWith("$")) {
            path = path.substring(1);
          }
          ctx.sql("JSON_VALUE(f.\"DATA\", '$." + path + "')");
        } else if (fieldExpr instanceof LiteralExpression lit) {
          Object val = lit.getValue();
          if (val instanceof String) {
            ctx.sql("'" + val + "'");
          } else {
            ctx.sql(String.valueOf(val));
          }
        } else {
          ctx.sql("NULL");
        }
        first = false;
      }
      ctx.sql(")");
    } else {
      // Unknown argument type
      ctx.sql("NULL");
    }
  }

  /**
   * Renders a $match condition with let variable substitution.
   * Handles $expr: {$eq: ["$field", "$$variable"]} pattern.
   *
   * @param unwindPath if non-null, use JSON_TABLE columns for fields under this path
   */
  private void renderLookupMatchCondition(
      SqlGenerationContext ctx, Expression filter, Map<String, String> letVars,
      String unwindPath) {
    if (filter instanceof LogicalExpression logical) {
      // Handle $and / $or by recursively rendering operands
      List<Expression> operands = logical.getOperands();
      String sqlOperator = logical.getOp() == LogicalOp.AND ? " AND " : " OR ";

      ctx.sql("(");
      for (int i = 0; i < operands.size(); i++) {
        if (i > 0) {
          ctx.sql(sqlOperator);
        }
        renderLookupMatchCondition(ctx, operands.get(i), letVars, unwindPath);
      }
      ctx.sql(")");
    } else if (filter instanceof ComparisonExpression comp) {
      Expression left = comp.getLeft();
      Expression right = comp.getRight();

      // Check if right side is null literal - use IS NULL / IS NOT NULL
      boolean rightIsNull = right instanceof LiteralExpression lit && lit.getValue() == null;

      if (rightIsNull) {
        // Special handling for null comparison
        renderLookupMatchField(ctx, left, letVars, unwindPath, false);
        if (comp.getOp() == ComparisonOp.EQ) {
          ctx.sql(" IS NULL");
        } else if (comp.getOp() == ComparisonOp.NE) {
          ctx.sql(" IS NOT NULL");
        } else {
          // Other comparisons with null don't make sense, render as always false
          ctx.sql(" IS NULL AND 1=0");
        }
      } else {
        // Render left side (inner table field reference)
        renderLookupMatchField(ctx, left, letVars, unwindPath, false);

        // Use the appropriate SQL operator based on the comparison type
        String sqlOp = switch (comp.getOp()) {
          case EQ -> " = ";
          case NE -> " <> ";
          case LT -> " < ";
          case LTE -> " <= ";
          case GT -> " > ";
          case GTE -> " >= ";
          default -> " = ";
        };
        ctx.sql(sqlOp);

        // Render right side (may be a $$variable reference)
        renderLookupMatchField(ctx, right, letVars, unwindPath, true);
      }
    } else if (filter instanceof InExpression inExpr) {
      // Handle $in / $nin
      Expression field = inExpr.getField();
      if (field instanceof FieldPathExpression fp) {
        String path = fp.getPath();
        if (path.startsWith("$") && !path.startsWith("$$")) {
          path = path.substring(1);
        }
        ctx.sql("JSON_VALUE(f.\"DATA\", '$." + path + "')");
      } else {
        ctx.sql("NULL");
      }

      if (inExpr.isNegated()) {
        ctx.sql(" NOT IN (");
      } else {
        ctx.sql(" IN (");
      }

      List<?> values = inExpr.getValues();
      for (int i = 0; i < values.size(); i++) {
        if (i > 0) {
          ctx.sql(", ");
        }
        Object val = values.get(i);
        if (val instanceof String) {
          ctx.sql("'" + ((String) val).replace("'", "''") + "'");
        } else {
          ctx.sql(String.valueOf(val));
        }
      }
      ctx.sql(")");
    } else {
      // Fallback for other expression types - pass all
      ctx.sql("1=1");
    }
  }

  /**
   * Renders a JSON_EXISTS predicate with type-safe filter expressions.
   */
  private void renderJsonExistsPredicate(SqlGenerationContext ctx, Expression filter) {
    JsonExistsPredicateBuilder builder = new JsonExistsPredicateBuilder(ctx);
    builder.render(filter);
  }

  /**
   * Renders a JSON_OBJECT for a $group stage result.
   * Constructs a JSON document with _id and accumulator fields.
   */
  private void renderJsonObjectGroup(SqlGenerationContext ctx, GroupStage groupStage) {
    ctx.sql("JSON_OBJECT(");

    boolean first = true;

    // Render _id field - use renderFieldAccess to match GROUP BY clause
    // Oracle requires GROUP BY expressions to match SELECT expressions exactly
    // Always include _id, even if null (MongoDB includes "_id": null in output)
    Expression idExpr = groupStage.getIdExpression();
    ctx.sql("'_id' VALUE ");
    if (idExpr != null) {
      renderFieldAccess(ctx, idExpr);
    } else {
      ctx.sql("null");
    }
    first = false;

    // Render accumulator fields
    for (Map.Entry<String, AccumulatorExpression> entry : groupStage.getAccumulators().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.sql("'" + entry.getKey() + "' VALUE ");
      renderAccumulator(ctx, entry.getValue());
      first = false;
    }

    ctx.sql(")");
  }

  /**
   * Renders a JSON_OBJECT for GROUP stage when preceded by a SORT stage.
   * Uses Oracle KEEP clause for $first/$last accumulators to respect sort order.
   */
  private void renderJsonObjectGroupWithSort(
      SqlGenerationContext ctx, GroupStage groupStage, SortStage sortStage) {
    ctx.sql("JSON_OBJECT(");

    boolean first = true;

    // Render _id field - use renderFieldAccess to match GROUP BY clause
    Expression idExpr = groupStage.getIdExpression();
    ctx.sql("'_id' VALUE ");
    if (idExpr != null) {
      renderFieldAccess(ctx, idExpr);
    } else {
      ctx.sql("null");
    }
    first = false;

    // Render accumulator fields - pass sort info for $first/$last
    for (Map.Entry<String, AccumulatorExpression> entry : groupStage.getAccumulators().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.sql("'" + entry.getKey() + "' VALUE ");
      renderAccumulatorWithSort(ctx, entry.getValue(), sortStage);
      first = false;
    }

    ctx.sql(")");
  }

  /**
   * Renders an accumulator expression with preceding sort context.
   * Uses Oracle KEEP clause for $first/$last to respect sort order.
   */
  private void renderAccumulatorWithSort(
      SqlGenerationContext ctx, AccumulatorExpression accum, SortStage sortStage) {
    var accumOp = accum.getOp();

    if (accumOp == AccumulatorOp.FIRST || accumOp == AccumulatorOp.LAST) {
      // Use MIN/MAX with KEEP (DENSE_RANK FIRST/LAST ORDER BY ...) for proper ordering
      // Use CASE to handle both JSON objects/arrays and scalar values correctly
      String func = accumOp == AccumulatorOp.FIRST ? "MIN" : "MAX";
      String rankDir = accumOp == AccumulatorOp.FIRST ? "FIRST" : "LAST";
      // Build the aggregate expression string for reuse
      ctx.sql("CASE WHEN SUBSTR(" + func + "(");
      renderAccumulatorArg(ctx, accum.getArgument());
      ctx.sql(") KEEP (DENSE_RANK " + rankDir + " ORDER BY ");
      renderSortKeysForKeep(ctx, sortStage);
      ctx.sql("), 1, 1) IN ('{', '[') THEN " + func + "(");
      renderAccumulatorArg(ctx, accum.getArgument());
      ctx.sql(") KEEP (DENSE_RANK " + rankDir + " ORDER BY ");
      renderSortKeysForKeep(ctx, sortStage);
      ctx.sql(") ELSE '\"' || " + func + "(");
      renderAccumulatorArg(ctx, accum.getArgument());
      ctx.sql(") KEEP (DENSE_RANK " + rankDir + " ORDER BY ");
      renderSortKeysForKeep(ctx, sortStage);
      ctx.sql(") || '\"' END FORMAT JSON");
    } else {
      // For other accumulators, use regular rendering
      renderAccumulator(ctx, accum);
    }
  }

  /**
   * Renders sort keys for Oracle KEEP clause.
   */
  private void renderSortKeysForKeep(SqlGenerationContext ctx, SortStage sortStage) {
    boolean first = true;
    for (SortStage.SortField field : sortStage.getSortFields()) {
      if (!first) {
        ctx.sql(", ");
      }
      String path = field.getFieldPath().getPath();
      String dir = field.getDirection() == SortStage.SortDirection.DESC ? "DESC" : "ASC";
      ctx.sql("JSON_VALUE(\"DATA\", '$." + path + "' RETURNING NUMBER NULL ON ERROR) ");
      ctx.sql(dir);
      ctx.sql(" NULLS LAST");
      first = false;
    }
  }

  /**
   * Renders a field access expression for GROUP BY and JSON_OBJECT _id contexts.
   * Uses dot notation with table alias (q."DATA".field) to preserve native JSON types.
   * This works because we wrap CTE references in inline views, which enables dot notation.
   * For WHERE clause comparisons, use renderFieldAccessForWhere() instead.
   */
  private void renderFieldAccess(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fieldPath) {
      // Use dot notation with table alias for type preservation
      // This works with inline view wrapper: FROM (SELECT * FROM "CTE") q
      ctx.sql("q.\"DATA\"." + quoteDotNotationPath(fieldPath.getPath()));
    } else if (expr instanceof DateExpression dateExpr) {
      // Custom CTE-mode rendering for DateExpression
      renderDateExpressionCte(ctx, dateExpr);
    } else if (expr instanceof CompoundIdExpression compoundId) {
      // Render compound _id as JSON_OBJECT
      renderCompoundIdAsJsonObject(ctx, compoundId);
    } else if (expr instanceof ConditionalExpression condExpr) {
      // Custom CTE-mode rendering for ConditionalExpression
      renderConditionalExpression(ctx, condExpr);
    } else if (expr instanceof LiteralExpression lit) {
      // Literal values are rendered as-is
      Object value = lit.getValue();
      if (value instanceof String) {
        ctx.sql("'" + value + "'");
      } else {
        ctx.sql(String.valueOf(value));
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      // Arithmetic expressions like $multiply, $add, etc.
      renderArithmeticExpression(ctx, arith);
    } else if (expr instanceof ArrayExpression arrayExpr) {
      // Array expressions like $size
      renderArrayExpressionForFieldAccess(ctx, arrayExpr);
    } else if (expr instanceof InlineObjectExpression objExpr) {
      // Inline object expression for $push: {key1: $field1, key2: $field2}
      renderInlineObjectAsJsonObject(ctx, objExpr);
    } else if (expr instanceof StringExpression stringExpr) {
      // String expressions like $concat, $toLower, $toUpper
      renderStringExpression(ctx, stringExpr);
    } else {
      // Fallback for other expressions - try rendering them
      ctx.sql("NULL");
    }
  }

  /**
   * Renders a field access expression for WHERE clause comparison contexts.
   * Uses dot notation to preserve native JSON types (numbers remain numbers, booleans remain
   * booleans).
   * Do NOT use this for GROUP BY contexts - Oracle doesn't support dot notation in GROUP BY.
   * Uses table alias q."DATA" for CTE context compatibility.
   */
  private void renderFieldAccessForWhere(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fieldPath) {
      // Use dot notation with table alias for type-preserving field access
      // The q. prefix is needed in CTE contexts where we reference the previous CTE
      ctx.sql("q.\"DATA\"." + fieldPath.getDotNotationPath());
    } else if (expr instanceof DateExpression dateExpr) {
      // Custom CTE-mode rendering for DateExpression
      renderDateExpressionCte(ctx, dateExpr);
    } else if (expr instanceof ConditionalExpression condExpr) {
      // Custom CTE-mode rendering for ConditionalExpression
      renderConditionalExpression(ctx, condExpr);
    } else if (expr instanceof LiteralExpression lit) {
      // Literal values are rendered as-is
      Object value = lit.getValue();
      if (value instanceof String) {
        ctx.sql("'" + value + "'");
      } else {
        ctx.sql(String.valueOf(value));
      }
    } else if (expr instanceof ArithmeticExpression arith) {
      // Arithmetic expressions like $multiply, $add, etc.
      renderArithmeticExpression(ctx, arith);
    } else if (expr instanceof ArrayExpression arrayExpr) {
      // Array expressions like $size
      renderArrayExpressionForFieldAccess(ctx, arrayExpr);
    } else {
      // Fallback to general renderFieldAccess
      renderFieldAccess(ctx, expr);
    }
  }

  /**
   * Renders a field access expression with type preservation for JSON_OBJECT contexts.
   * Uses JSON_QUERY which naturally preserves original JSON types (numbers, booleans, null)
   * and handles non-scalar values (objects, arrays) as well.
   * The same expression must be used in GROUP BY (see renderFieldAccessForGroupBy).
   */
  private void renderFieldAccessWithFormatJson(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fieldPath) {
      String path = fieldPath.getPath();
      // Use JSON_QUERY to preserve original JSON types and handle non-scalars
      ctx.sql("JSON_QUERY(\"DATA\", '$." + path + "')");
    } else if (expr instanceof DateExpression dateExpr) {
      // Date expressions return numbers, render normally
      renderDateExpressionCte(ctx, dateExpr);
    } else if (expr instanceof ArithmeticExpression arith) {
      // Arithmetic expressions return numbers
      renderArithmeticExpression(ctx, arith);
    } else if (expr instanceof LiteralExpression lit) {
      // Literal values are rendered as-is
      Object value = lit.getValue();
      if (value instanceof String) {
        ctx.sql("'" + value + "'");
      } else {
        ctx.sql(String.valueOf(value));
      }
    } else {
      // Fallback to regular rendering
      renderFieldAccess(ctx, expr);
    }
  }

  /**
   * Renders an ArrayExpression for use in field access context (comparisons, etc.).
   * Handles $size specially to produce numeric result for comparisons.
   */
  private void renderArrayExpressionForFieldAccess(
      SqlGenerationContext ctx, ArrayExpression arrayExpr) {
    ArrayOp op = arrayExpr.getOp();
    if (op == ArrayOp.SIZE) {
      // $size: render as JSON_VALUE with .size() path for numeric result
      Expression arrayArg = arrayExpr.getArrayExpression();
      if (arrayArg instanceof FieldPathExpression fp) {
        ctx.sql("JSON_VALUE(\"DATA\", '$." + fp.getPath() + ".size()' RETURNING NUMBER)");
      } else {
        ctx.sql("0");
      }
    } else {
      // For other array ops, fall back to full array expression rendering
      renderArrayExpression(ctx, arrayExpr);
    }
  }

  /**
   * Renders an InlineObjectExpression as JSON_OBJECT.
   * Used for $push with object argument: {productId: "$_id.productId", ...}
   * Uses renderExpressionValue for type preservation in output.
   */
  private void renderInlineObjectAsJsonObject(
      SqlGenerationContext ctx, InlineObjectExpression objExpr) {
    ctx.sql("JSON_OBJECT(");
    boolean first = true;
    for (Map.Entry<String, Expression> entry : objExpr.getFields().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.sql("'" + entry.getKey() + "' VALUE ");
      renderExpressionValue(ctx, entry.getValue());
      first = false;
    }
    ctx.sql(")");
  }

  /**
   * Renders an ObjectExpression ($mergeObjects, $objectToArray, $arrayToObject) in CTE mode.
   * Uses "DATA" column instead of "base.data" for field access.
   */
  private void renderObjectExpression(SqlGenerationContext ctx, ObjectExpression objExpr) {
    ObjectOp op = objExpr.getOp();

    switch (op) {
      case MERGE_OBJECTS -> renderMergeObjects(ctx, objExpr);
      case OBJECT_TO_ARRAY -> renderObjectToArray(ctx, objExpr);
      case ARRAY_TO_OBJECT -> renderArrayToObject(ctx, objExpr);
      case GET_FIELD -> renderGetField(ctx, objExpr);
      default -> ctx.sql("NULL");
    }
  }

  /**
   * Renders $getField operator in CTE mode.
   * MongoDB: {$getField: {field: "name", input: "$customer"}} or dynamic field
   * Oracle: Uses JSON_VALUE with path or dynamic path construction
   */
  private void renderGetField(SqlGenerationContext ctx, ObjectExpression objExpr) {
    List<Expression> args = objExpr.getAdditionalArgs();
    Expression inputExpr = objExpr.getInputExpression();

    if (args == null || args.isEmpty()) {
      ctx.sql("NULL");
      return;
    }

    Expression fieldExpr = args.get(0);

    if (fieldExpr instanceof LiteralExpression lit && lit.getValue() instanceof String fieldName) {
      // Static field name - use simple JSON_VALUE
      ctx.sql("JSON_VALUE(");
      if (inputExpr instanceof FieldPathExpression fieldPath) {
        ctx.sql("\"DATA\", '$.");
        ctx.sql(fieldPath.getPath());
        ctx.sql(".");
        ctx.sql(fieldName);
        ctx.sql("')");
      } else {
        renderExpressionValue(ctx, inputExpr);
        ctx.sql(", '$.");
        ctx.sql(fieldName);
        ctx.sql("')");
      }
    } else if (fieldExpr instanceof FieldPathExpression dynamicField) {
      // Dynamic field name - the field name comes from another field in the document
      // Use PL/SQL function for dynamic JSON path construction
      String inputPath = (inputExpr instanceof FieldPathExpression fp) ? fp.getPath() : null;
      String keyPath = dynamicField.getPath();

      if (inputPath != null) {
        // Call PL/SQL helper function: get_dynamic_json_field(doc, object_path, key_path)
        // Returns JSON type to preserve numbers, booleans, etc.
        ctx.sql("get_dynamic_json_field(\"DATA\", '$." + inputPath + "', '$." + keyPath + "')");
      } else {
        // Fallback when input is not a simple field path
        ctx.sql("NULL /* dynamic $getField with complex input not supported */");
      }
    } else {
      // Fallback for other expression types
      ctx.sql("NULL");
    }
  }

  /**
   * Renders $mergeObjects operator in CTE mode.
   * Uses JSON_MERGEPATCH to combine objects.
   */
  private void renderMergeObjects(SqlGenerationContext ctx, ObjectExpression objExpr) {
    List<Expression> args = objExpr.getAdditionalArgs();

    if (args == null || args.isEmpty()) {
      ctx.sql("JSON_OBJECT()");
      return;
    }

    if (args.size() == 1) {
      // Single object - just return it
      Expression obj = args.get(0);
      if (obj instanceof FieldPathExpression fieldPath) {
        ctx.sql("JSON_QUERY(\"DATA\", '$." + fieldPath.getPath() + "')");
      } else if (obj instanceof InlineObjectExpression inlineObj) {
        renderInlineObjectAsJsonObject(ctx, inlineObj);
      } else {
        renderExpressionValue(ctx, obj);
      }
      return;
    }

    // Multiple objects - chain JSON_MERGEPATCH calls
    int depth = args.size() - 1;

    // Open all the JSON_MERGEPATCH calls
    for (int i = 0; i < depth; i++) {
      ctx.sql("JSON_MERGEPATCH(");
    }

    // Render first object
    Expression first = args.get(0);
    renderMergeObjectArg(ctx, first);

    // Render remaining objects with closing parentheses
    for (int i = 1; i < args.size(); i++) {
      ctx.sql(", ");
      renderMergeObjectArg(ctx, args.get(i));
      ctx.sql(")");
    }
  }

  /**
   * Renders a single argument to $mergeObjects.
   */
  private void renderMergeObjectArg(SqlGenerationContext ctx, Expression obj) {
    if (obj instanceof FieldPathExpression fieldPath) {
      ctx.sql("JSON_QUERY(\"DATA\", '$." + fieldPath.getPath() + "')");
    } else if (obj instanceof InlineObjectExpression inlineObj) {
      renderInlineObjectAsJsonObject(ctx, inlineObj);
    } else {
      renderExpressionValue(ctx, obj);
    }
  }

  /**
   * Renders $objectToArray operator in CTE mode.
   */
  private void renderObjectToArray(SqlGenerationContext ctx, ObjectExpression objExpr) {
    Expression input = objExpr.getInputExpression();

    ctx.sql("(SELECT JSON_ARRAYAGG(JSON_OBJECT('k' VALUE key_col, 'v' VALUE val_col)) ");
    ctx.sql("FROM JSON_TABLE(");

    if (input instanceof FieldPathExpression fieldPath) {
      ctx.sql("\"DATA\", '$." + fieldPath.getPath());
    } else {
      ctx.sql("\"DATA\", '$");
    }

    ctx.sql(".*' COLUMNS (key_col VARCHAR2(4000) PATH '$.@key', ");
    ctx.sql("val_col VARCHAR2(4000) FORMAT JSON PATH '$')))");
  }

  /**
   * Renders $arrayToObject operator in CTE mode.
   */
  private void renderArrayToObject(SqlGenerationContext ctx, ObjectExpression objExpr) {
    Expression input = objExpr.getInputExpression();

    ctx.sql("(SELECT JSON_OBJECTAGG(key_col VALUE val_col) ");
    ctx.sql("FROM JSON_TABLE(");

    if (input instanceof FieldPathExpression fieldPath) {
      ctx.sql("\"DATA\", '$." + fieldPath.getPath());
    } else {
      ctx.sql("\"DATA\", '$");
    }

    ctx.sql("[*]' COLUMNS (key_col VARCHAR2(4000) PATH '$.k', ");
    ctx.sql("val_col VARCHAR2(4000) FORMAT JSON PATH '$.v')))");
  }

  /**
   * Renders the newRoot expression for $replaceRoot in CTE mode.
   * The result becomes the new DATA column, replacing the entire document.
   */
  private void renderReplaceRootExpression(SqlGenerationContext ctx, Expression newRoot) {
    if (newRoot instanceof FieldPathExpression fieldPath) {
      // Replace with a subdocument: {$replaceRoot: {newRoot: "$subdoc"}}
      ctx.sql("JSON_QUERY(q.\"DATA\", '$." + fieldPath.getPath() + "')");
    } else if (newRoot instanceof InlineObjectExpression inlineObj) {
      // Replace with a constructed object: {$replaceRoot: {newRoot: {a: "$b", c: "$d"}}}
      ctx.sql("JSON_OBJECT(");
      boolean first = true;
      for (Map.Entry<String, Expression> entry : inlineObj.getFields().entrySet()) {
        if (!first) {
          ctx.sql(", ");
        }
        ctx.sql("'" + entry.getKey() + "' VALUE ");
        renderReplaceRootFieldValue(ctx, entry.getValue());
        first = false;
      }
      ctx.sql(" RETURNING JSON)");
    } else if (newRoot instanceof ObjectExpression objExpr) {
      // Replace with an object expression: {$replaceRoot: {newRoot: {$mergeObjects: [...]}}}
      renderObjectExpression(ctx, objExpr);
    } else {
      // Fallback - try to render as expression value
      renderExpressionValue(ctx, newRoot);
    }
  }

  /**
   * Renders a field value for $replaceRoot inline object construction.
   * Handles field paths and nested field paths.
   */
  private void renderReplaceRootFieldValue(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fieldPath) {
      String path = fieldPath.getPath();
      // Use JSON_QUERY without wrapper - returns JSON scalar, object, or array as-is
      ctx.sql("JSON_QUERY(q.\"DATA\", '$." + path + "')");
    } else {
      renderExpressionValue(ctx, expr);
    }
  }

  /**
   * Renders a ConditionalExpression ($cond) in CTE mode.
   * Produces CASE WHEN ... THEN ... ELSE ... END with proper DATA column references.
   */
  private void renderConditionalExpression(
      SqlGenerationContext ctx, ConditionalExpression condExpr) {
    if (condExpr.getType() == ConditionalExpression.ConditionalType.IF_NULL) {
      // $ifNull: NVL/COALESCE(expr, replacement)
      // Handle type-specific access to avoid mismatches
      Expression valueExpr = condExpr.getThenExpr();
      Expression defaultExpr = condExpr.getElseExpr();

      if (isArrayLiteral(defaultExpr) && valueExpr instanceof FieldPathExpression fp) {
        // Array default: use JSON_QUERY for field path
        ctx.sql("COALESCE(JSON_QUERY(\"DATA\", '$." + fp.getPath() + "'), ");
        renderExpressionValue(ctx, defaultExpr);
        ctx.sql(")");
      } else if (isNumberLiteral(defaultExpr) && valueExpr instanceof FieldPathExpression fp) {
        // Number default: use NVL with RETURNING NUMBER
        ctx.sql("NVL(JSON_VALUE(\"DATA\", '$." + fp.getPath() + "' RETURNING NUMBER), ");
        renderExpressionValue(ctx, defaultExpr);
        ctx.sql(")");
      } else if (isNumberLiteral(defaultExpr) && valueExpr instanceof ArrayExpression ae
          && ae.getOp() == ArrayOp.ARRAY_ELEM_AT) {
        // $arrayElemAt with number default: use NVL with RETURNING NUMBER
        ctx.sql("NVL(");
        renderArrayElemAtForNumber(ctx, ae);
        ctx.sql(", ");
        renderExpressionValue(ctx, defaultExpr);
        ctx.sql(")");
      } else if (isNumberLiteral(defaultExpr) && valueExpr instanceof ObjectExpression objExpr
          && objExpr.getOp() == ObjectOp.GET_FIELD) {
        // $getField with number default: PL/SQL function returns JSON, wrap default
        ctx.sql("COALESCE(");
        renderObjectExpression(ctx, objExpr);
        ctx.sql(", JSON('");
        ctx.sql(String.valueOf(((LiteralExpression) defaultExpr).getValue()));
        ctx.sql("'))");
      } else {
        // General case
        ctx.sql("COALESCE(");
        renderExpressionValue(ctx, valueExpr);
        ctx.sql(", ");
        renderExpressionValue(ctx, defaultExpr);
        ctx.sql(")");
      }
    } else {
      // $cond: CASE WHEN condition THEN thenValue ELSE elseValue END
      // Must ensure type consistency between THEN and ELSE branches
      final Expression thenExpr = condExpr.getThenExpr();
      final Expression elseExpr = condExpr.getElseExpr();
      final boolean thenReturnsJson = returnsJsonType(thenExpr);
      final boolean elseReturnsJson = returnsJsonType(elseExpr);
      final boolean thenIsNumericLiteral = isNumberLiteral(thenExpr);
      final boolean elseIsNumericLiteral = isNumberLiteral(elseExpr);

      ctx.sql("CASE WHEN ");
      renderConditionExpression(ctx, condExpr.getCondition());
      ctx.sql(" THEN ");

      if (thenReturnsJson && elseIsNumericLiteral) {
        // THEN returns JSON_QUERY (VARCHAR2), ELSE is numeric - use string literal
        renderExpressionValue(ctx, thenExpr);
        ctx.sql(" ELSE '");
        ctx.sql(String.valueOf(((LiteralExpression) elseExpr).getValue()));
        ctx.sql("'");
      } else if (elseReturnsJson && thenIsNumericLiteral) {
        // ELSE returns JSON_QUERY (VARCHAR2), THEN is numeric - use string literal
        ctx.sql("'");
        ctx.sql(String.valueOf(((LiteralExpression) thenExpr).getValue()));
        ctx.sql("' ELSE ");
        renderExpressionValue(ctx, elseExpr);
      } else {
        // No type mismatch or both same type
        renderExpressionValue(ctx, thenExpr);
        ctx.sql(" ELSE ");
        renderExpressionValue(ctx, elseExpr);
      }
      ctx.sql(" END");
    }
  }

  /**
   * Checks if an expression returns a JSON type (vs. a scalar type).
   * Used for ensuring CASE branch type consistency.
   */
  private boolean returnsJsonType(Expression expr) {
    // Field paths return JSON_QUERY which produces JSON/CLOB
    if (expr instanceof FieldPathExpression) {
      return true;
    }
    // Nested conditionals may return JSON
    if (expr instanceof ConditionalExpression condExpr) {
      return returnsJsonType(condExpr.getThenExpr()) || returnsJsonType(condExpr.getElseExpr());
    }
    // $getField returns JSON
    if (expr instanceof ObjectExpression objExpr && objExpr.getOp() == ObjectOp.GET_FIELD) {
      return true;
    }
    return false;
  }

  /**
   * Renders a condition expression (for CASE WHEN) in CTE mode.
   * Handles InExpression, ArrayExpression ($in), ComparisonExpression, etc.
   */
  private void renderConditionExpression(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof ArrayExpression arrayExpr && arrayExpr.getOp() == ArrayOp.IN) {
      // $in expression form: {$in: ["$status", ["delivered", "shipped"]]}
      // indexExpression = "$status", arrayExpression = ["delivered", "shipped"]
      Expression valueExpr = arrayExpr.getIndexExpression();
      Expression arrayValueExpr = arrayExpr.getArrayExpression();
      renderExpressionValue(ctx, valueExpr);
      ctx.sql(" IN (");
      if (arrayValueExpr instanceof LiteralExpression litArray
          && litArray.getValue() instanceof java.util.List<?> list) {
        boolean first = true;
        for (Object item : list) {
          if (!first) {
            ctx.sql(", ");
          }
          if (item instanceof String s) {
            ctx.sql("'" + s.replace("'", "''") + "'");
          } else {
            ctx.sql(String.valueOf(item));
          }
          first = false;
        }
      }
      ctx.sql(")");
    } else if (expr instanceof InExpression inExpr) {
      // Query form: {field: {$in: [values...]}}
      Expression field = inExpr.getField();
      if (field instanceof FieldPathExpression fp) {
        // Use dot notation with table alias for type-preserving field access
        ctx.sql("q.\"DATA\"." + fp.getDotNotationPath());
      } else {
        renderExpressionValue(ctx, field);
      }
      if (inExpr.isNegated()) {
        ctx.sql(" NOT IN (");
      } else {
        ctx.sql(" IN (");
      }
      boolean first = true;
      for (Object value : inExpr.getValues()) {
        if (!first) {
          ctx.sql(", ");
        }
        if (value instanceof String) {
          ctx.sql("'" + value + "'");
        } else {
          ctx.sql(String.valueOf(value));
        }
        first = false;
      }
      ctx.sql(")");
    } else if (expr instanceof ComparisonExpression comp) {
      // Handle null comparisons specially - SQL requires IS NULL / IS NOT NULL
      Expression right = comp.getRight();
      if (right instanceof LiteralExpression lit && lit.getValue() == null) {
        renderFieldAccessForWhere(ctx, comp.getLeft());
        if (comp.getOp() == ComparisonOp.EQ) {
          ctx.sql(" IS NULL");
        } else if (comp.getOp() == ComparisonOp.NE) {
          ctx.sql(" IS NOT NULL");
        } else {
          ctx.sql(" IS NOT NULL");
        }
      } else {
        // Render: field op right - using dot notation for type preservation
        // Use renderFieldAccessForWhere (not renderExpressionValue) for comparisons
        // because JSON_QUERY returns quoted values like "100" which can't be compared numerically
        renderFieldAccessForWhere(ctx, comp.getLeft());
        String op = switch (comp.getOp()) {
          case EQ -> " = ";
          case NE -> " != ";
          case GT -> " > ";
          case GTE -> " >= ";
          case LT -> " < ";
          case LTE -> " <= ";
          default -> " = ";
        };
        ctx.sql(op);
        renderFieldAccessForWhere(ctx, right);
      }
    } else if (expr instanceof LogicalExpression logExpr) {
      // Handle $and, $or, $not, $nor
      var operands = logExpr.getOperands();
      var op = logExpr.getOp();
      switch (op) {
        case AND -> {
          ctx.sql("(");
          for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
              ctx.sql(" AND ");
            }
            renderConditionExpression(ctx, operands.get(i));
          }
          ctx.sql(")");
        }
        case OR -> {
          ctx.sql("(");
          for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
              ctx.sql(" OR ");
            }
            renderConditionExpression(ctx, operands.get(i));
          }
          ctx.sql(")");
        }
        case NOT -> {
          ctx.sql("NOT (");
          renderConditionExpression(ctx, operands.get(0));
          ctx.sql(")");
        }
        case NOR -> {
          ctx.sql("NOT (");
          for (int i = 0; i < operands.size(); i++) {
            if (i > 0) {
              ctx.sql(" OR ");
            }
            renderConditionExpression(ctx, operands.get(i));
          }
          ctx.sql(")");
        }
        default -> ctx.sql("1=1");
      }
    } else {
      // Fallback
      ctx.sql("1=1");
    }
  }

  /**
   * Renders a CompoundIdExpression as a JSON_OBJECT with type preservation.
   * Uses FORMAT JSON to preserve original JSON types (numbers, booleans, null).
   * Oracle requires GROUP BY expressions to match SELECT expressions exactly.
   * Note: For GROUP BY compatibility, we still use JSON_VALUE for GROUP BY clause,
   * but FORMAT JSON in JSON_OBJECT ensures the output preserves types.
   */
  private void renderCompoundIdAsJsonObject(
      SqlGenerationContext ctx, CompoundIdExpression compoundId) {
    ctx.sql("JSON_OBJECT(");
    boolean first = true;
    for (Map.Entry<String, Expression> entry : compoundId.getFields().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.sql("'" + entry.getKey() + "' VALUE ");
      // Use renderFieldAccess to match GROUP BY clause, then add FORMAT JSON
      // to preserve original JSON types (numbers stay numbers, not strings)
      renderFieldAccessWithFormatJson(ctx, entry.getValue());
      first = false;
    }
    ctx.sql(")");
  }

  /**
   * Renders a CompoundIdExpression as a JSON_OBJECT with type preservation.
   * Uses JSON_QUERY for field paths to preserve original JSON types (numbers, booleans).
   * Used for JSON_OBJECT VALUE contexts where type preservation is needed.
   */
  private void renderCompoundIdAsJsonObjectPreserveTypes(
      SqlGenerationContext ctx, CompoundIdExpression compoundId) {
    ctx.sql("JSON_OBJECT(");
    boolean first = true;
    for (Map.Entry<String, Expression> entry : compoundId.getFields().entrySet()) {
      if (!first) {
        ctx.sql(", ");
      }
      ctx.sql("'" + entry.getKey() + "' VALUE ");
      // Use renderExpressionValue for type preservation
      renderExpressionValue(ctx, entry.getValue());
      first = false;
    }
    ctx.sql(")");
  }

  /**
   * Checks if an expression is known to produce a numeric value.
   * Used to determine whether to use RETURNING NUMBER when accessing virtual fields.
   *
   * @param expr the expression to check
   * @return true if the expression produces a numeric value
   */
  private boolean isNumericExpression(Expression expr) {
    if (expr == null) {
      return false;
    }
    return expr instanceof DateExpression
        || expr instanceof ArithmeticExpression
        || (expr instanceof LiteralExpression lit && lit.getValue() instanceof Number);
  }

  /**
   * Renders a DateExpression in CTE mode.
   * Uses "DATA" column instead of "base.data" for field access.
   */
  private void renderDateExpressionCte(SqlGenerationContext ctx, DateExpression dateExpr) {
    Expression argument = dateExpr.getArgument();

    // Build timestamp expression
    StringBuilder timestampExpr = new StringBuilder();
    timestampExpr.append("TO_TIMESTAMP(");

    if (argument instanceof FieldPathExpression fieldPath) {
      // Use "DATA" column directly in CTE mode
      timestampExpr.append("JSON_VALUE(\"DATA\", '$.");
      timestampExpr.append(fieldPath.getPath());
      timestampExpr.append("')");
    } else {
      // For other expressions, render normally
      timestampExpr.append("NULL");
    }

    timestampExpr.append(", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3\"Z\"')");
    String timestampStr = timestampExpr.toString();

    // Render the appropriate SQL for the date operation
    final DateOp op = dateExpr.getOp();

    // Special handling for $week: MongoDB uses Sunday-start weeks (0-53)
    // Week 0 = days before first Sunday of year
    // Week 1+ = starting from first Sunday
    // This differs from Oracle's 'IW' (ISO week, Monday-start, 1-53)
    if (op == DateOp.WEEK) {
      ctx.sql("CASE WHEN TRUNC(");
      ctx.sql(timestampStr);
      ctx.sql(") < NEXT_DAY(TRUNC(");
      ctx.sql(timestampStr);
      ctx.sql(", 'YYYY') - INTERVAL '1' DAY, 'SUNDAY') THEN 0 ELSE ");
      ctx.sql("FLOOR((TRUNC(");
      ctx.sql(timestampStr);
      ctx.sql(") - NEXT_DAY(TRUNC(");
      ctx.sql(timestampStr);
      ctx.sql(", 'YYYY') - INTERVAL '1' DAY, 'SUNDAY')) / 7) + 1 END");
      return;
    }

    if (op.isExtractBased()) {
      ctx.sql(String.format(op.getSqlTemplate(), timestampStr));
    } else {
      ctx.sql("TO_NUMBER(");
      ctx.sql(String.format(op.getSqlTemplate(), timestampStr));
      ctx.sql(")");
    }
  }

  /**
   * Renders an accumulator expression (SUM, COUNT, AVG, etc.).
   * Uses appropriate JSON_VALUE/JSON_QUERY based on accumulator type:
   * - SUM, AVG: JSON_VALUE with RETURNING NUMBER for numeric operations
   * - PUSH, ADD_TO_SET: renderExpressionValue (JSON_QUERY) for type preservation
   * - Others: JSON_VALUE for general value extraction
   */
  private void renderAccumulator(SqlGenerationContext ctx, AccumulatorExpression accum) {
    var accumOp = accum.getOp();

    switch (accumOp) {
      case COUNT -> ctx.sql("COUNT(*)");
      case SUM, AVG -> {
        // Numeric accumulators need RETURNING NUMBER
        String func = accumOp == AccumulatorOp.SUM ? "SUM" : "AVG";
        ctx.sql(func + "(");
        renderNumericAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(")");
      }
      case MIN, MAX, FIRST, LAST -> {
        // Use MIN/MAX for FIRST/LAST approximation
        // Use CASE to handle both JSON objects/arrays and scalar values correctly:
        // - If value starts with { or [, it's a JSON object/array - use directly
        // - Otherwise it's a scalar (string/number/boolean) - wrap in quotes
        // Both cases use FORMAT JSON to embed properly in JSON_OBJECT
        String func = (accumOp == AccumulatorOp.MIN || accumOp == AccumulatorOp.FIRST)
            ? "MIN" : "MAX";
        ctx.sql("CASE WHEN SUBSTR(" + func + "(");
        renderDotNotationAccumulatorArg(ctx, accum.getArgument());
        ctx.sql("), 1, 1) IN ('{', '[') THEN " + func + "(");
        renderDotNotationAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(") ELSE '\"' || " + func + "(");
        renderDotNotationAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(") || '\"' END FORMAT JSON");
      }
      case PUSH -> {
        // $push: collect all values into array
        ctx.sql("JSON_ARRAYAGG(");
        renderExpressionValue(ctx, accum.getArgument());
        ctx.sql(" RETURNING JSON)");
      }
      case ADD_TO_SET -> {
        // $addToSet: collect unique values only
        // Oracle doesn't support JSON_ARRAYAGG(DISTINCT), so use LISTAGG DISTINCT
        // and convert to JSON array: JSON_QUERY('["' || LISTAGG(DISTINCT v, '","') || '"]')
        ctx.sql("JSON_QUERY('[\"' || LISTAGG(DISTINCT ");
        renderStringArgument(ctx, accum.getArgument());
        ctx.sql(", '\",\"') WITHIN GROUP (ORDER BY ");
        renderStringArgument(ctx, accum.getArgument());
        ctx.sql(") || '\"]', '$' RETURNING JSON)");
      }
      default -> ctx.sql("NULL");
    }
  }

  /**
   * Renders an accumulator argument that needs numeric output.
   * Handles FieldPath, Literal, Arithmetic, ArrayExpression ($size), ConditionalExpression.
   * Uses dot notation for type-preserving access - Oracle handles numeric conversion naturally.
   */
  private void renderNumericAccumulatorArg(SqlGenerationContext ctx, Expression arg) {
    if (arg instanceof FieldPathExpression fp) {
      // Use dot notation for type-preserving access (no CAST - let Oracle handle naturally)
      ctx.sql("q.\"DATA\"." + quoteDotNotationPath(fp.getPath()));
    } else if (arg instanceof LiteralExpression lit) {
      // $sum: 1 case
      Object val = lit.getValue();
      if (val instanceof Number) {
        ctx.sql(String.valueOf(val));
      } else {
        ctx.sql("NULL");
      }
    } else if (arg instanceof ArithmeticExpression arith) {
      renderArithmeticExpression(ctx, arith);
    } else if (arg instanceof ArrayExpression arrayExpr && arrayExpr.getOp() == ArrayOp.SIZE) {
      // $sum: {$size: "$events"} - use .size() JSON path function (requires JSON_VALUE)
      Expression sizeArg = arrayExpr.getArrayExpression();
      if (sizeArg instanceof FieldPathExpression fp) {
        ctx.sql("JSON_VALUE(q.\"DATA\", '$." + fp.getPath() + ".size()' RETURNING NUMBER)");
      } else {
        ctx.sql("NULL");
      }
    } else if (arg instanceof ConditionalExpression condExpr) {
      // $sum: {$cond: [{$eq: ["$status", "completed"]}, 1, 0]}
      renderConditionalAsValue(ctx, condExpr);
    } else {
      ctx.sql("NULL");
    }
  }

  /**
   * Renders a $bucketAuto groupBy field using dot notation for type preservation.
   * Uses q."DATA".field pattern with inline view wrapper for CTE access.
   */
  private void renderBucketAutoGroupByField(SqlGenerationContext ctx, Expression arg) {
    if (arg instanceof FieldPathExpression fp) {
      // Use dot notation for type-preserving access
      ctx.sql("q.\"DATA\"." + quoteDotNotationPath(fp.getPath()));
    } else if (arg instanceof LiteralExpression lit) {
      Object val = lit.getValue();
      if (val instanceof Number) {
        ctx.sql(String.valueOf(val));
      } else {
        ctx.sql("NULL");
      }
    } else if (arg instanceof ArithmeticExpression arith) {
      renderArithmeticExpression(ctx, arith);
    } else {
      ctx.sql("NULL");
    }
  }

  /**
   * Renders an accumulator for $bucketAuto output fields.
   * Unlike regular accumulators, BUCKETAUTO accumulators reference the "DATA" column
   * directly (without q. prefix) since the outer SELECT doesn't have the q alias in scope.
   */
  private void renderBucketAutoAccumulator(SqlGenerationContext ctx, AccumulatorExpression accum) {
    var accumOp = accum.getOp();

    switch (accumOp) {
      case COUNT -> ctx.sql("COUNT(*)");
      case SUM, AVG -> {
        // Numeric accumulators need RETURNING NUMBER
        String func = accumOp == AccumulatorOp.SUM ? "SUM" : "AVG";
        ctx.sql(func + "(");
        renderBucketAutoAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(")");
      }
      case MIN, MAX, FIRST, LAST -> {
        // MIN/MAX - access DATA column directly
        // Use CASE to handle both JSON objects/arrays and scalar values correctly
        String func = (accumOp == AccumulatorOp.MIN || accumOp == AccumulatorOp.FIRST)
            ? "MIN" : "MAX";
        ctx.sql("CASE WHEN SUBSTR(" + func + "(");
        renderBucketAutoAccumulatorArg(ctx, accum.getArgument());
        ctx.sql("), 1, 1) IN ('{', '[') THEN " + func + "(");
        renderBucketAutoAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(") ELSE '\"' || " + func + "(");
        renderBucketAutoAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(") || '\"' END FORMAT JSON");
      }
      case PUSH -> {
        ctx.sql("JSON_ARRAYAGG(");
        renderBucketAutoAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(" RETURNING JSON)");
      }
      case ADD_TO_SET -> {
        ctx.sql("JSON_QUERY('[\"' || LISTAGG(DISTINCT ");
        renderBucketAutoAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(", '\",\"') WITHIN GROUP (ORDER BY ");
        renderBucketAutoAccumulatorArg(ctx, accum.getArgument());
        ctx.sql(") || '\"]', '$' RETURNING JSON)");
      }
      default -> ctx.sql("NULL");
    }
  }

  /**
   * Renders an accumulator argument for $bucketAuto.
   * Uses JSON_VALUE on the DATA column directly (no table alias prefix).
   */
  private void renderBucketAutoAccumulatorArg(SqlGenerationContext ctx, Expression arg) {
    if (arg instanceof FieldPathExpression fp) {
      // Access DATA column directly - no q. prefix since we're in outer SELECT
      ctx.sql("JSON_VALUE(\"DATA\", '$." + fp.getPath() + "')");
    } else if (arg instanceof LiteralExpression lit) {
      Object val = lit.getValue();
      if (val instanceof Number) {
        ctx.sql(String.valueOf(val));
      } else if (val instanceof String s) {
        ctx.sql("'" + s.replace("'", "''") + "'");
      } else if (val == null) {
        ctx.sql("NULL");
      } else {
        ctx.sql(String.valueOf(val));
      }
    } else {
      ctx.sql("NULL");
    }
  }

  /**
   * Renders an accumulator argument for MIN/MAX/FIRST/LAST.
   * Handles FieldPath, Literal, ConditionalExpression.
   */
  private void renderAccumulatorArg(SqlGenerationContext ctx, Expression arg) {
    if (arg instanceof FieldPathExpression fp) {
      ctx.sql("JSON_VALUE(\"DATA\", '$." + fp.getPath() + "')");
    } else if (arg instanceof LiteralExpression lit) {
      Object val = lit.getValue();
      if (val instanceof String s) {
        ctx.sql("'" + s.replace("'", "''") + "'");
      } else if (val instanceof Number) {
        ctx.sql(String.valueOf(val));
      } else if (val == null) {
        ctx.sql("NULL");
      } else {
        ctx.sql(String.valueOf(val));
      }
    } else if (arg instanceof ConditionalExpression condExpr) {
      // $max: {$cond: [$isConversion, 1, 0]}
      renderConditionalAsValue(ctx, condExpr);
    } else {
      renderFieldAccess(ctx, arg);
    }
  }

  /**
   * Renders an accumulator argument using dot notation for type preservation.
   * Used by MIN/MAX/FIRST/LAST to work correctly with numbers, strings, and dates.
   * Uses q."DATA".field pattern for type-preserving access.
   */
  private void renderDotNotationAccumulatorArg(SqlGenerationContext ctx, Expression arg) {
    if (arg instanceof FieldPathExpression fp) {
      // Use dot notation for type-preserving access: q."DATA".field
      ctx.sql("q.\"DATA\"." + quoteDotNotationPath(fp.getPath()));
    } else if (arg instanceof LiteralExpression lit) {
      Object val = lit.getValue();
      if (val instanceof String s) {
        ctx.sql("'" + s.replace("'", "''") + "'");
      } else if (val instanceof Number) {
        ctx.sql(String.valueOf(val));
      } else if (val == null) {
        ctx.sql("NULL");
      } else {
        ctx.sql(String.valueOf(val));
      }
    } else if (arg instanceof ConditionalExpression condExpr) {
      // $max: {$cond: [$isConversion, 1, 0]}
      renderConditionalAsValue(ctx, condExpr);
    } else {
      renderFieldAccess(ctx, arg);
    }
  }

  /**
   * Renders a ConditionalExpression as a CASE expression for accumulator arguments.
   * For $ifNull, uses NVL with proper type casting to handle JSON/scalar type mismatch.
   */
  private void renderConditionalAsValue(SqlGenerationContext ctx, ConditionalExpression condExpr) {
    // Handle $ifNull specially - use NVL with type casting
    if (condExpr.getType() == ConditionalExpression.ConditionalType.IF_NULL) {
      renderIfNullAsValue(ctx, condExpr);
      return;
    }

    ctx.sql("CASE WHEN ");
    Expression condition = condExpr.getCondition();
    // Handle various condition types for $cond inside accumulators
    if (condition instanceof FieldPathExpression fp) {
      // Boolean field path like $isConversion - use dot notation with boolean comparison
      ctx.sql("q.\"DATA\"." + fp.getDotNotationPath() + " = true");
    } else if (condition instanceof ComparisonExpression) {
      renderComparisonAsWhere(ctx, condition);
    } else if (condition instanceof InExpression inExpr) {
      // $in condition like {$in: ["$status", ["resolved", "closed"]]}
      renderInExpressionCondition(ctx, inExpr);
    } else if (condition instanceof ArrayExpression arrayExpr
        && arrayExpr.getOp() == ArrayOp.IN) {
      // $in expression form: {$in: ["$field", [values]]}
      Expression valueExpr = arrayExpr.getIndexExpression();
      Expression arrayValueExpr = arrayExpr.getArrayExpression();
      renderExpressionValue(ctx, valueExpr);
      ctx.sql(" IN (");
      if (arrayValueExpr instanceof LiteralExpression litArray
          && litArray.getValue() instanceof java.util.List<?> list) {
        boolean first = true;
        for (Object item : list) {
          if (!first) {
            ctx.sql(", ");
          }
          if (item instanceof String) {
            ctx.sql("'" + ((String) item).replace("'", "''") + "'");
          } else {
            ctx.sql(String.valueOf(item));
          }
          first = false;
        }
      }
      ctx.sql(")");
    } else {
      // Fallback - use general condition expression rendering
      renderConditionExpression(ctx, condition);
    }
    ctx.sql(" THEN ");
    renderNumericAccumulatorArg(ctx, condExpr.getThenExpr());
    ctx.sql(" ELSE ");
    renderNumericAccumulatorArg(ctx, condExpr.getElseExpr());
    ctx.sql(" END");
  }

  /**
   * Renders an $ifNull expression as NVL for accumulator arguments.
   * Handles type compatibility between JSON fields and scalar defaults.
   * For $ifNull: ["$field", default], the thenExpr is the field, elseExpr is the default.
   */
  private void renderIfNullAsValue(SqlGenerationContext ctx, ConditionalExpression condExpr) {
    Expression fieldExpr = condExpr.getThenExpr();
    Expression defaultExpr = condExpr.getElseExpr();

    ctx.sql("NVL(");

    // For type compatibility in NVL, cast JSON field to match the default's type
    if (defaultExpr instanceof LiteralExpression lit) {
      Object defaultVal = lit.getValue();
      if (defaultVal instanceof Number && fieldExpr instanceof FieldPathExpression fp) {
        // Cast JSON field to NUMBER to match numeric default
        ctx.sql("CAST(q.\"DATA\"." + quoteDotNotationPath(fp.getPath()) + " AS NUMBER)");
      } else if (defaultVal instanceof String && fieldExpr instanceof FieldPathExpression fp) {
        // Cast JSON field to VARCHAR2 to match string default
        ctx.sql("CAST(q.\"DATA\"." + quoteDotNotationPath(fp.getPath()) + " AS VARCHAR2(4000))");
      } else {
        // Other cases - render field directly
        renderNumericAccumulatorArg(ctx, fieldExpr);
      }
    } else {
      // Non-literal default - render field directly
      renderNumericAccumulatorArg(ctx, fieldExpr);
    }

    ctx.sql(", ");
    renderNumericAccumulatorArg(ctx, defaultExpr);
    ctx.sql(")");
  }

  /**
   * Renders an InExpression as a SQL condition.
   */
  private void renderInExpressionCondition(SqlGenerationContext ctx, InExpression inExpr) {
    Expression field = inExpr.getField();
    if (field instanceof FieldPathExpression fp) {
      // Use dot notation with table alias for type-preserving field access
      ctx.sql("q.\"DATA\"." + fp.getDotNotationPath());
    } else {
      renderExpressionValue(ctx, field);
    }
    if (inExpr.isNegated()) {
      ctx.sql(" NOT IN (");
    } else {
      ctx.sql(" IN (");
    }
    List<?> values = inExpr.getValues();
    for (int i = 0; i < values.size(); i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      Object val = values.get(i);
      if (val instanceof String) {
        ctx.sql("'" + ((String) val).replace("'", "''") + "'");
      } else {
        ctx.sql(String.valueOf(val));
      }
    }
    ctx.sql(")");
  }

  /**
   * Renders the GROUP BY clause for a $group stage.
   * Handles compound _id by rendering each field as a separate GROUP BY column.
   */
  private void renderGroupByClause(SqlGenerationContext ctx, GroupStage groupStage) {
    Expression idExpr = groupStage.getIdExpression();
    if (idExpr != null) {
      ctx.sql(" GROUP BY ");
      if (idExpr instanceof CompoundIdExpression compoundId) {
        // Render each field expression separately for GROUP BY
        // Use same type coercion as SELECT to ensure expressions match exactly
        boolean first = true;
        for (Map.Entry<String, Expression> entry : compoundId.getFields().entrySet()) {
          if (!first) {
            ctx.sql(", ");
          }
          renderFieldAccessForGroupBy(ctx, entry.getValue());
          first = false;
        }
      } else {
        renderFieldAccess(ctx, idExpr);
      }
    }
  }

  /**
   * Renders a field access for GROUP BY clause.
   * Uses JSON_QUERY to match the SELECT expressions in compound _id.
   * Oracle requires GROUP BY expressions to match SELECT expressions exactly.
   */
  private void renderFieldAccessForGroupBy(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fieldPath) {
      String path = fieldPath.getPath();
      // Use same expression as renderFieldAccessWithFormatJson for GROUP BY matching
      ctx.sql("JSON_QUERY(\"DATA\", '$." + path + "')");
    } else if (expr instanceof DateExpression dateExpr) {
      // Date expressions return numbers
      renderDateExpressionCte(ctx, dateExpr);
    } else if (expr instanceof ArithmeticExpression arith) {
      // Arithmetic expressions return numbers
      renderArithmeticExpression(ctx, arith);
    } else {
      // Fallback to regular rendering
      renderFieldAccess(ctx, expr);
    }
  }

  /**
   * Renders the condition for a $redact stage.
   * Handles $cond expressions with $$KEEP and $$PRUNE.
   */
  private void renderRedactCondition(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof ConditionalExpression condExpr) {
      Expression thenExpr = condExpr.getThenExpr();
      Expression elseExpr = condExpr.getElseExpr();
      Expression condition = condExpr.getCondition();

      // Check if then/else are $$KEEP or $$PRUNE
      String thenVal = getLiteralValue(thenExpr);
      String elseVal = getLiteralValue(elseExpr);

      // KEEP and DESCEND both mean "include document" at document level
      boolean keepOrDescendThen = "$$KEEP".equals(thenVal) || "$$DESCEND".equals(thenVal);
      boolean keepOrDescendElse = "$$KEEP".equals(elseVal) || "$$DESCEND".equals(elseVal);

      if (keepOrDescendThen && "$$PRUNE".equals(elseVal)) {
        // KEEP/DESCEND if condition, PRUNE otherwise → WHERE condition is true
        renderComparisonAsWhere(ctx, condition);
      } else if ("$$PRUNE".equals(thenVal) && keepOrDescendElse) {
        // PRUNE if condition, KEEP/DESCEND otherwise → WHERE condition is false
        ctx.sql("NOT (");
        renderComparisonAsWhere(ctx, condition);
        ctx.sql(")");
      } else {
        // Fallback: pass all documents
        ctx.sql("1=1");
      }
    } else {
      // Unsupported expression, pass all
      ctx.sql("1=1");
    }
  }

  /**
   * Extracts a literal String value from an expression.
   */
  private String getLiteralValue(Expression expr) {
    if (expr instanceof LiteralExpression lit) {
      Object val = lit.getValue();
      return val instanceof String ? (String) val : null;
    }
    return null;
  }

  /**
   * Renders a comparison expression as WHERE condition.
   * Handles null comparisons specially using IS NULL / IS NOT NULL.
   */
  private void renderComparisonAsWhere(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof ComparisonExpression comp) {
      Expression left = comp.getLeft();
      Expression right = comp.getRight();

      // Handle null comparisons specially - SQL requires IS NULL / IS NOT NULL
      if (right instanceof LiteralExpression lit && lit.getValue() == null) {
        renderFieldAccessForWhere(ctx, left);
        if (comp.getOp() == ComparisonOp.EQ) {
          ctx.sql(" IS NULL");
        } else {
          // NE and other comparisons with null use IS NOT NULL
          ctx.sql(" IS NOT NULL");
        }
        return;
      }

      String op = switch (comp.getOp()) {
        case EQ -> "=";
        case NE -> "!=";
        case GT -> ">";
        case GTE -> ">=";
        case LT -> "<";
        case LTE -> "<=";
        default -> "=";
      };
      renderFieldAccessForWhere(ctx, left);
      ctx.sql(" " + op + " ");
      if (right instanceof LiteralExpression lit) {
        Object val = lit.getValue();
        if (val instanceof Number) {
          ctx.sql(val.toString());
        } else if (val instanceof String) {
          ctx.sql("'" + val + "'");
        } else {
          ctx.sql(String.valueOf(val));
        }
      } else {
        renderFieldAccessForWhere(ctx, right);
      }
    } else {
      ctx.sql("1=1");
    }
  }

  /**
   * Renders the ORDER BY clause for a $sort stage.
   * Uses a two-key pattern to handle both numeric and string sorting correctly:
   * 1. TO_NUMBER(field DEFAULT NULL ON CONVERSION ERROR) for numeric sort
   * 2. Dot notation field as fallback for string/date sort
   * This ensures numbers sort numerically and strings sort lexicographically.
   */
  private void renderOrderByClause(SqlGenerationContext ctx, SortStage sortStage) {
    var sortFields = sortStage.getSortFields();
    if (!sortFields.isEmpty()) {
      ctx.sql(" ORDER BY ");
      boolean first = true;
      for (var sortField : sortFields) {
        if (!first) {
          ctx.sql(", ");
        }
        String dir = sortField.getDirection() == SortStage.SortDirection.ASC ? "ASC" : "DESC";
        FieldPathExpression fieldPath = sortField.getFieldPath();

        String dotPath = "q.\"DATA\"." + quoteDotNotationPath(fieldPath.getPath());
        // Two-key pattern: numeric sort first, then string fallback
        // This handles computed numeric fields (from $sum, $avg, etc.) correctly
        ctx.sql("TO_NUMBER(" + dotPath + " DEFAULT NULL ON CONVERSION ERROR) ");
        ctx.sql(dir);
        ctx.sql(" NULLS LAST, ");
        ctx.sql(dotPath + " ");
        ctx.sql(dir);
        ctx.sql(" NULLS LAST");
        first = false;
      }
    }
  }

  /**
   * Renders a json_transform projection with KEEP and SET operations.
   * - Simple inclusions (field: 1 or field: true) use KEEP
   * - Computed fields (field: expression) use SET
   */
  private void renderJsonTransformProjection(SqlGenerationContext ctx, ProjectStage projectStage) {
    var projections = projectStage.getProjections();

    // Separate into KEEP fields (simple inclusions) and SET fields (computed)
    var keepFields = new java.util.ArrayList<String>();
    var setFields = new java.util.LinkedHashMap<String, Expression>();

    for (var entry : projections.entrySet()) {
      String fieldName = entry.getKey();
      ProjectStage.ProjectionField projection = entry.getValue();

      if (projection.isExcluded()) {
        continue; // Skip excluded fields
      }

      Expression expr = projection.getExpression();
      // Check if this is a simple inclusion
      if (isSimpleInclusion(fieldName, expr)) {
        keepFields.add(fieldName);
      } else if (expr != null) {
        // Computed field: field: expression
        setFields.put(fieldName, expr);
      } else {
        // Fallback for null expression - treat as inclusion
        keepFields.add(fieldName);
      }
    }

    ctx.sql("json_transform(\"DATA\"");

    // Render SET fields first (they add/modify fields)
    // Set context flag so expressions can use PATH syntax for type preservation
    inJsonTransformSet = true;
    try {
      for (var entry : setFields.entrySet()) {
        String fieldName = entry.getKey();
        Expression expr = entry.getValue();
        ctx.sql(", SET '$.\"" + fieldName + "\"' = ");
        renderExpressionValue(ctx, expr);
      }
    } finally {
      inJsonTransformSet = false;
    }

    // Render KEEP clause at the end to specify which fields to keep
    // This removes any fields not in the keep list (including original fields before SET)
    // Combine keepFields (simple inclusions) with setFields keys (computed fields)
    var allFieldsToKeep = new java.util.ArrayList<>(keepFields);
    allFieldsToKeep.addAll(setFields.keySet());

    if (!allFieldsToKeep.isEmpty()) {
      ctx.sql(", KEEP ");
      boolean first = true;
      for (String fieldName : allFieldsToKeep) {
        if (!first) {
          ctx.sql(", ");
        }
        ctx.sql("'$.\"" + fieldName + "\"'");
        first = false;
      }
    }

    ctx.sql(")");
  }

  /**
   * Renders a json_transform with SET operations for $addFields.
   */
  private void renderJsonTransformAddFields(
      SqlGenerationContext ctx, AddFieldsStage addFieldsStage) {
    ctx.sql("json_transform(\"DATA\"");

    // Set context flag so expressions can use PATH syntax for type preservation
    inJsonTransformSet = true;
    try {
      for (var entry : addFieldsStage.getFields().entrySet()) {
        String fieldName = entry.getKey();
        Expression expr = entry.getValue();
        ctx.sql(", SET '$.\"" + fieldName + "\"' = ");
        renderExpressionValue(ctx, expr);

        // Track fields set to numeric expressions for type preservation in compound _id
        if (isNumericExpression(expr)) {
          knownNumericFields.add(fieldName);
        }
      }
    } finally {
      inJsonTransformSet = false;
    }
    ctx.sql(")");
  }

  /**
   * Renders an expression value for json_transform SET.
   * Uses JSON_QUERY for field paths to preserve original JSON types (numbers, booleans).
   */
  private void renderExpressionValue(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fieldPath) {
      String path = fieldPath.getPath();
      int dotIndex = path.indexOf('.');

      // Check if this is a reference to a facet array field (e.g., "$data._id")
      // If so, we need to extract nested fields from each element using [*] syntax
      if (dotIndex > 0 && !activeFacetNames.isEmpty()) {
        String firstComponent = path.substring(0, dotIndex);
        if (activeFacetNames.contains(firstComponent)) {
          // This is a facet array reference like "data._id"
          // Use [*] to iterate over array elements and extract nested field
          // WITH WRAPPER returns results as a JSON array
          // Note: Cannot use RETURNING CLOB inside json_transform context
          String arrayPart = firstComponent;
          String nestedPart = path.substring(dotIndex + 1);
          ctx.sql("JSON_QUERY(\"DATA\", '$." + arrayPart + "[*]." + nestedPart
              + "' WITH WRAPPER)");
          return;
        }
      }

      // Standard field path: Use JSON_QUERY to preserve original JSON types
      ctx.sql("JSON_QUERY(\"DATA\", '$." + path + "')");
    } else if (expr instanceof DateExpression dateExpr) {
      // Custom CTE-mode rendering for DateExpression
      renderDateExpressionCte(ctx, dateExpr);
    } else if (expr instanceof ArithmeticExpression arith) {
      renderArithmeticExpression(ctx, arith);
    } else if (expr instanceof ArrayExpression arrayExpr) {
      renderArrayExpression(ctx, arrayExpr);
    } else if (expr instanceof ConditionalExpression condExpr) {
      // Custom CTE-mode rendering for ConditionalExpression
      renderConditionalExpression(ctx, condExpr);
    } else if (expr instanceof StringExpression stringExpr) {
      // Custom CTE-mode rendering for StringExpression
      renderStringExpression(ctx, stringExpr);
    } else if (expr instanceof SwitchExpression switchExpr) {
      // Custom CTE-mode rendering for SwitchExpression
      renderSwitchExpression(ctx, switchExpr);
    } else if (expr instanceof ComparisonExpression compExpr) {
      // Render comparison as boolean expression for use in conditions
      renderComparisonForValue(ctx, compExpr);
    } else if (expr instanceof TypeConversionExpression typeExpr) {
      // Render type conversion expressions ($toInt, $toString, $type, etc.)
      renderTypeConversionExpression(ctx, typeExpr);
    } else if (expr instanceof CompoundIdExpression compoundId) {
      // Render compound _id as JSON_OBJECT
      renderCompoundIdAsJsonObject(ctx, compoundId);
    } else if (expr instanceof InlineObjectExpression objExpr) {
      // Render inline object as JSON_OBJECT
      renderInlineObjectAsJsonObject(ctx, objExpr);
    } else if (expr instanceof ObjectExpression objExpr) {
      // Render object expressions like $mergeObjects
      renderObjectExpression(ctx, objExpr);
    } else if (expr instanceof LiteralExpression lit) {
      Object value = lit.getValue();
      if (value instanceof String) {
        ctx.sql("'" + value + "'");
      } else if (value instanceof java.util.List<?> list) {
        // Handle array literals - use JSON_ARRAY
        if (list.isEmpty()) {
          ctx.sql("JSON_ARRAY(RETURNING JSON)");
        } else {
          ctx.sql("JSON_ARRAY(");
          boolean first = true;
          for (Object item : list) {
            if (!first) {
              ctx.sql(", ");
            }
            if (item instanceof String s) {
              ctx.sql("'" + s.replace("'", "''") + "'");
            } else {
              ctx.sql(String.valueOf(item));
            }
            first = false;
          }
          ctx.sql(" RETURNING JSON)");
        }
      } else if (value == null) {
        ctx.sql("NULL");
      } else {
        ctx.sql(String.valueOf(value));
      }
    } else {
      ctx.sql("NULL");
    }
  }

  /**
   * Renders a TypeConversionExpression ($toInt, $toString, $type, etc.) in CTE mode.
   */
  private void renderTypeConversionExpression(
      SqlGenerationContext ctx, TypeConversionExpression typeExpr) {
    TypeConversionOp op = typeExpr.getOp();
    Expression arg = typeExpr.getArgument();

    switch (op) {
      case TO_INT, TO_LONG -> {
        // TRUNC(TO_NUMBER(JSON_VALUE(...))) for integer conversion
        ctx.sql("TRUNC(TO_NUMBER(");
        renderStringArgument(ctx, arg);
        ctx.sql("))");
      }
      case TO_DOUBLE -> {
        ctx.sql("TO_BINARY_DOUBLE(");
        renderStringArgument(ctx, arg);
        ctx.sql(")");
      }
      case TO_DECIMAL -> {
        ctx.sql("TO_NUMBER(");
        renderStringArgument(ctx, arg);
        ctx.sql(")");
      }
      case TO_STRING -> {
        // CAST ensures JSON_OBJECT produces a JSON string
        ctx.sql("CAST(TO_CHAR(");
        renderNumericAccumulatorArg(ctx, arg);
        ctx.sql(") AS VARCHAR2(4000))");
      }
      case TO_BOOL -> {
        // MongoDB toBool: null/0/false -> false, everything else -> true
        ctx.sql("CASE WHEN ");
        renderStringArgument(ctx, arg);
        ctx.sql(" IS NULL OR ");
        renderStringArgument(ctx, arg);
        ctx.sql(" IN ('0', 'false') THEN FALSE ELSE TRUE END");
      }
      case TO_DATE -> {
        ctx.sql("TO_TIMESTAMP_TZ(");
        renderStringArgument(ctx, arg);
        ctx.sql(", 'YYYY-MM-DD\"T\"HH24:MI:SS.FF3TZH:TZM')");
      }
      case TO_OBJECT_ID -> {
        // ObjectId stored as string, return as-is
        renderStringArgument(ctx, arg);
      }
      case TYPE -> {
        // Determine BSON type from JSON value characteristics
        // Use NULL ON ERROR to safely check if value is numeric
        ctx.sql("CASE ");
        ctx.sql("WHEN ");
        renderStringArgument(ctx, arg);
        ctx.sql(" IS NULL THEN 'null' ");
        ctx.sql("WHEN JSON_VALUE(\"DATA\", '$.");
        if (arg instanceof FieldPathExpression fp) {
          ctx.sql(fp.getPath());
        }
        ctx.sql("' RETURNING NUMBER NULL ON ERROR) IS NOT NULL ");
        ctx.sql("AND MOD(JSON_VALUE(\"DATA\", '$.");
        if (arg instanceof FieldPathExpression fp) {
          ctx.sql(fp.getPath());
        }
        ctx.sql("' RETURNING NUMBER NULL ON ERROR), 1) = 0 THEN 'int' ");
        ctx.sql("WHEN JSON_VALUE(\"DATA\", '$.");
        if (arg instanceof FieldPathExpression fp) {
          ctx.sql(fp.getPath());
        }
        ctx.sql("' RETURNING NUMBER NULL ON ERROR) IS NOT NULL THEN 'double' ");
        ctx.sql("WHEN ");
        renderStringArgument(ctx, arg);
        ctx.sql(" IN ('true', 'false') THEN 'bool' ");
        ctx.sql("ELSE 'string' END");
      }
      case IS_NUMBER -> {
        ctx.sql("CASE WHEN REGEXP_LIKE(");
        renderStringArgument(ctx, arg);
        ctx.sql(", '^-?[0-9]+(\\.[0-9]+)?$') THEN 1 ELSE 0 END");
      }
      case IS_STRING -> {
        ctx.sql("CASE WHEN ");
        renderStringArgument(ctx, arg);
        ctx.sql(" IS NOT NULL AND NOT REGEXP_LIKE(");
        renderStringArgument(ctx, arg);
        ctx.sql(", '^-?[0-9]+(\\.[0-9]+)?$') AND ");
        renderStringArgument(ctx, arg);
        ctx.sql(" NOT IN ('true', 'false') THEN 1 ELSE 0 END");
      }
      default -> ctx.sql("NULL");
    }
  }

  /**
   * Renders a StringExpression in CTE mode.
   * Handles $toUpper, $toLower, $concat, $substr, $trim, etc.
   * Uses renderStringArgument to ensure field paths use JSON_VALUE (not JSON_QUERY)
   * so that string functions operate on raw string values without JSON quotes.
   */
  private void renderStringExpression(SqlGenerationContext ctx, StringExpression stringExpr) {
    StringOp op = stringExpr.getOp();
    var args = stringExpr.getArguments();

    switch (op) {
      case TO_UPPER, TO_LOWER, STRLEN -> {
        // Simple function: FUNC(arg)
        ctx.sql(op.getSqlFunction());
        ctx.sql("(");
        if (!args.isEmpty()) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(")");
      }
      case CONCAT -> {
        // Concatenation: (arg1 || arg2 || ...)
        ctx.sql("(");
        for (int i = 0; i < args.size(); i++) {
          if (i > 0) {
            ctx.sql(" || ");
          }
          renderStringArgument(ctx, args.get(i));
        }
        ctx.sql(")");
      }
      case SUBSTR -> {
        // SUBSTR(str, start+1, length) - MongoDB is 0-based, Oracle is 1-based
        ctx.sql("SUBSTR(");
        if (args.size() >= 1) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(", ");
        if (args.size() >= 2) {
          ctx.sql("(");
          renderExpressionValue(ctx, args.get(1));
          ctx.sql(" + 1)");
        }
        if (args.size() >= 3) {
          ctx.sql(", ");
          renderExpressionValue(ctx, args.get(2));
        }
        ctx.sql(")");
      }
      case TRIM, LTRIM, RTRIM -> {
        // TRIM([LEADING|TRAILING|BOTH] FROM arg)
        String trimType = op == StringOp.LTRIM ? "LEADING"
            : (op == StringOp.RTRIM ? "TRAILING" : "BOTH");
        ctx.sql("TRIM(" + trimType + " FROM ");
        if (!args.isEmpty()) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(")");
      }
      case SPLIT -> {
        // $split: use REGEXP_SUBSTR with CONNECT BY to generate JSON array
        if (args.size() >= 2) {
          ctx.sql("(SELECT JSON_ARRAYAGG(REGEXP_SUBSTR(");
          renderStringArgument(ctx, args.get(0));
          ctx.sql(", '[^' || ");
          renderStringArgument(ctx, args.get(1));
          ctx.sql(" || ']+', 1, LEVEL)) FROM DUAL CONNECT BY REGEXP_SUBSTR(");
          renderStringArgument(ctx, args.get(0));
          ctx.sql(", '[^' || ");
          renderStringArgument(ctx, args.get(1));
          ctx.sql(" || ']+', 1, LEVEL) IS NOT NULL)");
        } else {
          ctx.sql("NULL");
        }
      }
      case INDEX_OF_CP -> {
        // $indexOfCP: INSTR returns 1-based, MongoDB is 0-based, -1 if not found
        // Oracle INSTR returns 0 if not found, so: NVL(NULLIF(INSTR(...), 0), 0) - 1
        ctx.sql("(NVL(NULLIF(INSTR(");
        if (args.size() >= 1) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(", ");
        if (args.size() >= 2) {
          renderStringArgument(ctx, args.get(1));
        }
        ctx.sql("), 0), 0) - 1)");
      }
      case REGEX_MATCH -> {
        // $regexMatch: returns true/false - use CASE with REGEXP_LIKE
        ctx.sql("CASE WHEN REGEXP_LIKE(");
        if (args.size() >= 1) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(", ");
        if (args.size() >= 2) {
          renderStringArgument(ctx, args.get(1));
        }
        ctx.sql(") THEN TRUE ELSE FALSE END");
      }
      case REPLACE_ONE -> {
        // $replaceOne: replace first occurrence only
        ctx.sql("REGEXP_REPLACE(");
        if (args.size() >= 1) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(", ");
        if (args.size() >= 2) {
          renderStringArgument(ctx, args.get(1));
        }
        ctx.sql(", ");
        if (args.size() >= 3) {
          renderStringArgument(ctx, args.get(2));
        }
        ctx.sql(", 1, 1)"); // position 1, occurrence 1 (first only)
      }
      case REPLACE_ALL -> {
        // $replaceAll: replace all occurrences
        ctx.sql("REPLACE(");
        if (args.size() >= 1) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(", ");
        if (args.size() >= 2) {
          renderStringArgument(ctx, args.get(1));
        }
        ctx.sql(", ");
        if (args.size() >= 3) {
          renderStringArgument(ctx, args.get(2));
        } else {
          ctx.sql("''");
        }
        ctx.sql(")");
      }
      case STRCASECMP -> {
        // $strcasecmp: compare strings case-insensitively, return -1, 0, or 1
        ctx.sql("CASE WHEN UPPER(");
        if (args.size() >= 1) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(") < UPPER(");
        if (args.size() >= 2) {
          renderStringArgument(ctx, args.get(1));
        }
        ctx.sql(") THEN -1 WHEN UPPER(");
        if (args.size() >= 1) {
          renderStringArgument(ctx, args.get(0));
        }
        ctx.sql(") > UPPER(");
        if (args.size() >= 2) {
          renderStringArgument(ctx, args.get(1));
        }
        ctx.sql(") THEN 1 ELSE 0 END");
      }
      default -> {
        // For other string operations, delegate to default render (may not work)
        ctx.sql("NULL");
      }
    }
  }

  /**
   * Renders an argument for string operations.
   * Uses JSON_VALUE for field paths to get raw string values without JSON quotes.
   * This is different from renderExpressionValue which uses JSON_QUERY to preserve types.
   */
  private void renderStringArgument(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fieldPath) {
      // Use JSON_VALUE with table alias to get raw string value without quotes
      ctx.sql("JSON_VALUE(q.\"DATA\", '$." + fieldPath.getPath() + "')");
    } else if (expr instanceof LiteralExpression lit) {
      Object value = lit.getValue();
      if (value instanceof String) {
        ctx.sql("'" + value.toString().replace("'", "''") + "'");
      } else {
        ctx.sql(String.valueOf(value));
      }
    } else if (expr instanceof StringExpression stringExpr) {
      // Nested string expression
      renderStringExpression(ctx, stringExpr);
    } else {
      // For other expressions, fall back to renderExpressionValue
      renderExpressionValue(ctx, expr);
    }
  }

  /**
   * Renders $arrayElemAt with RETURNING NUMBER for use in $ifNull with number default.
   */
  private void renderArrayElemAtForNumber(SqlGenerationContext ctx, ArrayExpression arrayExpr) {
    Expression arrayInput = arrayExpr.getArrayExpression();
    Expression indexExpr = arrayExpr.getIndexExpression();

    if (arrayInput instanceof FieldPathExpression fieldPath) {
      String path = fieldPath.getPath();
      String index = "0";
      if (indexExpr instanceof LiteralExpression lit) {
        Object val = lit.getValue();
        if (val instanceof Number) {
          index = String.valueOf(((Number) val).intValue());
        }
      }
      ctx.sql("JSON_VALUE(\"DATA\", '$." + path + "[" + index + "]' RETURNING NUMBER)");
    } else {
      // Fall back to general rendering
      ctx.sql("NULL");
    }
  }

  /**
   * Renders an array expression for json_transform SET.
   */
  private void renderArrayExpression(SqlGenerationContext ctx, ArrayExpression arrayExpr) {
    ArrayOp op = arrayExpr.getOp();
    Expression arrayInput = arrayExpr.getArrayExpression();
    Expression indexExpr = arrayExpr.getIndexExpression();

    if (op == ArrayOp.SIZE) {
      // Always use JSON_VALUE with RETURNING NUMBER for $size
      // PATH syntax only works directly in json_transform SET, not inside nested JSON_OBJECT
      if (arrayInput instanceof FieldPathExpression fieldPath) {
        ctx.sql("JSON_VALUE(\"DATA\", '$." + fieldPath.getPath() + ".size()' RETURNING NUMBER)");
      } else {
        // $size on an expression - delegate to expression render
        ctx.sql("JSON_VALUE(");
        renderExpressionValue(ctx, arrayInput);
        ctx.sql(", '$.size()' RETURNING NUMBER)");
      }
    } else if (op == ArrayOp.ARRAY_ELEM_AT) {
      // $arrayElemAt: use JSON_QUERY for object/array access, JSON_VALUE for scalars
      // Since we don't know the type at compile time, use JSON_QUERY which handles both
      if (arrayInput instanceof FieldPathExpression fieldPath) {
        String path = fieldPath.getPath();
        String index = "0"; // Default to first element
        if (indexExpr instanceof LiteralExpression lit) {
          Object val = lit.getValue();
          if (val instanceof Number) {
            index = String.valueOf(((Number) val).intValue());
          }
        }
        ctx.sql("JSON_QUERY(\"DATA\", '$." + path + "[" + index + "]')");
      } else if (arrayInput instanceof StringExpression strExpr
          && strExpr.getOp() == StringOp.SPLIT) {
        // $arrayElemAt on $split: use REGEXP_SUBSTR to get nth element
        var args = strExpr.getArguments();
        int index = 1; // REGEXP_SUBSTR is 1-based
        if (indexExpr instanceof LiteralExpression lit) {
          Object val = lit.getValue();
          if (val instanceof Number) {
            index = ((Number) val).intValue() + 1; // Convert 0-based to 1-based
          }
        }
        if (args.size() >= 2) {
          ctx.sql("REGEXP_SUBSTR(");
          renderExpressionValue(ctx, args.get(0));
          ctx.sql(", '[^' || ");
          renderExpressionValue(ctx, args.get(1));
          ctx.sql(" || ']+', 1, " + index + ")");
        } else {
          ctx.sql("NULL");
        }
      } else {
        // Complex array expression - fall back to standard render
        ctx.visit(arrayExpr);
      }
    } else {
      // For other array operations, delegate to the standard render
      ctx.visit(arrayExpr);
    }
  }

  /**
   * Renders an arithmetic expression for json_transform.
   * Handles date subtraction specially for proper interval-to-milliseconds conversion.
   */
  private void renderArithmeticExpression(
      SqlGenerationContext ctx, ArithmeticExpression arith) {
    var operands = arith.getOperands();
    var op = arith.getOp();

    if (op.requiresFunctionCall()) {
      // Function-style: FUNC(arg1, arg2, ...)
      ctx.sql(op.getSqlOperator());
      ctx.sql("(");
      boolean first = true;
      for (Expression operand : operands) {
        if (!first) {
          ctx.sql(", ");
        }
        renderNumericOperand(ctx, operand);
        first = false;
      }
      ctx.sql(")");
    } else if (op == ArithmeticOp.SUBTRACT && isPotentialDateSubtraction(operands)) {
      // Date subtraction: convert to milliseconds using EXTRACT
      renderDateSubtraction(ctx, operands);
    } else {
      // Infix-style: (arg1 OP arg2 OP ...)
      ctx.sql("(");
      boolean first = true;
      for (Expression operand : operands) {
        if (!first) {
          ctx.sql(" " + op.getSqlOperator() + " ");
        }
        renderNumericOperand(ctx, operand);
        first = false;
      }
      ctx.sql(")");
    }
  }

  /**
   * Renders an expression as a numeric value for arithmetic operations.
   * Uses JSON_VALUE with RETURNING NUMBER for field paths.
   */
  private void renderNumericOperand(SqlGenerationContext ctx, Expression expr) {
    if (expr instanceof FieldPathExpression fieldPath) {
      // Use JSON_VALUE RETURNING NUMBER for numeric operations
      ctx.sql("JSON_VALUE(\"DATA\", '$." + fieldPath.getPath() + "' RETURNING NUMBER)");
    } else if (expr instanceof ArithmeticExpression arith) {
      renderArithmeticExpression(ctx, arith);
    } else if (expr instanceof DateExpression dateExpr) {
      // Date expressions like $month, $year return numeric values
      renderDateExpressionCte(ctx, dateExpr);
    } else if (expr instanceof LiteralExpression lit) {
      Object value = lit.getValue();
      if (value instanceof Number) {
        ctx.sql(String.valueOf(value));
      } else {
        ctx.sql("NULL");
      }
    } else if (expr instanceof ConditionalExpression condExpr) {
      // Handle $ifNull and $cond in numeric context
      renderConditionalExpressionNumeric(ctx, condExpr);
    } else if (expr instanceof ArrayExpression arrayExpr) {
      // Handle $size in numeric context
      renderArrayExpressionNumeric(ctx, arrayExpr);
    } else if (expr instanceof AccumulatorExpression accumExpr) {
      // Handle expression-level $sum (array sum, not group accumulator)
      renderExpressionLevelAccumulator(ctx, accumExpr);
    } else {
      ctx.sql("NULL");
    }
  }

  /**
   * Renders a conditional expression in numeric context.
   * Handles $ifNull with NVL and $cond with CASE.
   */
  private void renderConditionalExpressionNumeric(
      SqlGenerationContext ctx, ConditionalExpression condExpr) {
    if (condExpr.getType() == ConditionalExpression.ConditionalType.IF_NULL) {
      // $ifNull: NVL(expr, default)
      Expression valueExpr = condExpr.getThenExpr();
      final Expression defaultExpr = condExpr.getElseExpr();

      ctx.sql("NVL(");
      renderNumericOperand(ctx, valueExpr);
      ctx.sql(", ");
      renderNumericOperand(ctx, defaultExpr);
      ctx.sql(")");
    } else {
      // $cond: CASE WHEN condition THEN thenExpr ELSE elseExpr END
      ctx.sql("CASE WHEN ");
      renderConditionExpression(ctx, condExpr.getCondition());
      ctx.sql(" THEN ");
      renderNumericOperand(ctx, condExpr.getThenExpr());
      ctx.sql(" ELSE ");
      renderNumericOperand(ctx, condExpr.getElseExpr());
      ctx.sql(" END");
    }
  }

  /**
   * Renders an ArrayExpression in numeric context.
   * Handles $size, $sum (SUM_ARRAY), and $avg (AVG_ARRAY) to produce numeric values.
   */
  private void renderArrayExpressionNumeric(SqlGenerationContext ctx, ArrayExpression arrayExpr) {
    ArrayOp op = arrayExpr.getOp();
    Expression arrayArg = arrayExpr.getArrayExpression();

    if (op == ArrayOp.SIZE) {
      if (arrayArg instanceof FieldPathExpression fp) {
        // $size on field path: JSON_VALUE("DATA", '$.field.size()' RETURNING NUMBER)
        ctx.sql("JSON_VALUE(\"DATA\", '$." + fp.getPath() + ".size()' RETURNING NUMBER)");
      } else {
        // Nested expression - try to render and get size
        ctx.sql("0");
      }
    } else if (op == ArrayOp.SUM_ARRAY) {
      // Expression-level $sum: sum values from an array field
      // MongoDB: {$sum: "$orders.amount"} -> sum all amount values from orders array
      renderArrayAggregateSubquery(ctx, arrayArg, "SUM");
    } else if (op == ArrayOp.AVG_ARRAY) {
      // Expression-level $avg: average values from an array field
      renderArrayAggregateSubquery(ctx, arrayArg, "AVG");
    } else {
      // For other array ops, fall back to NULL (not supported in numeric context)
      ctx.sql("NULL");
    }
  }

  /**
   * Renders a subquery to aggregate (SUM/AVG) values from an array field.
   * Handles both simple arrays (e.g., "$scores") and dotted paths (e.g., "$orders.amount").
   */
  private void renderArrayAggregateSubquery(
      SqlGenerationContext ctx, Expression arrayArg, String aggFunc) {
    if (arrayArg instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      // Check if this is a dotted path (e.g., "orders.amount")
      int dotIndex = path.indexOf('.');
      if (dotIndex > 0) {
        // Dotted path: extract array part and field part
        String arrayPart = path.substring(0, dotIndex);
        String fieldPart = path.substring(dotIndex + 1);
        // Use subquery with JSON_TABLE to aggregate array elements
        ctx.sql("(SELECT NVL(" + aggFunc + "(TO_NUMBER(val)), 0) FROM JSON_TABLE(\"DATA\", '$.");
        ctx.sql(arrayPart);
        ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$.");
        ctx.sql(fieldPart);
        ctx.sql("')))");
      } else {
        // Simple path: aggregate array of numbers directly
        ctx.sql("(SELECT NVL(" + aggFunc + "(TO_NUMBER(val)), 0) FROM JSON_TABLE(\"DATA\", '$.");
        ctx.sql(path);
        ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
      }
    } else if (arrayArg instanceof LiteralExpression lit && lit.getValue() instanceof Number n) {
      // $sum: 1 or similar literal - just return the literal
      ctx.sql(String.valueOf(n));
    } else {
      ctx.sql("0");
    }
  }

  /**
   * Renders an AccumulatorExpression in expression context (not group context).
   * In MongoDB, $sum can be used as an expression to sum array elements.
   * For example: {$sum: "$orders.amount"} sums all amount values from the orders array.
   */
  private void renderExpressionLevelAccumulator(
      SqlGenerationContext ctx, AccumulatorExpression accumExpr) {
    AccumulatorOp op = accumExpr.getOp();
    Expression arg = accumExpr.getArgument();

    if (op == AccumulatorOp.SUM) {
      // Expression-level $sum: sum values from an array field
      // MongoDB: {$sum: "$orders.amount"} -> sum all amount values from orders array
      if (arg instanceof FieldPathExpression fp) {
        String path = fp.getPath();
        // Check if this is a dotted path (e.g., "orders.amount")
        int dotIndex = path.indexOf('.');
        if (dotIndex > 0) {
          // Dotted path: extract array part and field part
          String arrayPart = path.substring(0, dotIndex);
          String fieldPart = path.substring(dotIndex + 1);
          // Use subquery with JSON_TABLE to sum array elements
          ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), 0) FROM JSON_TABLE(\"DATA\", '$.");
          ctx.sql(arrayPart);
          ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$.");
          ctx.sql(fieldPart);
          ctx.sql("')))");
        } else {
          // Simple path: sum array of numbers directly
          ctx.sql("(SELECT NVL(SUM(TO_NUMBER(val)), 0) FROM JSON_TABLE(\"DATA\", '$.");
          ctx.sql(path);
          ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
        }
      } else if (arg instanceof LiteralExpression lit && lit.getValue() instanceof Number n) {
        // $sum: 1 as expression (not in group) - just return the literal
        ctx.sql(String.valueOf(n));
      } else {
        ctx.sql("0");
      }
    } else if (op == AccumulatorOp.AVG) {
      // Expression-level $avg: average values from an array field
      if (arg instanceof FieldPathExpression fp) {
        String path = fp.getPath();
        int dotIndex = path.indexOf('.');
        if (dotIndex > 0) {
          String arrayPart = path.substring(0, dotIndex);
          String fieldPart = path.substring(dotIndex + 1);
          ctx.sql("(SELECT AVG(TO_NUMBER(val)) FROM JSON_TABLE(\"DATA\", '$.");
          ctx.sql(arrayPart);
          ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$.");
          ctx.sql(fieldPart);
          ctx.sql("')))");
        } else {
          ctx.sql("(SELECT AVG(TO_NUMBER(val)) FROM JSON_TABLE(\"DATA\", '$.");
          ctx.sql(path);
          ctx.sql("[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))");
        }
      } else {
        ctx.sql("NULL");
      }
    } else {
      // Other accumulators not supported as expressions
      ctx.sql("NULL");
    }
  }

  /**
   * Checks if this subtraction might involve dates (two field path expressions).
   */
  private boolean isPotentialDateSubtraction(List<Expression> operands) {
    if (operands.size() != 2) {
      return false;
    }
    // Two field paths that could be dates
    return operands.get(0) instanceof FieldPathExpression
        && operands.get(1) instanceof FieldPathExpression;
  }

  /**
   * Renders date subtraction with EXTRACT for interval-to-milliseconds conversion.
   * MongoDB $subtract with dates returns milliseconds.
   */
  private void renderDateSubtraction(SqlGenerationContext ctx, List<Expression> operands) {
    FieldPathExpression field1 = (FieldPathExpression) operands.get(0);
    FieldPathExpression field2 = (FieldPathExpression) operands.get(1);
    String path1 = field1.getPath();
    String path2 = field2.getPath();

    // Runtime detection: CASE WHEN numeric THEN num_sub ELSE date_sub END
    ctx.sql("CASE WHEN ");
    ctx.sql("JSON_VALUE(\"DATA\", '$." + path1 + "' RETURNING NUMBER NULL ON ERROR) IS NOT NULL");
    ctx.sql(" AND ");
    ctx.sql("JSON_VALUE(\"DATA\", '$." + path2 + "' RETURNING NUMBER NULL ON ERROR) IS NOT NULL");
    ctx.sql(" THEN (");
    ctx.sql("JSON_VALUE(\"DATA\", '$." + path1 + "' RETURNING NUMBER)");
    ctx.sql(" - ");
    ctx.sql("JSON_VALUE(\"DATA\", '$." + path2 + "' RETURNING NUMBER)");
    ctx.sql(") ELSE ");
    // Date subtraction - use EXTRACT to convert interval to milliseconds
    renderDateSubtractionInterval(ctx, path1, path2);
    ctx.sql(" END");
  }

  /**
   * Renders date subtraction with EXTRACT components to get milliseconds.
   */
  private void renderDateSubtractionInterval(SqlGenerationContext ctx, String path1, String path2) {
    // TIMESTAMP - TIMESTAMP yields INTERVAL
    // Extract components: DAY * 86400000 + HOUR * 3600000 + MINUTE * 60000 + SECOND * 1000
    String ts1 = "JSON_VALUE(\"DATA\", '$." + path1 + "' RETURNING TIMESTAMP)";
    String ts2 = "JSON_VALUE(\"DATA\", '$." + path2 + "' RETURNING TIMESTAMP)";

    ctx.sql("(EXTRACT(DAY FROM (" + ts1 + " - " + ts2 + ")) * 86400000");
    ctx.sql(" + EXTRACT(HOUR FROM (" + ts1 + " - " + ts2 + ")) * 3600000");
    ctx.sql(" + EXTRACT(MINUTE FROM (" + ts1 + " - " + ts2 + ")) * 60000");
    ctx.sql(" + EXTRACT(SECOND FROM (" + ts1 + " - " + ts2 + ")) * 1000)");
  }

  /**
   * Renders a CASE expression for $bucket stage.
   * Handles mixed types (numeric boundaries with string default) by casting to strings.
   */
  private void renderBucketCaseExpression(SqlGenerationContext ctx, BucketStage bucketStage) {
    boolean needsStringCast = bucketHasMixedTypes(bucketStage);

    ctx.sql("CASE");
    List<Object> boundaries = bucketStage.getBoundaries();
    for (int i = 0; i < boundaries.size() - 1; i++) {
      final Object lower = boundaries.get(i);
      ctx.sql(" WHEN ");
      renderFieldAccess(ctx, bucketStage.getGroupBy());
      ctx.sql(" >= ");
      renderBucketLiteral(ctx, lower, false);
      ctx.sql(" AND ");
      renderFieldAccess(ctx, bucketStage.getGroupBy());
      ctx.sql(" < ");
      renderBucketLiteral(ctx, boundaries.get(i + 1), false);
      ctx.sql(" THEN ");
      renderBucketLiteral(ctx, lower, needsStringCast);
    }
    if (bucketStage.hasDefault()) {
      ctx.sql(" ELSE ");
      renderBucketLiteral(ctx, bucketStage.getDefaultBucket(), false);
    }
    ctx.sql(" END");
  }

  /**
   * Checks if bucket stage has mixed types (numeric boundaries with string default).
   */
  private boolean bucketHasMixedTypes(BucketStage bucket) {
    if (!bucket.hasDefault() || bucket.getBoundaries().isEmpty()) {
      return false;
    }
    boolean boundariesAreNumeric = bucket.getBoundaries().get(0) instanceof Number;
    boolean defaultIsNumeric = bucket.getDefaultBucket() instanceof Number;
    return boundariesAreNumeric != defaultIsNumeric;
  }

  /**
   * Renders a bucket literal value, optionally casting to string for type compatibility.
   */
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

  /**
   * Renders a window function for CTE mode.
   */
  private void renderWindowFunctionCte(
      SqlGenerationContext ctx,
      SetWindowFieldsStage windowStage,
      SetWindowFieldsStage.WindowField field) {
    String op = field.operator();

    // Map MongoDB window operators to Oracle
    switch (op) {
      case "$sum" -> {
        ctx.sql("SUM(JSON_VALUE(q.\"DATA\", '$.");
        ctx.sql(cleanFieldPath(field.argument()));
        ctx.sql("' RETURNING NUMBER))");
      }
      case "$avg" -> {
        ctx.sql("AVG(JSON_VALUE(q.\"DATA\", '$.");
        ctx.sql(cleanFieldPath(field.argument()));
        ctx.sql("' RETURNING NUMBER))");
      }
      case "$min" -> {
        ctx.sql("MIN(JSON_VALUE(q.\"DATA\", '$.");
        ctx.sql(cleanFieldPath(field.argument()));
        ctx.sql("'))");
      }
      case "$max" -> {
        ctx.sql("MAX(JSON_VALUE(q.\"DATA\", '$.");
        ctx.sql(cleanFieldPath(field.argument()));
        ctx.sql("'))");
      }
      case "$count" -> ctx.sql("COUNT(*)");
      case "$rank" -> ctx.sql("RANK()");
      case "$denseRank" -> ctx.sql("DENSE_RANK()");
      case "$rowNumber", "$documentNumber" -> ctx.sql("ROW_NUMBER()");
      default -> ctx.sql("NULL /* unsupported: " + op + " */");
    }

    // Add OVER clause
    ctx.sql(" OVER (");
    renderOverClauseCte(ctx, windowStage, field.window());
    ctx.sql(")");
  }

  private void renderOverClauseCte(
      SqlGenerationContext ctx,
      SetWindowFieldsStage windowStage,
      SetWindowFieldsStage.WindowSpec window) {
    boolean hasClause = false;

    // PARTITION BY clause
    String partitionBy = windowStage.getPartitionBy();
    if (partitionBy != null) {
      ctx.sql("PARTITION BY JSON_VALUE(q.\"DATA\", '$.");
      ctx.sql(cleanFieldPath(partitionBy));
      ctx.sql("')");
      hasClause = true;
    }

    // ORDER BY clause - use two-key pattern for type-agnostic sorting
    // First key: numeric sort (NULLs for non-numbers)
    // Second key: string sort (for when first key is all NULLs, e.g., dates)
    Map<String, Integer> sortBy = windowStage.getSortBy();
    if (!sortBy.isEmpty()) {
      if (hasClause) {
        ctx.sql(" ");
      }
      ctx.sql("ORDER BY ");
      boolean firstSort = true;
      for (Map.Entry<String, Integer> sortEntry : sortBy.entrySet()) {
        if (!firstSort) {
          ctx.sql(", ");
        }
        String dir = sortEntry.getValue() < 0 ? "DESC" : "ASC";
        String fieldPath = sortEntry.getKey();
        // First key: numeric sort
        ctx.sql("JSON_VALUE(q.\"DATA\", '$." + fieldPath + "' RETURNING NUMBER NULL ON ERROR) ");
        ctx.sql(dir);
        ctx.sql(" NULLS LAST, ");
        // Second key: string sort (fallback for dates, strings)
        ctx.sql("JSON_VALUE(q.\"DATA\", '$." + fieldPath + "') ");
        ctx.sql(dir);
        ctx.sql(" NULLS LAST");
        firstSort = false;
      }
      hasClause = true;
    }

    // Window frame clause (ROWS BETWEEN ... AND ...)
    if (window != null && window.bounds() != null && window.bounds().size() >= 2) {
      if (hasClause) {
        ctx.sql(" ");
      }
      // MongoDB uses "documents" for ROWS, "range" for RANGE
      String frameType = "documents".equals(window.type()) ? "ROWS" : "RANGE";
      ctx.sql(frameType + " BETWEEN ");
      ctx.sql(renderWindowBound(window.bounds().get(0), true));  // start bound
      ctx.sql(" AND ");
      ctx.sql(renderWindowBound(window.bounds().get(1), false)); // end bound
    }
  }

  /**
   * Converts a MongoDB window bound to Oracle syntax.
   * MongoDB: "unbounded", "current", or a number
   * Oracle: UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW, or N PRECEDING/FOLLOWING
   *
   * @param bound the bound value
   * @param isStart true for start bound (uses PRECEDING), false for end bound (uses FOLLOWING)
   */
  private String renderWindowBound(String bound, boolean isStart) {
    if ("unbounded".equals(bound)) {
      return isStart ? "UNBOUNDED PRECEDING" : "UNBOUNDED FOLLOWING";
    } else if ("current".equals(bound)) {
      return "CURRENT ROW";
    } else {
      // Numeric bound - negative means preceding, positive means following
      try {
        int n = Integer.parseInt(bound);
        if (n == 0) {
          return "CURRENT ROW";
        } else if (n < 0) {
          return Math.abs(n) + " PRECEDING";
        } else {
          return n + " FOLLOWING";
        }
      } catch (NumberFormatException e) {
        return "CURRENT ROW"; // fallback
      }
    }
  }

  /**
   * Renders a facet sub-pipeline to SQL.
   * Analyzes the stages and generates appropriate SELECT/GROUP BY/ORDER BY/FETCH clauses.
   */
  private void renderFacetSubPipeline(
      SqlGenerationContext ctx, String previousCte, List<Stage> stages) {
    // Find key stages
    GroupStage groupStage = null;
    SortStage sortStage = null;
    LimitStage limitStage = null;
    SkipStage skipStage = null;
    MatchStage matchStage = null;
    CountStage countStage = null;
    ProjectStage projectStage = null;

    for (Stage stage : stages) {
      if (stage instanceof GroupStage gs) {
        groupStage = gs;
      } else if (stage instanceof SortStage ss) {
        sortStage = ss;
      } else if (stage instanceof LimitStage ls) {
        limitStage = ls;
      } else if (stage instanceof SkipStage ss) {
        skipStage = ss;
      } else if (stage instanceof MatchStage ms) {
        matchStage = ms;
      } else if (stage instanceof CountStage cs) {
        countStage = cs;
      } else if (stage instanceof ProjectStage ps) {
        projectStage = ps;
      }
    }

    if (countStage != null) {
      // COUNT facet: SELECT JSON_OBJECT('count' VALUE COUNT(*)) AS DATA
      ctx.sql("SELECT JSON_OBJECT('" + countStage.getFieldName() + "' VALUE COUNT(*)) AS \"DATA\"");
      ctx.sql(" FROM \"" + previousCte + "\" q");
      if (matchStage != null) {
        ctx.sql(" WHERE ");
        renderJsonExistsPredicate(ctx, matchStage.getFilter());
      }
    } else if (groupStage != null) {
      // GROUP facet: wrap in subquery to enable ORDER BY on JSON fields
      boolean needsSort = sortStage != null || limitStage != null;

      if (needsSort) {
        ctx.sql("SELECT \"DATA\" FROM (");
      }

      ctx.sql("SELECT ");
      renderJsonObjectGroup(ctx, groupStage);
      ctx.sql(" AS \"DATA\" FROM \"" + previousCte + "\" q");

      if (matchStage != null) {
        ctx.sql(" WHERE ");
        renderJsonExistsPredicate(ctx, matchStage.getFilter());
      }

      // GROUP BY clause
      Expression idExpr = groupStage.getIdExpression();
      if (idExpr != null) {
        ctx.sql(" GROUP BY ");
        renderFieldAccess(ctx, idExpr);
      }

      if (needsSort) {
        ctx.sql(")"); // close subquery

        // ORDER BY for $group facets - access JSON fields from subquery result
        if (sortStage != null) {
          ctx.sql(" ORDER BY ");
          renderPostGroupSortClause(ctx, sortStage, groupStage);
        }

        if (limitStage != null) {
          ctx.sql(" FETCH FIRST " + limitStage.getLimit() + " ROWS ONLY");
        }
      }
    } else if (sortStage != null || limitStage != null || skipStage != null
        || projectStage != null) {
      // SORT/SKIP/LIMIT/PROJECT facet
      // If project exists, wrap in outer SELECT with projection
      if (projectStage != null) {
        ctx.sql("SELECT ");
        renderFacetProjection(ctx, projectStage);
        ctx.sql(" AS \"DATA\" FROM (");
      }

      ctx.sql("SELECT \"DATA\" FROM \"" + previousCte + "\" q");

      if (matchStage != null) {
        ctx.sql(" WHERE ");
        renderJsonExistsPredicate(ctx, matchStage.getFilter());
      }

      if (sortStage != null) {
        ctx.sql(" ORDER BY ");
        renderSortClause(ctx, sortStage);
      }

      if (skipStage != null) {
        ctx.sql(" OFFSET " + skipStage.getSkip() + " ROWS");
      }

      if (limitStage != null) {
        ctx.sql(" FETCH FIRST " + limitStage.getLimit() + " ROWS ONLY");
      }

      if (projectStage != null) {
        ctx.sql(")");
      }
    } else {
      // Passthrough: just select all data
      ctx.sql("SELECT \"DATA\" FROM \"" + previousCte + "\" q");
      if (matchStage != null) {
        ctx.sql(" WHERE ");
        renderJsonExistsPredicate(ctx, matchStage.getFilter());
      }
    }
  }

  /**
   * Renders ORDER BY clause for sort stage in facet sub-pipelines.
   * Uses dual sort keys: numeric first (handles numbers), then string fallback.
   */
  private void renderSortClause(SqlGenerationContext ctx, SortStage sortStage) {
    boolean first = true;
    for (SortStage.SortField field : sortStage.getSortFields()) {
      if (!first) {
        ctx.sql(", ");
      }
      String path = field.getFieldPath().getPath();
      String dir = field.getDirection() == SortStage.SortDirection.DESC ? "DESC" : "ASC";

      // First sort key: numeric (NULLS LAST so non-numbers sort after numbers)
      ctx.sql("JSON_VALUE(q.\"DATA\", '$.");
      ctx.sql(path);
      ctx.sql("' RETURNING NUMBER NULL ON ERROR) ");
      ctx.sql(dir);
      ctx.sql(" NULLS LAST, ");

      // Second sort key: string fallback (for when first key is all NULLs)
      ctx.sql("JSON_VALUE(q.\"DATA\", '$.");
      ctx.sql(path);
      ctx.sql("') ");
      ctx.sql(dir);
      ctx.sql(" NULLS LAST");

      first = false;
    }
  }

  /**
   * Renders ORDER BY clause for sort after GROUP - uses JSON_VALUE on DATA column.
   * Called after wrapping GROUP BY in a subquery.
   */
  private void renderPostGroupSortClause(
      SqlGenerationContext ctx, SortStage sortStage, GroupStage groupStage) {
    boolean first = true;
    for (SortStage.SortField field : sortStage.getSortFields()) {
      if (!first) {
        ctx.sql(", ");
      }
      String sortField = field.getFieldPath().getPath();

      // Access the field from the JSON_OBJECT result via JSON_VALUE
      ctx.sql("JSON_VALUE(\"DATA\", '$.");
      ctx.sql(sortField);
      ctx.sql("')");

      if (field.getDirection() == SortStage.SortDirection.DESC) {
        ctx.sql(" DESC");
      }
      first = false;
    }
  }

  /**
   * Renders a projection for facet sub-pipeline as JSON_OBJECT.
   * Extracts specified fields from the DATA column, preserving their JSON types.
   */
  private void renderFacetProjection(SqlGenerationContext ctx, ProjectStage projectStage) {
    ctx.sql("JSON_OBJECT(");
    boolean first = true;
    for (var entry : projectStage.getProjections().entrySet()) {
      String fieldName = entry.getKey();
      ProjectStage.ProjectionField field = entry.getValue();
      if (field.isExcluded()) {
        continue;
      }
      if (!first) {
        ctx.sql(", ");
      }
      // Use JSON_QUERY to preserve the original JSON type (number, string, boolean, etc.)
      ctx.sql("'" + fieldName + "' VALUE JSON_QUERY(\"DATA\", '$." + fieldName + "')");
      first = false;
    }
    ctx.sql(")");
  }

  private String cleanFieldPath(String fieldPath) {
    if (fieldPath == null) {
      return "";
    }
    return fieldPath.startsWith("$") ? fieldPath.substring(1) : fieldPath;
  }

  /**
   * Renders a SwitchExpression ($switch) in CTE mode.
   * Produces CASE WHEN ... THEN ... ELSE ... END with proper DATA column references.
   */
  private void renderSwitchExpression(SqlGenerationContext ctx, SwitchExpression switchExpr) {
    ctx.sql("CASE");
    for (var branch : switchExpr.getBranches()) {
      ctx.sql(" WHEN ");
      renderConditionExpression(ctx, branch.caseExpr());
      ctx.sql(" THEN ");
      renderExpressionValue(ctx, branch.thenExpr());
    }
    if (switchExpr.getDefaultExpr() != null) {
      ctx.sql(" ELSE ");
      renderExpressionValue(ctx, switchExpr.getDefaultExpr());
    }
    ctx.sql(" END");
  }

  /**
   * Renders a ComparisonExpression as a boolean result for use in value contexts.
   * Used when a comparison appears as a field value
   * (e.g., needsReorder: {$lte: ["$stock", "$threshold"]}).
   * Uses JSON constructor to produce actual JSON boolean values (not strings).
   */
  private void renderComparisonForValue(SqlGenerationContext ctx, ComparisonExpression compExpr) {
    // Render as CASE WHEN comparison THEN JSON('true') ELSE JSON('false') END
    // JSON() constructor produces actual JSON boolean values, not strings
    ctx.sql("CASE WHEN ");
    renderConditionExpression(ctx, compExpr);
    ctx.sql(" THEN JSON('true') ELSE JSON('false') END");
  }

  /**
   * Checks if an expression is an array literal (empty list or LiteralExpression with List).
   */
  private boolean isArrayLiteral(Expression expr) {
    if (expr instanceof LiteralExpression lit) {
      return lit.getValue() instanceof java.util.List;
    }
    return false;
  }

  /**
   * Checks if an expression is a number literal.
   */
  private boolean isNumberLiteral(Expression expr) {
    if (expr instanceof LiteralExpression lit) {
      return lit.getValue() instanceof Number;
    }
    return false;
  }

  /**
   * Checks if a projection expression represents a simple inclusion.
   * Simple inclusions use KEEP (field: 1, field: true, or field: "$field").
   * Computed fields use SET (field: expression).
   */
  private boolean isSimpleInclusion(String fieldName, Expression expr) {
    if (expr == null) {
      return true;
    }
    if (expr instanceof LiteralExpression lit) {
      Object value = lit.getValue();
      // MongoDB uses 1 or true for inclusion
      return value.equals(1) || value.equals(true) || Integer.valueOf(1).equals(value);
    }
    if (expr instanceof FieldPathExpression fp) {
      String path = fp.getPath();
      // Self-reference: {field: "$field"} is a simple inclusion
      return path.equals(fieldName) || path.equals("$" + fieldName);
    }
    return false;
  }

  /**
   * Checks if a GroupStage contains any $first or $last accumulators.
   * Used to determine if we need to combine with preceding sort for KEEP clause.
   */
  private boolean hasFirstOrLastAccumulator(GroupStage groupStage) {
    for (AccumulatorExpression accum : groupStage.getAccumulators().values()) {
      AccumulatorOp op = accum.getOp();
      if (op == AccumulatorOp.FIRST || op == AccumulatorOp.LAST) {
        return true;
      }
    }
    return false;
  }

  /** Internal representation of a CTE definition. */
  private record CteDefinition(String name, CteType type, Object payload) {
    /** Gets the stage (for regular CTE types). */
    Stage stage() {
      return (Stage) payload;
    }

    /** Gets the combined sort/pagination stages (for SORT_PAGINATION type). */
    SortPaginationStages sortPagination() {
      return (SortPaginationStages) payload;
    }

    /** Gets the combined sort/group stages (for SORT_GROUP type). */
    SortGroupStages sortGroup() {
      return (SortGroupStages) payload;
    }

    /** Gets the recursive graphLookup context (for GRAPHLOOKUP_* types). */
    RecursiveGraphLookupContext recursiveGraphLookup() {
      return (RecursiveGraphLookupContext) payload;
    }
  }

  /** Types of CTEs in the pipeline. */
  private enum CteType {
    BASE,
    MATCH,
    PROJECT,
    GROUP,
    SORT,
    LIMIT,
    SKIP,
    COUNT,
    SAMPLE,
    ADDFIELDS,
    UNWIND,
    LOOKUP,
    UNIONWITH,
    BUCKET,
    BUCKETAUTO,
    FACET,
    REDACT,
    SETWINDOWFIELDS,
    REPLACEROOT,
    GRAPHLOOKUP,
    GRAPHLOOKUP_PATHS,  // Recursive CTE for $graphLookup with maxDepth > 0
    GRAPHLOOKUP_AGG,    // Aggregation CTE for recursive $graphLookup
    GRAPHLOOKUP_JOIN,   // Join CTE to merge recursive results back
    SORT_PAGINATION,  // Combined sort + skip/limit for order preservation
    SORT_GROUP        // Combined sort + group for $first/$last with order
  }

  /**
   * Holds combined sort and pagination stages.
   * CTEs don't preserve ordering, so we must combine $sort with following $skip/$limit
   * into a single CTE with ORDER BY ... OFFSET ... FETCH FIRST.
   */
  private record SortPaginationStages(
      SortStage sort,
      SkipStage skip,   // may be null
      LimitStage limit  // may be null
  ) {}

  /**
   * Holds combined sort and group stages.
   * When $group follows $sort and uses $first/$last accumulators,
   * we need to use Oracle's KEEP (DENSE_RANK FIRST/LAST ORDER BY ...) syntax.
   */
  private record SortGroupStages(
      SortStage sort,
      GroupStage group
  ) {}

  /**
   * Holds context for recursive $graphLookup CTEs.
   * Used to pass the source CTE name to the paths and join CTEs.
   */
  private record RecursiveGraphLookupContext(
      GraphLookupStage stage,
      String sourceCte,  // The CTE before the graphLookup (to join back to)
      String pathsCte,   // Name of the graph_paths CTE
      String aggCte      // Name of the graph_ CTE
  ) {}

  /**
   * Converts a field path to Oracle dot notation format with proper quoting.
   * Handles paths like "productId", "customer.name", "_id", etc.
   * Field names that start with underscore, digit, or contain special characters
   * must be quoted in Oracle's simplified JSON syntax.
   *
   * @param fieldPath the MongoDB field path (without leading $ or .)
   * @return the properly quoted Oracle dot notation path
   */
  private static String quoteDotNotationPath(String fieldPath) {
    if (fieldPath == null || fieldPath.isEmpty()) {
      return fieldPath;
    }
    // Remove leading $ or . if present
    String path = fieldPath;
    if (path.startsWith("$")) {
      path = path.substring(1);
    }
    if (path.startsWith(".")) {
      path = path.substring(1);
    }
    // Split by dots for nested paths and quote each segment if needed
    String[] segments = path.split("\\.");
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < segments.length; i++) {
      if (i > 0) {
        result.append(".");
      }
      String segment = segments[i];
      // Quote if starts with underscore, digit, or contains non-alphanumeric chars
      if (needsQuoting(segment)) {
        result.append("\"").append(segment).append("\"");
      } else {
        result.append(segment);
      }
    }
    return result.toString();
  }

  /**
   * Determines if a field name needs quoting in Oracle dot notation.
   * Names starting with underscore, digit, or containing special characters need quotes.
   */
  private static boolean needsQuoting(String fieldName) {
    if (fieldName == null || fieldName.isEmpty()) {
      return false;
    }
    char first = fieldName.charAt(0);
    // Must quote if starts with underscore or digit
    if (first == '_' || Character.isDigit(first)) {
      return true;
    }
    // Must quote if contains any non-alphanumeric characters
    for (int i = 0; i < fieldName.length(); i++) {
      char c = fieldName.charAt(i);
      if (!Character.isLetterOrDigit(c) && c != '_') {
        return true;
      }
    }
    return false;
  }
}
