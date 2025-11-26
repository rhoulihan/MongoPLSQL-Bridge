/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import com.oracle.mongodb.translator.ast.stage.AddFieldsStage;
import java.util.ArrayList;
import java.util.List;

/**
 * Renders a MongoDB aggregation pipeline as a properly structured Oracle SQL query.
 *
 * <p>This class analyzes the pipeline stages and combines them into a single SQL query
 * with the correct clause ordering:
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
 * <p>Multiple $match stages are combined with AND. The last $project or $group
 * determines the SELECT clause.
 */
public final class PipelineRenderer {

    private final OracleConfiguration config;

    public PipelineRenderer(OracleConfiguration config) {
        this.config = config;
    }

    /**
     * Renders the pipeline to the given context.
     */
    public void render(Pipeline pipeline, SqlGenerationContext ctx) {
        // Analyze pipeline to extract components
        PipelineComponents components = analyzePipeline(pipeline);

        // Render SELECT clause
        renderSelectClause(components, ctx);

        // Render FROM clause
        renderFromClause(components, ctx);

        // Render JOIN clauses ($lookup stages)
        renderJoinClauses(components, ctx);

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

    private PipelineComponents analyzePipeline(Pipeline pipeline) {
        PipelineComponents components = new PipelineComponents();

        for (Stage stage : pipeline.getStages()) {
            if (stage instanceof MatchStage match) {
                components.matchStages.add(match);
            } else if (stage instanceof GroupStage group) {
                components.groupStage = group;
            } else if (stage instanceof ProjectStage project) {
                components.projectStage = project;
            } else if (stage instanceof SortStage sort) {
                components.sortStage = sort;
            } else if (stage instanceof SkipStage skip) {
                components.skipStage = skip;
            } else if (stage instanceof LimitStage limit) {
                components.limitStage = limit;
            } else if (stage instanceof LookupStage lookup) {
                components.lookupStages.add(lookup);
            } else if (stage instanceof UnwindStage unwind) {
                components.unwindStages.add(unwind);
            } else if (stage instanceof AddFieldsStage addFields) {
                components.addFieldsStages.add(addFields);
            }
            // For unknown stages, we skip them (they won't be rendered)
        }

        return components;
    }

    private void renderSelectClause(PipelineComponents components, SqlGenerationContext ctx) {
        ctx.sql("SELECT ");

        if (components.groupStage != null) {
            // $group determines the SELECT clause
            renderGroupSelectClause(components.groupStage, ctx);
        } else if (components.projectStage != null) {
            // $project determines the SELECT clause
            renderProjectSelectClause(components.projectStage, ctx);
        } else {
            // Default: select all data
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
    }

    private void renderGroupSelectClause(GroupStage group, SqlGenerationContext ctx) {
        boolean first = true;

        // Render _id expression if present
        if (group.getIdExpression() != null) {
            ctx.visit(group.getIdExpression());
            ctx.sql(" AS _id");
            first = false;
        }

        // Render accumulators
        for (var entry : group.getAccumulators().entrySet()) {
            if (!first) {
                ctx.sql(", ");
            }
            ctx.visit(entry.getValue());
            ctx.sql(" AS ");
            ctx.sql(entry.getKey());
            first = false;
        }

        // If nothing was rendered, select a placeholder
        if (first) {
            ctx.sql("NULL AS dummy");
        }
    }

    private void renderProjectSelectClause(ProjectStage project, SqlGenerationContext ctx) {
        boolean first = true;

        for (var entry : project.getProjections().entrySet()) {
            String alias = entry.getKey();
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
            ctx.sql(alias);
            first = false;
        }

        if (first) {
            ctx.sql("NULL AS dummy");
        }
    }

    private void renderFromClause(PipelineComponents components, SqlGenerationContext ctx) {
        ctx.sql(" FROM ");
        ctx.sql(config.qualifiedTableName());

        // Add alias if we have joins or unwinds (to disambiguate table references)
        if (!components.lookupStages.isEmpty() || !components.unwindStages.isEmpty()) {
            ctx.sql(" ");
            ctx.sql(ctx.getBaseTableAlias());
        }

        // Render unwind stages as cross joins with JSON_TABLE
        for (UnwindStage unwind : components.unwindStages) {
            ctx.sql(", ");
            ctx.visit(unwind);
        }
    }

    private void renderJoinClauses(PipelineComponents components, SqlGenerationContext ctx) {
        for (LookupStage lookup : components.lookupStages) {
            ctx.sql(" ");
            ctx.visit(lookup);
        }
    }

    private void renderWhereClause(PipelineComponents components, SqlGenerationContext ctx) {
        if (components.matchStages.isEmpty()) {
            return;
        }

        ctx.sql(" WHERE ");

        if (components.matchStages.size() == 1) {
            // Single match - render its filter directly
            ctx.visit(components.matchStages.get(0).getFilter());
        } else {
            // Multiple matches - combine with AND
            List<Expression> filters = new ArrayList<>();
            for (MatchStage match : components.matchStages) {
                filters.add(match.getFilter());
            }
            LogicalExpression combined = new LogicalExpression(LogicalOp.AND, filters);
            ctx.visit(combined);
        }
    }

    private void renderGroupByClause(PipelineComponents components, SqlGenerationContext ctx) {
        if (components.groupStage == null || components.groupStage.getIdExpression() == null) {
            return;
        }

        ctx.sql(" GROUP BY ");
        ctx.visit(components.groupStage.getIdExpression());
    }

    private void renderOrderByClause(PipelineComponents components, SqlGenerationContext ctx) {
        if (components.sortStage == null || components.sortStage.getSortFields().isEmpty()) {
            return;
        }

        ctx.sql(" ORDER BY ");

        boolean first = true;
        for (SortStage.SortField field : components.sortStage.getSortFields()) {
            if (!first) {
                ctx.sql(", ");
            }
            ctx.visit(field.getFieldPath());
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
        if (components.limitStage == null) {
            return;
        }

        ctx.sql(" FETCH FIRST ");
        ctx.sql(String.valueOf(components.limitStage.getLimit()));
        ctx.sql(" ROWS ONLY");
    }

    /**
     * Holds the decomposed components of a pipeline.
     */
    private static class PipelineComponents {
        List<MatchStage> matchStages = new ArrayList<>();
        List<LookupStage> lookupStages = new ArrayList<>();
        List<UnwindStage> unwindStages = new ArrayList<>();
        List<AddFieldsStage> addFieldsStages = new ArrayList<>();
        GroupStage groupStage;
        ProjectStage projectStage;
        SortStage sortStage;
        SkipStage skipStage;
        LimitStage limitStage;
    }
}
