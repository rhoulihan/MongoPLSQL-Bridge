/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.optimizer;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.Stage;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Optimizer that pushes $match stages as early as possible in the pipeline.
 *
 * <p>This optimizer applies two key transformations:
 *
 * <ul>
 *   <li>Push $match before stages that don't change the filter fields
 *   <li>Merge consecutive $match stages into a single stage
 * </ul>
 *
 * <p>Pushing filters early reduces the amount of data processed by subsequent stages.
 */
public class PredicatePushdownOptimizer implements PipelineOptimizer {

  @Override
  public Pipeline optimize(Pipeline pipeline) {
    if (pipeline.getStages().size() < 2) {
      return pipeline;
    }

    List<Stage> stages = new ArrayList<>(pipeline.getStages());

    // First pass: merge consecutive match stages
    stages = mergeConsecutiveMatches(stages);

    // Second pass: push match stages as early as possible
    stages = pushMatchStages(stages);

    return Pipeline.of(pipeline.getCollection(), stages);
  }

  /** Merges consecutive $match stages into a single $match with $and. */
  private List<Stage> mergeConsecutiveMatches(List<Stage> stages) {
    List<Stage> result = new ArrayList<>();
    int i = 0;

    while (i < stages.size()) {
      Stage current = stages.get(i);

      if (current instanceof MatchStage match) {
        // Collect consecutive match stages
        List<Expression> conditions = new ArrayList<>();
        conditions.add(match.getFilter());

        int j = i + 1;
        while (j < stages.size() && stages.get(j) instanceof MatchStage nextMatch) {
          conditions.add(nextMatch.getFilter());
          j++;
        }

        if (conditions.size() > 1) {
          // Merge into single AND expression
          Expression merged = new LogicalExpression(LogicalOp.AND, conditions);
          result.add(new MatchStage(merged));
        } else {
          result.add(current);
        }

        i = j;
      } else {
        result.add(current);
        i++;
      }
    }

    return result;
  }

  /** Pushes $match stages as early as possible. */
  private List<Stage> pushMatchStages(List<Stage> stages) {
    List<Stage> result = new ArrayList<>(stages);
    boolean changed;

    do {
      changed = false;
      for (int i = 1; i < result.size(); i++) {
        Stage current = result.get(i);
        Stage previous = result.get(i - 1);

        if (current instanceof MatchStage match && canPushBefore(match, previous)) {
          // Swap positions
          result.set(i - 1, current);
          result.set(i, previous);
          changed = true;
        }
      }
    } while (changed);

    return result;
  }

  /** Determines if a match stage can be pushed before another stage. */
  private boolean canPushBefore(MatchStage match, Stage before) {
    // Cannot push before another match (they should be merged first)
    if (before instanceof MatchStage) {
      return false;
    }

    // Can push before $limit, $skip (they don't change fields)
    if (before instanceof LimitStage || before instanceof SkipStage) {
      return true;
    }

    // Can push before $sort (sorting doesn't change values)
    if (before instanceof SortStage) {
      return true;
    }

    // Can push before $project if the filter fields are preserved
    if (before instanceof ProjectStage project) {
      Set<String> matchFields = extractFieldPaths(match.getFilter());
      Set<String> projectedFields = project.getProjections().keySet();

      // Can push if all fields used in match are in the projection
      return projectedFields.containsAll(matchFields);
    }

    // For other stages (like $group, $lookup), be conservative
    return false;
  }

  /** Extracts all field paths referenced in an expression. */
  private Set<String> extractFieldPaths(Expression expr) {
    Set<String> fields = new HashSet<>();
    collectFieldPaths(expr, fields);
    return fields;
  }

  private void collectFieldPaths(Expression expr, Set<String> fields) {
    if (expr instanceof FieldPathExpression fieldPath) {
      fields.add(fieldPath.getPath());
    } else if (expr instanceof ComparisonExpression comp) {
      collectFieldPaths(comp.getLeft(), fields);
      collectFieldPaths(comp.getRight(), fields);
    } else if (expr instanceof LogicalExpression logical) {
      for (Expression operand : logical.getOperands()) {
        collectFieldPaths(operand, fields);
      }
    }
    // Add more expression types as needed
  }

  @Override
  public String getName() {
    return "predicate-pushdown";
  }
}
