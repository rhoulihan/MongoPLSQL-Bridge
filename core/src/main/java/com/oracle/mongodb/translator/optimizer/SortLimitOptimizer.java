/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.optimizer;

import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.Stage;
import java.util.ArrayList;
import java.util.List;

/**
 * Optimizer that combines consecutive $sort, $skip, and $limit stages.
 *
 * <p>This optimizer applies several transformations:
 *
 * <ul>
 *   <li>Marks $sort with limit hint when followed by $limit (Top-N optimization)
 *   <li>Handles $sort + $skip + $limit pattern for OFFSET/FETCH
 *   <li>Merges consecutive $limit stages (takes minimum)
 *   <li>Merges consecutive $skip stages (sums values)
 *   <li>Removes redundant consecutive $sort stages (last one wins)
 * </ul>
 *
 * <p>These optimizations are important because:
 *
 * <ul>
 *   <li>Oracle can stop sorting after finding the top N rows
 *   <li>Memory usage is reduced (only N rows need to be kept)
 *   <li>Indexes can be used more efficiently
 *   <li>Fewer stages means simpler SQL and fewer subqueries
 * </ul>
 */
public class SortLimitOptimizer implements PipelineOptimizer {

  @Override
  public Pipeline optimize(Pipeline pipeline) {
    if (pipeline.getStages().isEmpty()) {
      return pipeline;
    }

    List<Stage> stages = new ArrayList<>(pipeline.getStages());

    // First pass: merge consecutive limits
    stages = mergeConsecutiveLimits(stages);

    // Second pass: merge consecutive skips
    stages = mergeConsecutiveSkips(stages);

    // Third pass: remove redundant consecutive sorts
    stages = removeRedundantSorts(stages);

    // Fourth pass: apply sort-limit and sort-skip-limit optimizations
    stages = applySortLimitOptimizations(stages);

    return Pipeline.of(pipeline.getCollection(), stages);
  }

  /** Merges consecutive $limit stages, keeping the minimum value. */
  private List<Stage> mergeConsecutiveLimits(List<Stage> stages) {
    List<Stage> result = new ArrayList<>();
    int i = 0;

    while (i < stages.size()) {
      Stage current = stages.get(i);

      if (current instanceof LimitStage limit) {
        int minLimit = limit.getLimit();
        int j = i + 1;

        // Collect consecutive limits
        while (j < stages.size() && stages.get(j) instanceof LimitStage nextLimit) {
          minLimit = Math.min(minLimit, nextLimit.getLimit());
          j++;
        }

        result.add(new LimitStage(minLimit));
        i = j;
      } else {
        result.add(current);
        i++;
      }
    }

    return result;
  }

  /** Merges consecutive $skip stages, summing their values. */
  private List<Stage> mergeConsecutiveSkips(List<Stage> stages) {
    List<Stage> result = new ArrayList<>();
    int i = 0;

    while (i < stages.size()) {
      Stage current = stages.get(i);

      if (current instanceof SkipStage skip) {
        int totalSkip = skip.getSkip();
        int j = i + 1;

        // Collect consecutive skips
        while (j < stages.size() && stages.get(j) instanceof SkipStage nextSkip) {
          totalSkip += nextSkip.getSkip();
          j++;
        }

        result.add(new SkipStage(totalSkip));
        i = j;
      } else {
        result.add(current);
        i++;
      }
    }

    return result;
  }

  /** Removes redundant consecutive $sort stages, keeping only the last one. */
  private List<Stage> removeRedundantSorts(List<Stage> stages) {
    List<Stage> result = new ArrayList<>();
    int i = 0;

    while (i < stages.size()) {
      Stage current = stages.get(i);

      if (current instanceof SortStage) {
        // Look ahead for more consecutive sorts
        int lastSortIndex = i;
        int j = i + 1;

        while (j < stages.size() && stages.get(j) instanceof SortStage) {
          lastSortIndex = j;
          j++;
        }

        // Only keep the last sort (it overwrites all previous sorts)
        result.add(stages.get(lastSortIndex));
        i = j;
      } else {
        result.add(current);
        i++;
      }
    }

    return result;
  }

  /** Applies sort-limit and sort-skip-limit optimizations. */
  private List<Stage> applySortLimitOptimizations(List<Stage> stages) {
    List<Stage> result = new ArrayList<>();

    for (int i = 0; i < stages.size(); i++) {
      Stage current = stages.get(i);

      if (current instanceof SortStage sort) {
        int limitHint = calculateLimitHint(stages, i);
        if (limitHint > 0) {
          result.add(sort.withLimitHint(limitHint));
        } else {
          result.add(current);
        }
      } else {
        result.add(current);
      }
    }

    return result;
  }

  /**
   * Calculates the limit hint for a sort stage.
   *
   * <p>Handles patterns:
   *
   * <ul>
   *   <li>$sort + $limit -> limit value
   *   <li>$sort + $skip + $limit -> skip + limit (total rows needed)
   * </ul>
   */
  private int calculateLimitHint(List<Stage> stages, int sortIndex) {
    if (sortIndex + 1 >= stages.size()) {
      return 0;
    }

    Stage next = stages.get(sortIndex + 1);

    // Pattern: $sort + $limit
    if (next instanceof LimitStage limit) {
      return limit.getLimit();
    }

    // Pattern: $sort + $skip + $limit
    if (next instanceof SkipStage skip && sortIndex + 2 < stages.size()) {
      Stage afterSkip = stages.get(sortIndex + 2);
      if (afterSkip instanceof LimitStage limit) {
        return skip.getSkip() + limit.getLimit();
      }
    }

    return 0;
  }

  @Override
  public String getName() {
    return "sort-limit";
  }
}
