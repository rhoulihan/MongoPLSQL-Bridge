/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.optimizer;

import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.Stage;
import java.util.ArrayList;
import java.util.List;

/**
 * Optimizer that combines consecutive $sort and $limit stages.
 *
 * <p>When a $sort is immediately followed by a $limit, this optimizer marks the sort with a "limit
 * hint" that allows the SQL generator to use Oracle's Top-N optimization (FETCH FIRST N ROWS ONLY).
 *
 * <p>This optimization is important because:
 *
 * <ul>
 *   <li>Oracle can stop sorting after finding the top N rows
 *   <li>Memory usage is reduced (only N rows need to be kept)
 *   <li>Indexes can be used more efficiently
 * </ul>
 */
public class SortLimitOptimizer implements PipelineOptimizer {

  @Override
  public Pipeline optimize(Pipeline pipeline) {
    if (pipeline.getStages().size() < 2) {
      return pipeline;
    }

    List<Stage> stages = new ArrayList<>();
    List<Stage> originalStages = pipeline.getStages();

    for (int i = 0; i < originalStages.size(); i++) {
      Stage current = originalStages.get(i);

      if (current instanceof SortStage sort) {
        // Check if next stage is a limit
        if (i + 1 < originalStages.size()
            && originalStages.get(i + 1) instanceof LimitStage limit) {
          // Create sort with limit hint
          stages.add(sort.withLimitHint(limit.getLimit()));
        } else {
          stages.add(current);
        }
      } else {
        stages.add(current);
      }
    }

    return Pipeline.of(pipeline.getCollection(), stages);
  }

  @Override
  public String getName() {
    return "sort-limit";
  }
}
