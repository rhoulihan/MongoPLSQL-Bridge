/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.expression.ArrayExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Analyzes a pipeline to detect stages that require CTE (Common Table Expression) breakpoints.
 *
 * <p>CTEs are needed when:
 *
 * <ul>
 *   <li>Multiple $group stages exist - each group after the first needs intermediate results
 *   <li>$project after $group uses array operations on grouped output fields
 *   <li>$unwind → $lookup sequences require materialization before aggregation
 * </ul>
 */
public final class PipelineStageSequence {

  private final List<StageGroup> stageGroups;

  private PipelineStageSequence(List<StageGroup> stageGroups) {
    this.stageGroups = Collections.unmodifiableList(new ArrayList<>(stageGroups));
  }

  /**
   * Analyzes a pipeline and returns a sequence of stage groups.
   *
   * @param pipeline the pipeline to analyze
   * @return the stage sequence analysis
   */
  public static PipelineStageSequence analyze(Pipeline pipeline) {
    List<Stage> stages = pipeline.getStages();
    List<StageGroup> groups = new ArrayList<>();

    if (stages.isEmpty()) {
      // Empty pipeline: single group with no stages
      groups.add(new StageGroup(new ArrayList<>(), null));
      return new PipelineStageSequence(groups);
    }

    List<Stage> currentGroup = new ArrayList<>();
    int cteIndex = 0;
    boolean hasSeenGroup = false;

    for (int i = 0; i < stages.size(); i++) {
      Stage stage = stages.get(i);

      if (stage instanceof GroupStage) {
        if (hasSeenGroup) {
          // Multiple $group stages: previous group becomes CTE
          String cteName = "cte_group_" + cteIndex++;
          groups.add(new StageGroup(currentGroup, cteName));
          currentGroup = new ArrayList<>();
        }
        hasSeenGroup = true;
      }

      if (stage instanceof ProjectStage && hasSeenGroup) {
        ProjectStage project = (ProjectStage) stage;
        if (hasArrayOperationsOnGroupOutput(project)) {
          // $project with array ops after $group: group becomes CTE
          if (!currentGroup.isEmpty() && containsGroupStage(currentGroup)) {
            String cteName = "cte_group_" + cteIndex++;
            groups.add(new StageGroup(currentGroup, cteName));
            currentGroup = new ArrayList<>();
          }
        }
      }

      currentGroup.add(stage);
    }

    // Add final group (the main query, no CTE name)
    groups.add(new StageGroup(currentGroup, null));

    return new PipelineStageSequence(groups);
  }

  private static boolean containsGroupStage(List<Stage> stages) {
    for (Stage stage : stages) {
      if (stage instanceof GroupStage) {
        return true;
      }
    }
    return false;
  }

  private static int countGroupStages(List<Stage> stages) {
    int count = 0;
    for (Stage stage : stages) {
      if (stage instanceof GroupStage) {
        count++;
      }
    }
    return count;
  }

  private static boolean hasArrayOperationsOnGroupOutput(ProjectStage project) {
    for (ProjectionField field : project.getProjections().values()) {
      Expression expr = field.getExpression();
      if (hasArrayExpression(expr)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasArrayExpression(Expression expr) {
    if (expr == null) {
      return false;
    }
    // Check if expression is or contains an ArrayExpression (like $slice, $sortArray)
    return expr instanceof ArrayExpression;
  }

  /** Returns true if this pipeline requires CTEs. */
  public boolean requiresCtes() {
    // If we have more than one group, CTEs are needed
    return stageGroups.size() > 1;
  }

  /** Returns the number of CTEs needed. */
  public int getCteCount() {
    int count = 0;
    for (StageGroup group : stageGroups) {
      if (group.getCteName() != null) {
        count++;
      }
    }
    return count;
  }

  /** Returns the stage groups in order. */
  public List<StageGroup> getStageGroups() {
    return stageGroups;
  }

  /** A group of stages that will be rendered together, optionally as a CTE. */
  public static final class StageGroup {
    private final List<Stage> stages;
    private final String cteName; // null for final query

    StageGroup(List<Stage> stages, String cteName) {
      this.stages = Collections.unmodifiableList(new ArrayList<>(stages));
      this.cteName = cteName;
    }

    /** Returns the stages in this group. */
    public List<Stage> getStages() {
      return stages;
    }

    /** Returns the CTE name, or null if this is the final query. */
    public String getCteName() {
      return cteName;
    }

    @Override
    public String toString() {
      return "StageGroup(cteName=" + cteName + ", stages=" + stages.size() + ")";
    }
  }
}
