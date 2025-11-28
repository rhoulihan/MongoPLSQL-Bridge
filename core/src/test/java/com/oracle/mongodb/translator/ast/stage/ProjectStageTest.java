/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProjectStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderSimpleFieldProjection() {
    // { $project: { name: 1 } }
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));

    var stage = new ProjectStage(projections);

    stage.render(context);

    assertThat(context.toSql()).isEqualTo("SELECT JSON_VALUE(data, '$.name') AS name");
  }

  @Test
  void shouldRenderMultipleFieldProjections() {
    // { $project: { name: 1, status: 1, amount: 1 } }
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
    projections.put("status", ProjectionField.include(FieldPathExpression.of("status")));
    projections.put("amount", ProjectionField.include(FieldPathExpression.of("amount")));

    var stage = new ProjectStage(projections);

    stage.render(context);

    assertThat(context.toSql())
        .contains("SELECT")
        .contains("AS name")
        .contains("AS status")
        .contains("AS amount");
  }

  @Test
  void shouldRenderFieldRename() {
    // { $project: { userName: "$name" } }
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("userName", ProjectionField.include(FieldPathExpression.of("name")));

    var stage = new ProjectStage(projections);

    stage.render(context);

    assertThat(context.toSql()).isEqualTo("SELECT JSON_VALUE(data, '$.name') AS userName");
  }

  @Test
  void shouldRenderLiteralProjection() {
    // { $project: { fixedValue: "constant" } }
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("fixedValue", ProjectionField.include(LiteralExpression.of("constant")));

    var stage = new ProjectStage(projections);

    stage.render(context);

    assertThat(context.toSql()).contains("AS fixedValue");
  }

  @Test
  void shouldSkipExcludedFields() {
    // { $project: { _id: 0, name: 1 } }
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("_id", ProjectionField.exclude());
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));

    var stage = new ProjectStage(projections);

    stage.render(context);

    assertThat(context.toSql())
        .isEqualTo("SELECT JSON_VALUE(data, '$.name') AS name")
        .doesNotContain("_id");
  }

  @Test
  void shouldHandleEmptyProjection() {
    // Edge case: all fields excluded
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("_id", ProjectionField.exclude());

    var stage = new ProjectStage(projections, true);

    stage.render(context);

    assertThat(context.toSql()).isEqualTo("SELECT NULL AS dummy");
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new ProjectStage(new LinkedHashMap<>());

    assertThat(stage.getOperatorName()).isEqualTo("$project");
  }

  @Test
  void shouldReturnProjections() {
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
    var stage = new ProjectStage(projections);

    assertThat(stage.getProjections()).containsKey("name");
  }

  @Test
  void shouldReturnExclusionMode() {
    var inclusionStage = new ProjectStage(new LinkedHashMap<>(), false);
    var exclusionStage = new ProjectStage(new LinkedHashMap<>(), true);

    assertThat(inclusionStage.isExclusionMode()).isFalse();
    assertThat(exclusionStage.isExclusionMode()).isTrue();
  }

  @Test
  void shouldProvideReadableToString() {
    var projections = new LinkedHashMap<String, ProjectionField>();
    projections.put("name", ProjectionField.include(FieldPathExpression.of("name")));
    var stage = new ProjectStage(projections);

    assertThat(stage.toString()).contains("ProjectStage");
  }

  @Test
  void shouldProvideReadableProjectionFieldToString() {
    var includedField = ProjectionField.include(FieldPathExpression.of("name"));
    var excludedField = ProjectionField.exclude();

    assertThat(includedField.toString()).contains("name");
    assertThat(excludedField.toString()).isEqualTo("EXCLUDED");
  }

  @Test
  void shouldHandleNullProjectionsMap() {
    var stage = new ProjectStage(null);

    assertThat(stage.getProjections()).isEmpty();
  }

  @Test
  void shouldHandleNullProjectionsMapWithExclusionMode() {
    var stage = new ProjectStage(null, true);

    assertThat(stage.getProjections()).isEmpty();
    assertThat(stage.isExclusionMode()).isTrue();
  }
}
