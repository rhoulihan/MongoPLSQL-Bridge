/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.JsonReturnType;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortDirection;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortField;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SortStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldRenderSingleFieldAscending() {
    // { $sort: { name: 1 } }
    var sortFields = List.of(new SortField(FieldPathExpression.of("name"), SortDirection.ASC));
    var stage = new SortStage(sortFields);

    stage.render(context);

    assertThat(context.toSql()).isEqualTo("ORDER BY JSON_VALUE(data, '$.name')");
  }

  @Test
  void shouldRenderSingleFieldDescending() {
    // { $sort: { createdAt: -1 } }
    var sortFields =
        List.of(new SortField(FieldPathExpression.of("createdAt"), SortDirection.DESC));
    var stage = new SortStage(sortFields);

    stage.render(context);

    assertThat(context.toSql()).isEqualTo("ORDER BY JSON_VALUE(data, '$.createdAt') DESC");
  }

  @Test
  void shouldRenderMultipleFields() {
    // { $sort: { status: 1, createdAt: -1 } }
    var sortFields =
        List.of(
            new SortField(FieldPathExpression.of("status"), SortDirection.ASC),
            new SortField(FieldPathExpression.of("createdAt"), SortDirection.DESC));
    var stage = new SortStage(sortFields);

    stage.render(context);

    assertThat(context.toSql())
        .isEqualTo("ORDER BY JSON_VALUE(data, '$.status'), JSON_VALUE(data, '$.createdAt') DESC");
  }

  @Test
  void shouldRenderNumericFieldSort() {
    // { $sort: { score: -1 } }
    var sortFields =
        List.of(
            new SortField(
                FieldPathExpression.of("score", JsonReturnType.NUMBER), SortDirection.DESC));
    var stage = new SortStage(sortFields);

    stage.render(context);

    assertThat(context.toSql()).contains("RETURNING NUMBER").contains("DESC");
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new SortStage(List.of());

    assertThat(stage.getOperatorName()).isEqualTo("$sort");
  }

  @Test
  void shouldReturnSortFields() {
    var sortFields = List.of(new SortField(FieldPathExpression.of("name"), SortDirection.ASC));
    var stage = new SortStage(sortFields);

    assertThat(stage.getSortFields()).hasSize(1);
  }

  @Test
  void shouldProvideReadableToString() {
    var sortFields = List.of(new SortField(FieldPathExpression.of("name"), SortDirection.ASC));
    var stage = new SortStage(sortFields);

    assertThat(stage.toString()).contains("SortStage");
  }

  @Test
  void shouldParseSortDirectionFromMongo() {
    assertThat(SortDirection.fromMongo(1)).isEqualTo(SortDirection.ASC);
    assertThat(SortDirection.fromMongo(-1)).isEqualTo(SortDirection.DESC);
  }

  @Test
  void shouldParseSortDirectionFromMongoForHigherValues() {
    // Values > 1 should be ASC, values < -1 should be DESC
    assertThat(SortDirection.fromMongo(5)).isEqualTo(SortDirection.ASC);
    assertThat(SortDirection.fromMongo(-5)).isEqualTo(SortDirection.DESC);
  }

  @Test
  void shouldThrowOnInvalidSortDirection() {
    org.assertj.core.api.Assertions.assertThatThrownBy(() -> SortDirection.fromMongo(0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid sort direction");
  }

  @Test
  void shouldReturnMongoValueFromSortDirection() {
    assertThat(SortDirection.ASC.getMongoValue()).isEqualTo(1);
    assertThat(SortDirection.DESC.getMongoValue()).isEqualTo(-1);
  }

  @Test
  void shouldProvideReadableSortFieldToString() {
    var field = new SortField(FieldPathExpression.of("name"), SortDirection.ASC);

    assertThat(field.toString()).contains("name").contains("ASC");
  }

  @Test
  void shouldCreateSortStageFromMapSpec() {
    var sortSpec = new java.util.LinkedHashMap<String, Integer>();
    sortSpec.put("name", 1);
    sortSpec.put("age", -1);

    var stage = new SortStage(sortSpec);

    assertThat(stage.getSortFields()).hasSize(2);
    assertThat(stage.getSortFields().get(0).getDirection()).isEqualTo(SortDirection.ASC);
    assertThat(stage.getSortFields().get(1).getDirection()).isEqualTo(SortDirection.DESC);
  }

  @Test
  void shouldReturnLimitHint() {
    var sortFields = List.of(new SortField(FieldPathExpression.of("name"), SortDirection.ASC));
    var stageWithHint = new SortStage(sortFields, 10);
    var stageWithoutHint = new SortStage(sortFields);

    assertThat(stageWithHint.getLimitHint()).isEqualTo(10);
    assertThat(stageWithoutHint.getLimitHint()).isNull();
  }

  @Test
  void shouldCreateWithLimitHint() {
    var sortFields = List.of(new SortField(FieldPathExpression.of("name"), SortDirection.ASC));
    var stage = new SortStage(sortFields);

    var stageWithHint = stage.withLimitHint(5);

    assertThat(stageWithHint.getLimitHint()).isEqualTo(5);
    assertThat(stageWithHint.getSortFields()).hasSize(1);
  }

  @Test
  void shouldHandleNullSortFields() {
    var stage = new SortStage((List<SortField>) null);

    assertThat(stage.getSortFields()).isEmpty();
  }

  @Test
  void shouldThrowOnNullFieldPath() {
    org.assertj.core.api.Assertions.assertThatNullPointerException()
        .isThrownBy(() -> new SortField(null, SortDirection.ASC))
        .withMessageContaining("fieldPath");
  }

  @Test
  void shouldThrowOnNullDirection() {
    org.assertj.core.api.Assertions.assertThatNullPointerException()
        .isThrownBy(() -> new SortField(FieldPathExpression.of("name"), null))
        .withMessageContaining("direction");
  }
}
