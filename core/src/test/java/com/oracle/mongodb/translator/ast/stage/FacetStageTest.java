/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FacetStageTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldCreateWithSingleFacet() {
    var stage = new FacetStage(Map.of("categorizedByPrice", List.of(new LimitStage(10))));

    assertThat(stage.getFacets()).hasSize(1);
    assertThat(stage.getFacetNames()).containsExactly("categorizedByPrice");
    assertThat(stage.getFacetPipeline("categorizedByPrice")).hasSize(1);
  }

  @Test
  void shouldCreateWithMultipleFacets() {
    var stage =
        new FacetStage(
            Map.of(
                "byPrice", List.of(new LimitStage(5)),
                "byStatus", List.of(new SkipStage(2), new LimitStage(10))));

    assertThat(stage.getFacets()).hasSize(2);
    assertThat(stage.getFacetNames()).containsExactlyInAnyOrder("byPrice", "byStatus");
    assertThat(stage.getFacetPipeline("byPrice")).hasSize(1);
    assertThat(stage.getFacetPipeline("byStatus")).hasSize(2);
  }

  @Test
  void shouldReturnOperatorName() {
    var stage = new FacetStage(Map.of("test", List.of()));

    assertThat(stage.getOperatorName()).isEqualTo("$facet");
  }

  @Test
  void shouldRenderJsonObject() {
    var stage =
        new FacetStage(
            Map.of(
                "prices", List.of(new LimitStage(10)),
                "counts", List.of()));

    stage.render(context);

    String sql = context.toSql();
    assertThat(sql).contains("JSON_OBJECT(");
    assertThat(sql).contains("'prices'");
    assertThat(sql).contains("'counts'");
  }

  @Test
  void shouldThrowOnNullFacets() {
    assertThatNullPointerException()
        .isThrownBy(() -> new FacetStage(null))
        .withMessageContaining("facets");
  }

  @Test
  void shouldThrowOnEmptyFacets() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new FacetStage(Map.of()))
        .withMessageContaining("at least one");
  }

  @Test
  void shouldProvideReadableToString() {
    var stage =
        new FacetStage(
            Map.of(
                "byPrice", List.of(new LimitStage(5)),
                "byStatus", List.of(new SkipStage(2))));

    String toString = stage.toString();
    assertThat(toString)
        .contains("FacetStage")
        .contains("byPrice")
        .contains("byStatus")
        .contains("stages");
  }

  @Test
  void shouldReturnNullForUnknownFacet() {
    var stage = new FacetStage(Map.of("known", List.of()));

    assertThat(stage.getFacetPipeline("unknown")).isNull();
  }
}
