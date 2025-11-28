/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import org.bson.Document;
import org.junit.jupiter.api.Test;

class StageParserRegistryTest {

  @Test
  void shouldContainLimitParser() {
    var registry = new StageParserRegistry();

    assertThat(registry.hasParser("$limit")).isTrue();
  }

  @Test
  void shouldContainSkipParser() {
    var registry = new StageParserRegistry();

    assertThat(registry.hasParser("$skip")).isTrue();
  }

  @Test
  void shouldReturnNullForUnknownStage() {
    var registry = new StageParserRegistry();

    assertThat(registry.getParser("$unknown")).isNull();
  }

  @Test
  void shouldParseLimitValue() {
    var registry = new StageParserRegistry();
    var parser = registry.getParser("$limit");

    var stage = parser.parse(10);

    assertThat(stage).isInstanceOf(LimitStage.class);
    assertThat(((LimitStage) stage).getLimit()).isEqualTo(10);
  }

  @Test
  void shouldParseSkipValue() {
    var registry = new StageParserRegistry();
    var parser = registry.getParser("$skip");

    var stage = parser.parse(20);

    assertThat(stage).isInstanceOf(SkipStage.class);
    assertThat(((SkipStage) stage).getSkip()).isEqualTo(20);
  }

  @Test
  void shouldRegisterCustomParser() {
    var registry = new StageParserRegistry();
    registry.register("$custom", value -> new LimitStage(999));

    assertThat(registry.hasParser("$custom")).isTrue();
  }

  @Test
  void shouldReturnRegisteredParserNames() {
    var registry = new StageParserRegistry();

    var names = registry.getRegisteredOperators();

    assertThat(names).contains("$limit", "$skip", "$match", "$group", "$project", "$sort");
  }

  @Test
  void shouldContainMatchParser() {
    var registry = new StageParserRegistry();

    assertThat(registry.hasParser("$match")).isTrue();
  }

  @Test
  void shouldParseMatchStage() {
    var registry = new StageParserRegistry();
    var parser = registry.getParser("$match");

    var filter = new Document("status", "active");
    var stage = parser.parse(filter);

    assertThat(stage).isInstanceOf(MatchStage.class);
    assertThat(stage.getOperatorName()).isEqualTo("$match");
  }

  @Test
  void shouldParseComplexMatchStage() {
    var registry = new StageParserRegistry();
    var parser = registry.getParser("$match");

    var filter =
        new Document().append("status", "active").append("amount", new Document("$gt", 100));
    var stage = parser.parse(filter);

    assertThat(stage).isInstanceOf(MatchStage.class);
  }

  @Test
  void shouldContainGroupParser() {
    var registry = new StageParserRegistry();

    assertThat(registry.hasParser("$group")).isTrue();
  }

  @Test
  void shouldParseGroupStage() {
    var registry = new StageParserRegistry();
    var parser = registry.getParser("$group");

    var groupDoc = new Document().append("_id", "$status").append("count", new Document("$sum", 1));
    var stage = parser.parse(groupDoc);

    assertThat(stage).isInstanceOf(GroupStage.class);
    assertThat(stage.getOperatorName()).isEqualTo("$group");
  }

  @Test
  void shouldContainProjectParser() {
    var registry = new StageParserRegistry();

    assertThat(registry.hasParser("$project")).isTrue();
  }

  @Test
  void shouldParseProjectStage() {
    var registry = new StageParserRegistry();
    var parser = registry.getParser("$project");

    var projectDoc = new Document().append("name", 1).append("status", 1);
    var stage = parser.parse(projectDoc);

    assertThat(stage).isInstanceOf(ProjectStage.class);
    assertThat(stage.getOperatorName()).isEqualTo("$project");
  }

  @Test
  void shouldContainSortParser() {
    var registry = new StageParserRegistry();

    assertThat(registry.hasParser("$sort")).isTrue();
  }

  @Test
  void shouldParseSortStage() {
    var registry = new StageParserRegistry();
    var parser = registry.getParser("$sort");

    var sortDoc = new Document().append("name", 1).append("createdAt", -1);
    var stage = parser.parse(sortDoc);

    assertThat(stage).isInstanceOf(SortStage.class);
    assertThat(stage.getOperatorName()).isEqualTo("$sort");
    assertThat(((SortStage) stage).getSortFields()).hasSize(2);
  }
}
