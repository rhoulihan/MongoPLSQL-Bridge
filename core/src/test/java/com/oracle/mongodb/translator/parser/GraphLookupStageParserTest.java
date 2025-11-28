/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.GraphLookupStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GraphLookupStageParserTest {

  private GraphLookupStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new GraphLookupStageParser();
  }

  @Test
  void shouldParseBasicGraphLookup() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "name",
                "as": "reportingHierarchy"
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getFrom()).isEqualTo("employees");
    assertThat(stage.getStartWith()).isEqualTo("$reportsTo");
    assertThat(stage.getConnectFromField()).isEqualTo("reportsTo");
    assertThat(stage.getConnectToField()).isEqualTo("name");
    assertThat(stage.getAs()).isEqualTo("reportingHierarchy");
    assertThat(stage.getMaxDepth()).isNull();
    assertThat(stage.getDepthField()).isNull();
  }

  @Test
  void shouldParseWithMaxDepth() {
    var doc =
        Document.parse(
            """
            {
                "from": "categories",
                "startWith": "$parentCategory",
                "connectFromField": "parentCategory",
                "connectToField": "_id",
                "as": "ancestors",
                "maxDepth": 5
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getMaxDepth()).isEqualTo(5);
  }

  @Test
  void shouldParseWithDepthField() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$managerId",
                "connectFromField": "managerId",
                "connectToField": "_id",
                "as": "managers",
                "depthField": "level"
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getDepthField()).isEqualTo("level");
  }

  @Test
  void shouldParseWithAllOptionalFields() {
    var doc =
        Document.parse(
            """
            {
                "from": "nodes",
                "startWith": "$parentId",
                "connectFromField": "parentId",
                "connectToField": "_id",
                "as": "path",
                "maxDepth": 10,
                "depthField": "depth"
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getFrom()).isEqualTo("nodes");
    assertThat(stage.getStartWith()).isEqualTo("$parentId");
    assertThat(stage.getConnectFromField()).isEqualTo("parentId");
    assertThat(stage.getConnectToField()).isEqualTo("_id");
    assertThat(stage.getAs()).isEqualTo("path");
    assertThat(stage.getMaxDepth()).isEqualTo(10);
    assertThat(stage.getDepthField()).isEqualTo("depth");
  }

  @Test
  void shouldHandleIntegerMaxDepth() {
    var doc =
        Document.parse(
            """
            {
                "from": "tree",
                "startWith": "$parent",
                "connectFromField": "parent",
                "connectToField": "id",
                "as": "ancestors",
                "maxDepth": 3
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getMaxDepth()).isEqualTo(3);
  }

  @Test
  void shouldHandleDoubleMaxDepth() {
    var doc =
        new Document()
            .append("from", "tree")
            .append("startWith", "$parent")
            .append("connectFromField", "parent")
            .append("connectToField", "id")
            .append("as", "ancestors")
            .append("maxDepth", 7.0);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getMaxDepth()).isEqualTo(7);
  }

  @Test
  void shouldHandleNonStringStartWith() {
    // When startWith is not a string (e.g., an expression document), convert to string
    var doc =
        new Document()
            .append("from", "employees")
            .append("startWith", new Document("$literal", "CEO"))
            .append("connectFromField", "reportsTo")
            .append("connectToField", "name")
            .append("as", "hierarchy");

    GraphLookupStage stage = parser.parse(doc);

    // Non-string startWith should be converted to string representation
    assertThat(stage.getStartWith()).contains("literal");
  }

  @Test
  void shouldThrowOnMissingFrom() {
    var doc =
        Document.parse(
            """
            {
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "name",
                "as": "hierarchy"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("from");
  }

  @Test
  void shouldThrowOnMissingStartWith() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "connectFromField": "reportsTo",
                "connectToField": "name",
                "as": "hierarchy"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("startWith");
  }

  @Test
  void shouldThrowOnMissingConnectFromField() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$reportsTo",
                "connectToField": "name",
                "as": "hierarchy"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("connectFromField");
  }

  @Test
  void shouldThrowOnMissingConnectToField() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "as": "hierarchy"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("connectToField");
  }

  @Test
  void shouldThrowOnMissingAs() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "name"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("as");
  }

  @Test
  void shouldThrowOnNonStringFrom() {
    var doc =
        new Document()
            .append("from", 123)
            .append("startWith", "$reportsTo")
            .append("connectFromField", "reportsTo")
            .append("connectToField", "name")
            .append("as", "hierarchy");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("from")
        .withMessageContaining("string");
  }

  @Test
  void shouldThrowOnNonStringConnectFromField() {
    var doc =
        new Document()
            .append("from", "employees")
            .append("startWith", "$reportsTo")
            .append("connectFromField", 123)
            .append("connectToField", "name")
            .append("as", "hierarchy");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("connectFromField")
        .withMessageContaining("string");
  }

  @Test
  void shouldThrowOnNonStringConnectToField() {
    var doc =
        new Document()
            .append("from", "employees")
            .append("startWith", "$reportsTo")
            .append("connectFromField", "reportsTo")
            .append("connectToField", 123)
            .append("as", "hierarchy");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("connectToField")
        .withMessageContaining("string");
  }

  @Test
  void shouldThrowOnNonStringAs() {
    var doc =
        new Document()
            .append("from", "employees")
            .append("startWith", "$reportsTo")
            .append("connectFromField", "reportsTo")
            .append("connectToField", "name")
            .append("as", 123);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("as")
        .withMessageContaining("string");
  }

  @Test
  void shouldThrowOnNonNumericMaxDepth() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "name",
                "as": "hierarchy",
                "maxDepth": "five"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("maxDepth")
        .withMessageContaining("number");
  }

  @Test
  void shouldThrowOnNonStringDepthField() {
    var doc =
        new Document()
            .append("from", "employees")
            .append("startWith", "$reportsTo")
            .append("connectFromField", "reportsTo")
            .append("connectToField", "name")
            .append("as", "hierarchy")
            .append("depthField", 123);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("depthField")
        .withMessageContaining("string");
  }

  @Test
  void shouldReturnCorrectOperatorName() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "name",
                "as": "hierarchy"
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getOperatorName()).isEqualTo("$graphLookup");
  }

  // restrictSearchWithMatch tests

  @Test
  void shouldParseWithRestrictSearchWithMatch() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "name",
                "as": "hierarchy",
                "restrictSearchWithMatch": {
                    "status": "active"
                }
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getRestrictSearchWithMatch()).isNotNull();
    assertThat(stage.getRestrictSearchWithMatch().getString("status")).isEqualTo("active");
  }

  @Test
  void shouldParseWithComplexRestrictSearchWithMatch() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$reportsTo",
                "connectFromField": "reportsTo",
                "connectToField": "name",
                "as": "hierarchy",
                "restrictSearchWithMatch": {
                    "status": "active",
                    "department": { "$in": ["Engineering", "Sales"] }
                }
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getRestrictSearchWithMatch()).isNotNull();
    assertThat(stage.getRestrictSearchWithMatch().getString("status")).isEqualTo("active");
    assertThat(stage.getRestrictSearchWithMatch().get("department")).isNotNull();
  }

  @Test
  void shouldParseWithAllOptionsIncludingRestrictSearchWithMatch() {
    var doc =
        Document.parse(
            """
            {
                "from": "employees",
                "startWith": "$managerId",
                "connectFromField": "managerId",
                "connectToField": "_id",
                "as": "managers",
                "maxDepth": 5,
                "depthField": "level",
                "restrictSearchWithMatch": {
                    "active": true
                }
            }
            """);

    GraphLookupStage stage = parser.parse(doc);

    assertThat(stage.getFrom()).isEqualTo("employees");
    assertThat(stage.getMaxDepth()).isEqualTo(5);
    assertThat(stage.getDepthField()).isEqualTo("level");
    assertThat(stage.getRestrictSearchWithMatch()).isNotNull();
    assertThat(stage.getRestrictSearchWithMatch().getBoolean("active")).isTrue();
  }

  @Test
  void shouldThrowOnNonDocumentRestrictSearchWithMatch() {
    var doc =
        new Document()
            .append("from", "employees")
            .append("startWith", "$reportsTo")
            .append("connectFromField", "reportsTo")
            .append("connectToField", "name")
            .append("as", "hierarchy")
            .append("restrictSearchWithMatch", "invalid");

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("restrictSearchWithMatch")
        .withMessageContaining("document");
  }
}
