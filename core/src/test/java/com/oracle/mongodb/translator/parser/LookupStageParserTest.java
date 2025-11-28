/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.LookupStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LookupStageParserTest {

  private LookupStageParser parser;

  @BeforeEach
  void setUp() {
    parser = new LookupStageParser();
  }

  @Test
  void shouldParseBasicLookup() {
    var doc =
        Document.parse(
            """
            {
                "from": "inventory",
                "localField": "item",
                "foreignField": "sku",
                "as": "inventory_docs"
            }
            """);

    LookupStage stage = parser.parse(doc);

    assertThat(stage.getFrom()).isEqualTo("inventory");
    assertThat(stage.getLocalField()).isEqualTo("item");
    assertThat(stage.getForeignField()).isEqualTo("sku");
    assertThat(stage.getAs()).isEqualTo("inventory_docs");
  }

  @Test
  void shouldParseWithNestedFields() {
    var doc =
        Document.parse(
            """
            {
                "from": "users",
                "localField": "metadata.authorId",
                "foreignField": "profile.userId",
                "as": "author"
            }
            """);

    LookupStage stage = parser.parse(doc);

    assertThat(stage.getLocalField()).isEqualTo("metadata.authorId");
    assertThat(stage.getForeignField()).isEqualTo("profile.userId");
  }

  @Test
  void shouldThrowOnMissingFrom() {
    var doc =
        Document.parse(
            """
            {
                "localField": "item",
                "foreignField": "sku",
                "as": "docs"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("from");
  }

  @Test
  void shouldThrowOnMissingLocalField() {
    var doc =
        Document.parse(
            """
            {
                "from": "inventory",
                "foreignField": "sku",
                "as": "docs"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("localField");
  }

  @Test
  void shouldThrowOnMissingForeignField() {
    var doc =
        Document.parse(
            """
            {
                "from": "inventory",
                "localField": "item",
                "as": "docs"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("foreignField");
  }

  @Test
  void shouldThrowOnMissingAs() {
    var doc =
        Document.parse(
            """
            {
                "from": "inventory",
                "localField": "item",
                "foreignField": "sku"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("as");
  }

  @Test
  void shouldThrowOnNonStringFrom() {
    var doc =
        Document.parse(
            """
            {
                "from": 123,
                "localField": "item",
                "foreignField": "sku",
                "as": "docs"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("from")
        .withMessageContaining("string");
  }

  @Test
  void shouldParsePipelineFormWithLet() {
    var doc =
        Document.parse(
            """
            {
                "from": "warehouses",
                "let": { "order_item": "$item", "order_qty": "$quantity" },
                "pipeline": [
                    { "$match": { "status": "active" } }
                ],
                "as": "stockdata"
            }
            """);

    LookupStage stage = parser.parse(doc);

    assertThat(stage.getFrom()).isEqualTo("warehouses");
    assertThat(stage.getAs()).isEqualTo("stockdata");
    assertThat(stage.getLetVariables()).containsEntry("order_item", "item");
    assertThat(stage.getLetVariables()).containsEntry("order_qty", "quantity");
    assertThat(stage.getPipeline()).hasSize(1);
  }

  @Test
  void shouldParsePipelineFormWithPipelineOnly() {
    var doc =
        Document.parse(
            """
            {
                "from": "inventory",
                "pipeline": [
                    { "$match": { "instock": true } }
                ],
                "as": "items"
            }
            """);

    LookupStage stage = parser.parse(doc);

    assertThat(stage.getFrom()).isEqualTo("inventory");
    assertThat(stage.getAs()).isEqualTo("items");
    assertThat(stage.getLetVariables()).isEmpty();
    assertThat(stage.getPipeline()).hasSize(1);
  }

  @Test
  void shouldParsePipelineFormWithLetOnly() {
    var doc =
        Document.parse(
            """
            {
                "from": "orders",
                "let": { "cust_id": "$customerId" },
                "as": "customer_orders"
            }
            """);

    LookupStage stage = parser.parse(doc);

    assertThat(stage.getFrom()).isEqualTo("orders");
    assertThat(stage.getAs()).isEqualTo("customer_orders");
    assertThat(stage.getLetVariables()).containsEntry("cust_id", "customerId");
    assertThat(stage.getPipeline()).isEmpty();
  }

  @Test
  void shouldThrowOnInvalidLetVariable() {
    var doc =
        Document.parse(
            """
            {
                "from": "warehouses",
                "let": { "invalid_var": "not_a_field_ref" },
                "pipeline": [],
                "as": "stockdata"
            }
            """);

    assertThatIllegalArgumentException()
        .isThrownBy(() -> parser.parse(doc))
        .withMessageContaining("invalid_var")
        .withMessageContaining("$");
  }

  @Test
  void shouldParsePipelineFormWithEmptyPipeline() {
    var doc =
        Document.parse(
            """
            {
                "from": "inventory",
                "pipeline": [],
                "as": "items"
            }
            """);

    LookupStage stage = parser.parse(doc);

    assertThat(stage.getFrom()).isEqualTo("inventory");
    assertThat(stage.getPipeline()).isEmpty();
  }

  @Test
  void shouldParsePipelineFormWithMultipleStages() {
    var doc =
        Document.parse(
            """
            {
                "from": "products",
                "pipeline": [
                    { "$match": { "active": true } },
                    { "$project": { "name": 1, "price": 1 } },
                    { "$sort": { "price": 1 } }
                ],
                "as": "product_details"
            }
            """);

    LookupStage stage = parser.parse(doc);

    assertThat(stage.getPipeline()).hasSize(3);
  }
}
