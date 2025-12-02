/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;

class LookupSizeExpressionTest {

  private DefaultSqlGenerationContext createContext() {
    return new DefaultSqlGenerationContext();
  }

  private DefaultSqlGenerationContext createContextWithAlias(String alias) {
    return new DefaultSqlGenerationContext(false, null, alias);
  }

  @Test
  void shouldRenderCorrelatedSubqueryForLookupSize() {
    var context = createContext();
    var expr = new LookupSizeExpression("inventory", "_id", "productId");

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM inventory WHERE JSON_VALUE(inventory.data, '$.productId')"
                + " = JSON_VALUE(data, '$._id'))");
  }

  @Test
  void shouldRenderWithBaseTableAlias() {
    var context = createContextWithAlias("base");
    var expr = new LookupSizeExpression("orders", "customerId", "custId");

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM orders WHERE JSON_VALUE(orders.data, '$.custId')"
                + " = JSON_VALUE(base.data, '$.customerId'))");
  }

  @Test
  void shouldRenderWithDifferentTableAndFieldNames() {
    var context = createContext();
    var expr = new LookupSizeExpression("line_items", "orderId", "order_id");

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM line_items WHERE JSON_VALUE(line_items.data, '$.order_id')"
                + " = JSON_VALUE(data, '$.orderId'))");
  }

  @Test
  void shouldRenderWithNestedFieldPaths() {
    var context = createContext();
    var expr = new LookupSizeExpression("reviews", "product.id", "item.productRef");

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM reviews WHERE JSON_VALUE(reviews.data, '$.item.productRef')"
                + " = JSON_VALUE(data, '$.product.id'))");
  }

  @Test
  void shouldThrowOnNullForeignTable() {
    assertThatThrownBy(() -> new LookupSizeExpression(null, "localField", "foreignField"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("foreignTable must not be null");
  }

  @Test
  void shouldThrowOnNullLocalField() {
    assertThatThrownBy(() -> new LookupSizeExpression("table", null, "foreignField"))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("localField must not be null");
  }

  @Test
  void shouldThrowOnNullForeignField() {
    assertThatThrownBy(() -> new LookupSizeExpression("table", "localField", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("foreignField must not be null");
  }

  @Test
  void shouldProduceDescriptiveToString() {
    var expr = new LookupSizeExpression("inventory", "_id", "productId");

    assertThat(expr.toString()).isEqualTo("LookupSize(inventory.productId = _id)");
  }

  @Test
  void shouldRenderWithEmptyBaseTableAlias() {
    var context = createContextWithAlias("");
    var expr = new LookupSizeExpression("products", "sku", "itemSku");

    expr.render(context);

    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM products WHERE JSON_VALUE(products.data, '$.itemSku')"
                + " = JSON_VALUE(data, '$.sku'))");
  }
}
