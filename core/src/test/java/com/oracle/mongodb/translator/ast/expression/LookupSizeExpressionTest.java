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

    // Uses dot notation with alias and quoted field names for type preservation
    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM inventory sz_inv WHERE sz_inv.data.\"productId\""
                + " = data.\"_id\")");
  }

  @Test
  void shouldRenderWithBaseTableAlias() {
    var context = createContextWithAlias("base");
    var expr = new LookupSizeExpression("orders", "customerId", "custId");

    expr.render(context);

    // Uses dot notation with alias and quoted field names for type preservation
    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM orders sz_ord WHERE sz_ord.data.\"custId\""
                + " = base.data.\"customerId\")");
  }

  @Test
  void shouldRenderWithDifferentTableAndFieldNames() {
    var context = createContext();
    var expr = new LookupSizeExpression("line_items", "orderId", "order_id");

    expr.render(context);

    // Uses dot notation with alias and quoted field names for type preservation
    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM line_items sz_lin WHERE sz_lin.data.\"order_id\""
                + " = data.\"orderId\")");
  }

  @Test
  void shouldRenderWithNestedFieldPaths() {
    var context = createContext();
    var expr = new LookupSizeExpression("reviews", "product.id", "item.productRef");

    expr.render(context);

    // Uses dot notation with alias and quoted field names for type preservation
    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM reviews sz_rev WHERE sz_rev.data.\"item\".\"productRef\""
                + " = data.\"product\".\"id\")");
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

    // Uses dot notation with alias and quoted field names for type preservation
    assertThat(context.toSql())
        .isEqualTo(
            "(SELECT COUNT(*) FROM products sz_pro WHERE sz_pro.data.\"itemSku\""
                + " = data.\"sku\")");
  }
}
