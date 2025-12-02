/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DefaultSqlGenerationContextTest {

  private DefaultSqlGenerationContext context;

  @BeforeEach
  void setUp() {
    context = new DefaultSqlGenerationContext();
  }

  @Test
  void shouldAppendSqlFragments() {
    context.sql("SELECT ");
    context.sql("* ");
    context.sql("FROM table_name");

    assertThat(context.toSql()).isEqualTo("SELECT * FROM table_name");
  }

  @Test
  void shouldCollectBindVariables() {
    context.sql("WHERE status = ");
    context.bind("active");

    assertThat(context.toSql()).isEqualTo("WHERE status = :1");
    assertThat(context.getBindVariables()).containsExactly("active");
  }

  @Test
  void shouldNumberBindVariablesSequentially() {
    context.sql("WHERE status = ");
    context.bind("active");
    context.sql(" AND age > ");
    context.bind(21);

    assertThat(context.toSql()).isEqualTo("WHERE status = :1 AND age > :2");
    assertThat(context.getBindVariables()).containsExactly("active", 21);
  }

  @Test
  void shouldQuoteIdentifiersWithSpecialCharacters() {
    context.sql("SELECT ");
    context.identifier("user-name");
    context.sql(" FROM ");
    context.identifier("my-table");

    assertThat(context.toSql()).isEqualTo("SELECT \"user-name\" FROM \"my-table\"");
  }

  @Test
  void shouldNotQuoteSimpleIdentifiers() {
    context.sql("SELECT ");
    context.identifier("username");

    assertThat(context.toSql()).isEqualTo("SELECT username");
  }

  @Test
  void shouldInlineValuesWhenConfigured() {
    var inlineContext = new DefaultSqlGenerationContext(true, null);

    inlineContext.sql("WHERE status = ");
    inlineContext.bind("active");
    inlineContext.sql(" AND age > ");
    inlineContext.bind(21);

    assertThat(inlineContext.toSql()).isEqualTo("WHERE status = 'active' AND age > 21");
    assertThat(inlineContext.getBindVariables()).isEmpty();
  }

  @Test
  void shouldEscapeSingleQuotesWhenInlining() {
    var inlineContext = new DefaultSqlGenerationContext(true, null);

    inlineContext.sql("WHERE name = ");
    inlineContext.bind("O'Brien");

    assertThat(inlineContext.toSql()).isEqualTo("WHERE name = 'O''Brien'");
  }

  @Test
  void shouldHandleNullWhenInlining() {
    var inlineContext = new DefaultSqlGenerationContext(true, null);

    inlineContext.sql("WHERE status = ");
    inlineContext.bind(null);

    assertThat(inlineContext.toSql()).isEqualTo("WHERE status = NULL");
  }

  @Test
  void shouldHandleNumberWhenInlining() {
    var inlineContext = new DefaultSqlGenerationContext(true, null);

    inlineContext.sql("WHERE count = ");
    inlineContext.bind(42);

    assertThat(inlineContext.toSql()).isEqualTo("WHERE count = 42");
  }

  @Test
  void shouldHandleDoubleWhenInlining() {
    var inlineContext = new DefaultSqlGenerationContext(true, null);

    inlineContext.sql("WHERE price = ");
    inlineContext.bind(99.99);

    assertThat(inlineContext.toSql()).isEqualTo("WHERE price = 99.99");
  }

  @Test
  void shouldHandleBooleanWhenInlining() {
    var inlineContext = new DefaultSqlGenerationContext(true, null);

    inlineContext.sql("WHERE active = ");
    inlineContext.bind(true);

    assertThat(inlineContext.toSql()).isEqualTo("WHERE active = true");
  }

  @Test
  void shouldHandleFalseBooleanWhenInlining() {
    var inlineContext = new DefaultSqlGenerationContext(true, null);

    inlineContext.sql("WHERE active = ");
    inlineContext.bind(false);

    assertThat(inlineContext.toSql()).isEqualTo("WHERE active = false");
  }

  @Test
  void shouldQuoteIdentifiersWithDigitPrefix() {
    context.sql("SELECT ");
    context.identifier("1column");

    assertThat(context.toSql()).isEqualTo("SELECT \"1column\"");
  }

  @Test
  void shouldQuoteReservedWords() {
    context.sql("SELECT ");
    context.identifier("SELECT");

    // Reserved words are quoted
    assertThat(context.toSql()).isEqualTo("SELECT \"SELECT\"");
  }

  @Test
  void shouldProvideDefaultBaseTableAlias() {
    // Default context has null base alias (no table alias needed)
    assertThat(context.getBaseTableAlias()).isNull();
  }

  @Test
  void shouldUseCustomBaseTableAlias() {
    var customContext = new DefaultSqlGenerationContext(false, null, "custom");

    assertThat(customContext.getBaseTableAlias()).isEqualTo("custom");
  }

  @Test
  void shouldReturnEmptyBindVariablesInitially() {
    assertThat(context.getBindVariables()).isEmpty();
  }

  @Test
  void shouldStartWithEmptySql() {
    assertThat(context.toSql()).isEmpty();
  }

  @Test
  void shouldQuoteIdentifiersWithSpaces() {
    context.sql("SELECT ");
    context.identifier("column name");

    assertThat(context.toSql()).isEqualTo("SELECT \"column name\"");
  }

  @Test
  void shouldNotQuoteIdentifiersWithUnderscores() {
    context.sql("SELECT ");
    context.identifier("column_name");

    assertThat(context.toSql()).isEqualTo("SELECT column_name");
  }

  @Test
  void shouldReturnInlineValueSetting() {
    // Default context should not be inlined
    assertThat(context.inline()).isFalse();

    // Inline context should return true
    var inlineContext = new DefaultSqlGenerationContext(true);
    assertThat(inlineContext.inline()).isTrue();
  }

  @Test
  void shouldReturnDialect() {
    // Default context has Oracle 26ai dialect
    assertThat(context.dialect()).isNotNull();

    // Context with custom dialect should return it - create a simple mock dialect
    var mockDialect =
        new com.oracle.mongodb.translator.generator.dialect.OracleDialect() {
          @Override
          public String name() {
            return "mock";
          }

          @Override
          public boolean supportsJsonValueReturning() {
            return true;
          }

          @Override
          public boolean supportsNestedPath() {
            return true;
          }

          @Override
          public boolean supportsJsonCollectionTables() {
            return false;
          }
        };
    var dialectContext = new DefaultSqlGenerationContext(false, mockDialect);
    assertThat(dialectContext.dialect()).isNotNull();
    assertThat(dialectContext.dialect().name()).isEqualTo("mock");
  }

  @Test
  void shouldRegisterAndResolveLookupField() {
    context.registerLookupField("inventory", "products", "productId", "_id");

    var sizeExpr = context.getLookupSizeExpression("inventory");

    assertThat(sizeExpr).isNotNull();
    assertThat(sizeExpr)
        .isInstanceOf(com.oracle.mongodb.translator.ast.expression.LookupSizeExpression.class);
    assertThat(context.isLookupConsumedBySize("inventory")).isTrue();
  }

  @Test
  void shouldReturnNullForUnknownLookupField() {
    var sizeExpr = context.getLookupSizeExpression("unknown");

    assertThat(sizeExpr).isNull();
  }

  @Test
  void shouldGenerateUniqueTableAliases() {
    String alias1 = context.generateTableAlias("orders");
    String alias2 = context.generateTableAlias("orders");
    String alias3 = context.generateTableAlias("customers");

    assertThat(alias1).isEqualTo("orders_1");
    assertThat(alias2).isEqualTo("orders_2");
    assertThat(alias3).isEqualTo("customers_1");
  }

  @Test
  void shouldCreateNestedContextWithInheritedState() {
    context.registerVirtualField(
        "total", com.oracle.mongodb.translator.ast.expression.LiteralExpression.of(100));
    context.registerLookupTableAlias("customer", "cust_1");

    var nestedContext = context.createNestedContext();

    assertThat(nestedContext.getVirtualField("total")).isNotNull();
    assertThat(nestedContext.getLookupTableAlias("customer")).isEqualTo("cust_1");
    assertThat(nestedContext.getBaseTableAlias()).isNull();
  }

  @Test
  void shouldRegisterAndRetrieveLookupTableAlias() {
    context.registerLookupTableAlias("product", "prod_1");

    // Should find by exact match
    assertThat(context.getLookupTableAliasByAs("product")).isEqualTo("prod_1");
    // Should find by path prefix
    assertThat(context.getLookupTableAlias("product.name")).isEqualTo("prod_1");
    assertThat(context.getLookupTableAlias("product")).isEqualTo("prod_1");
    // Should not find unregistered
    assertThat(context.getLookupTableAlias("unknown")).isNull();
  }

  @Test
  void shouldFormatInlineValueForNonStandardTypes() {
    var inlineContext = new DefaultSqlGenerationContext(true);

    // Test with a non-standard type (like java.util.Date)
    var date = new java.util.Date(0);
    inlineContext.sql("WHERE date = ");
    inlineContext.bind(date);

    // Non-standard types are converted via toString and quoted
    assertThat(inlineContext.toSql()).contains("'");
  }

  @Test
  void shouldHandleJsonFieldFormatting() {
    // JSON fields are validated and appended as-is
    context.jsonField("user_name");

    assertThat(context.toSql()).isEqualTo("user_name");
  }

  @Test
  void shouldHandleTableNameWithValidation() {
    context.tableName("my_table");

    assertThat(context.toSql()).isEqualTo("my_table");
  }

  @Test
  void shouldQuoteTableNameWithDollarPrefix() {
    // Dollar prefix identifiers get quoted
    context.identifier("$items");

    assertThat(context.toSql()).isEqualTo("\"$items\"");
  }

  @Test
  void shouldVisitAstNode() {
    var literal = com.oracle.mongodb.translator.ast.expression.LiteralExpression.of("test");

    context.visit(literal);

    // Should render the literal
    assertThat(context.toSql()).contains(":1").satisfies(s -> {
      assertThat(context.getBindVariables()).containsExactly("test");
    });
  }

  @Test
  void shouldRegisterAndRetrieveVirtualField() {
    var expr = com.oracle.mongodb.translator.ast.expression.LiteralExpression.of(42);
    context.registerVirtualField("computedField", expr);

    var retrieved = context.getVirtualField("computedField");

    assertThat(retrieved).isEqualTo(expr);
  }

  @Test
  void shouldReturnNullForUnknownVirtualField() {
    var result = context.getVirtualField("unknown");

    assertThat(result).isNull();
  }

  @Test
  void shouldTrackLookupConsumedBySize() {
    // Initially not consumed
    assertThat(context.isLookupConsumedBySize("items")).isFalse();

    // Register and get size expression to mark as consumed
    context.registerLookupField("items", "inventory", "itemId", "_id");
    context.getLookupSizeExpression("items");

    // Now should be consumed
    assertThat(context.isLookupConsumedBySize("items")).isTrue();
  }

  @Test
  void shouldNotTrackAsConsumedWithoutGettingSizeExpression() {
    context.registerLookupField("items", "inventory", "itemId", "_id");

    // Not consumed until getLookupSizeExpression is called
    assertThat(context.isLookupConsumedBySize("items")).isFalse();
  }
}
