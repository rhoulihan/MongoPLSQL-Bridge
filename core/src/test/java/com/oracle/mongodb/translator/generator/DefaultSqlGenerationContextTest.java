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
}
