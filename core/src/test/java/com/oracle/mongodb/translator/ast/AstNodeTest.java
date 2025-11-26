/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;

class AstNodeTest {

    @Test
    void shouldRenderSimpleNode() {
        AstNode node = ctx -> ctx.sql("SELECT 1 FROM DUAL");

        var context = new DefaultSqlGenerationContext();
        node.render(context);

        assertThat(context.toSql()).isEqualTo("SELECT 1 FROM DUAL");
    }

    @Test
    void shouldSupportNestedRendering() {
        AstNode inner = ctx -> ctx.sql("42");
        AstNode outer = ctx -> {
            ctx.sql("SELECT ");
            ctx.visit(inner);
            ctx.sql(" FROM DUAL");
        };

        var context = new DefaultSqlGenerationContext();
        outer.render(context);

        assertThat(context.toSql()).isEqualTo("SELECT 42 FROM DUAL");
    }
}
