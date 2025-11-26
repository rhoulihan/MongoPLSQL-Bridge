/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.generator.dialect.OracleDialect;
import java.util.List;

/**
 * Context for SQL generation. AST nodes use this interface to build SQL.
 */
public interface SqlGenerationContext {

    /**
     * Appends a raw SQL fragment.
     */
    void sql(String fragment);

    /**
     * Recursively renders a child AST node.
     */
    void visit(AstNode node);

    /**
     * Adds a bind variable and appends the placeholder.
     */
    void bind(Object value);

    /**
     * Appends an identifier, quoting if necessary.
     */
    void identifier(String name);

    /**
     * Returns whether values should be inlined (for debugging).
     */
    boolean inline();

    /**
     * Returns the target Oracle dialect.
     */
    OracleDialect dialect();

    /**
     * Returns the generated SQL string.
     */
    String toSql();

    /**
     * Returns the collected bind variables.
     */
    List<Object> getBindVariables();
}
