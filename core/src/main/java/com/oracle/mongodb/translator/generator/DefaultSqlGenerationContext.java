/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.generator.dialect.Oracle26aiDialect;
import com.oracle.mongodb.translator.generator.dialect.OracleDialect;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Default implementation of SqlGenerationContext.
 */
public class DefaultSqlGenerationContext implements SqlGenerationContext {

    private static final Pattern SIMPLE_IDENTIFIER =
        Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    private final StringBuilder sql = new StringBuilder();
    private final List<Object> bindVariables = new ArrayList<>();
    private final boolean inlineValues;
    private final OracleDialect dialect;

    public DefaultSqlGenerationContext() {
        this(false, Oracle26aiDialect.INSTANCE);
    }

    public DefaultSqlGenerationContext(boolean inlineValues, OracleDialect dialect) {
        this.inlineValues = inlineValues;
        this.dialect = dialect != null ? dialect : Oracle26aiDialect.INSTANCE;
    }

    @Override
    public void sql(String fragment) {
        sql.append(fragment);
    }

    @Override
    public void visit(AstNode node) {
        node.render(this);
    }

    @Override
    public void bind(Object value) {
        if (inlineValues) {
            sql.append(formatInlineValue(value));
        } else {
            bindVariables.add(value);
            sql.append(":").append(bindVariables.size());
        }
    }

    @Override
    public void identifier(String name) {
        if (SIMPLE_IDENTIFIER.matcher(name).matches()) {
            sql.append(name);
        } else {
            sql.append("\"").append(name).append("\"");
        }
    }

    @Override
    public boolean inline() {
        return inlineValues;
    }

    @Override
    public OracleDialect dialect() {
        return dialect;
    }

    @Override
    public String toSql() {
        return sql.toString();
    }

    @Override
    public List<Object> getBindVariables() {
        return List.copyOf(bindVariables);
    }

    private String formatInlineValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String str) {
            return "'" + str.replace("'", "''") + "'";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "'" + value.toString().replace("'", "''") + "'";
    }
}
