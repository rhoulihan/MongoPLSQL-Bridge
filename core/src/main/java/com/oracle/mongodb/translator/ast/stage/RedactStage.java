/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a $redact stage that restricts content of documents.
 *
 * <p>The $redact stage uses a conditional expression to determine if a document
 * or embedded document should be included in the output. The expression must
 * evaluate to one of three system variables:
 * <ul>
 *   <li>$$DESCEND - keep this document and descend into embedded documents</li>
 *   <li>$$PRUNE - exclude this document/subdocument</li>
 *   <li>$$KEEP - keep this document but do not descend</li>
 * </ul>
 *
 * <p>In Oracle SQL, this is translated to conditional filtering using CASE WHEN.
 * Note that full recursive document-level redaction is complex to implement
 * in SQL, so this provides a simplified implementation for document-level filtering.
 *
 * <p>Example:
 * <pre>
 * {$redact: {$cond: {if: {$eq: ["$level", 5]}, then: "$$PRUNE", else: "$$DESCEND"}}}
 * </pre>
 * translates to filtering based on the condition.
 */
public final class RedactStage implements Stage {

    private final Expression expression;

    /**
     * Creates a redact stage.
     *
     * @param expression the redact expression that evaluates to $$DESCEND, $$PRUNE, or $$KEEP
     */
    public RedactStage(Expression expression) {
        this.expression = Objects.requireNonNull(expression, "expression must not be null");
    }

    /**
     * Returns the redact expression.
     */
    public Expression getExpression() {
        return expression;
    }

    @Override
    public String getOperatorName() {
        return "$redact";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // $redact is complex in its full form (recursive document processing)
        // For SQL, we simplify to document-level filtering
        // The expression is typically a $cond that evaluates to $$PRUNE, $$KEEP, or $$DESCEND
        // We render this as a CASE WHEN expression that filters rows

        ctx.sql("/* $redact */ WHERE CASE WHEN ");
        ctx.visit(expression);
        ctx.sql(" = '$$PRUNE' THEN 0 ELSE 1 END = 1");
    }

    @Override
    public String toString() {
        return "RedactStage(" + expression + ")";
    }
}
