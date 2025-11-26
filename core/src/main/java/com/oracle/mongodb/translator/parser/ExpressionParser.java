/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.InExpression;
import com.oracle.mongodb.translator.ast.expression.JsonReturnType;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses MongoDB filter expressions into AST Expression nodes.
 */
public final class ExpressionParser {

    private static final Set<String> COMPARISON_OPS = Set.of(
        "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin"
    );

    private static final Set<String> LOGICAL_OPS = Set.of(
        "$and", "$or", "$not", "$nor"
    );

    /**
     * Parses a filter document into an Expression.
     */
    public Expression parse(Document filter) {
        return parseDocument(filter);
    }

    private Expression parseDocument(Document doc) {
        List<Expression> conditions = new ArrayList<>();

        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (key.startsWith("$")) {
                // Logical operator at top level
                conditions.add(parseTopLevelOperator(key, value));
            } else {
                // Field condition
                conditions.add(parseFieldCondition(key, value));
            }
        }

        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("Empty filter document");
        }

        if (conditions.size() == 1) {
            return conditions.get(0);
        }

        // Multiple conditions are implicitly ANDed
        return new LogicalExpression(LogicalOp.AND, conditions);
    }

    private Expression parseTopLevelOperator(String op, Object value) {
        if (LOGICAL_OPS.contains(op)) {
            return parseLogicalOperator(op, value);
        }
        throw new UnsupportedOperatorException(op);
    }

    private Expression parseLogicalOperator(String op, Object value) {
        LogicalOp logicalOp = LogicalOp.fromMongo(op);

        if (logicalOp == LogicalOp.NOT) {
            if (!(value instanceof Document)) {
                throw new IllegalArgumentException("$not requires a document value");
            }
            return new LogicalExpression(logicalOp, List.of(parseDocument((Document) value)));
        }

        if (!(value instanceof List)) {
            throw new IllegalArgumentException(op + " requires an array value");
        }

        @SuppressWarnings("unchecked")
        List<Document> docs = (List<Document>) value;
        List<Expression> operands = new ArrayList<>();

        for (Document doc : docs) {
            operands.add(parseDocument(doc));
        }

        return new LogicalExpression(logicalOp, operands);
    }

    private Expression parseFieldCondition(String fieldPath, Object value) {
        if (value instanceof Document) {
            return parseFieldOperators(fieldPath, (Document) value);
        }

        // Simple equality: {"status": "active"} or {"deletedAt": null}
        if (value == null) {
            return new ComparisonExpression(
                ComparisonOp.EQ,
                createFieldPath(fieldPath, null),
                LiteralExpression.ofNull()
            );
        }

        return new ComparisonExpression(
            ComparisonOp.EQ,
            createFieldPath(fieldPath, value),
            LiteralExpression.of(value)
        );
    }

    private Expression parseFieldOperators(String fieldPath, Document operators) {
        List<Expression> conditions = new ArrayList<>();

        for (Map.Entry<String, Object> entry : operators.entrySet()) {
            String op = entry.getKey();
            Object value = entry.getValue();

            if (COMPARISON_OPS.contains(op)) {
                conditions.add(parseComparisonOperator(fieldPath, op, value));
            } else if (op.equals("$not")) {
                conditions.add(parseNotOperator(fieldPath, value));
            } else {
                throw new UnsupportedOperatorException(op);
            }
        }

        if (conditions.size() == 1) {
            return conditions.get(0);
        }

        return new LogicalExpression(LogicalOp.AND, conditions);
    }

    private Expression parseComparisonOperator(String fieldPath, String op, Object value) {
        ComparisonOp comparisonOp = ComparisonOp.fromMongo(op);

        if (comparisonOp == ComparisonOp.IN || comparisonOp == ComparisonOp.NIN) {
            return parseInOperator(fieldPath, comparisonOp, value);
        }

        return new ComparisonExpression(
            comparisonOp,
            createFieldPath(fieldPath, value),
            LiteralExpression.of(value)
        );
    }

    private Expression parseInOperator(String fieldPath, ComparisonOp op, Object value) {
        if (!(value instanceof List)) {
            throw new IllegalArgumentException(op.getMongoOperator() + " requires an array");
        }

        @SuppressWarnings("unchecked")
        List<Object> values = (List<Object>) value;

        // Create IN expression with array of values
        return new InExpression(
            createFieldPath(fieldPath, values.isEmpty() ? null : values.get(0)),
            values,
            op == ComparisonOp.NIN
        );
    }

    private Expression parseNotOperator(String fieldPath, Object value) {
        if (!(value instanceof Document)) {
            throw new IllegalArgumentException("$not requires a document value");
        }

        Expression inner = parseFieldOperators(fieldPath, (Document) value);
        return new LogicalExpression(LogicalOp.NOT, List.of(inner));
    }

    private FieldPathExpression createFieldPath(String path, Object sampleValue) {
        JsonReturnType returnType = inferReturnType(sampleValue);
        return FieldPathExpression.of(path, returnType);
    }

    private JsonReturnType inferReturnType(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return JsonReturnType.NUMBER;
        }
        if (value instanceof Boolean) {
            return null; // Let Oracle handle boolean comparison
        }
        // Default: string comparison (no RETURNING clause needed)
        return null;
    }
}
