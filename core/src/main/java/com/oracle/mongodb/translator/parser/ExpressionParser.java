/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.ArithmeticOp;
import com.oracle.mongodb.translator.ast.expression.ArrayExpression;
import com.oracle.mongodb.translator.ast.expression.ArrayOp;
import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.ConditionalExpression;
import com.oracle.mongodb.translator.ast.expression.DateExpression;
import com.oracle.mongodb.translator.ast.expression.DateOp;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.InExpression;
import com.oracle.mongodb.translator.ast.expression.JsonReturnType;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.expression.StringExpression;
import com.oracle.mongodb.translator.ast.expression.StringOp;
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

    private static final Set<String> ARITHMETIC_OPS = Set.of(
        "$add", "$subtract", "$multiply", "$divide", "$mod"
    );

    private static final Set<String> CONDITIONAL_OPS = Set.of(
        "$cond", "$ifNull"
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

    /**
     * Parses a value that can be used in $project, $addFields, or $set stages.
     * The value can be:
     * <ul>
     *   <li>A string starting with $ - field reference</li>
     *   <li>A literal value (string, number, boolean, null)</li>
     *   <li>A document containing an expression operator</li>
     * </ul>
     *
     * @param value the value to parse
     * @return the parsed Expression
     */
    public Expression parseValue(Object value) {
        if (value == null) {
            return LiteralExpression.ofNull();
        }

        if (value instanceof String str) {
            if (str.startsWith("$")) {
                // Field reference: "$fieldName" or "$nested.field"
                return FieldPathExpression.of(str.substring(1));
            }
            return LiteralExpression.of(str);
        }

        if (value instanceof Number || value instanceof Boolean) {
            return LiteralExpression.of(value);
        }

        if (value instanceof Document doc) {
            return parseExpressionDocument(doc);
        }

        if (value instanceof List) {
            // Arrays are treated as literal arrays
            return LiteralExpression.of(value);
        }

        throw new IllegalArgumentException(
            "Unsupported value type: " + value.getClass().getSimpleName());
    }

    /**
     * Parses an expression document like {$add: [...]} or {$cond: {...}}.
     */
    private Expression parseExpressionDocument(Document doc) {
        if (doc.isEmpty()) {
            throw new IllegalArgumentException("Empty expression document");
        }

        // Get the first (and usually only) key
        String op = doc.keySet().iterator().next();
        Object operand = doc.get(op);

        if (ARITHMETIC_OPS.contains(op)) {
            return parseArithmeticExpression(op, operand);
        }

        if (CONDITIONAL_OPS.contains(op)) {
            return parseConditionalExpression(op, operand);
        }

        if (StringOp.isStringOp(op)) {
            return parseStringExpression(op, operand);
        }

        if (DateOp.isDateOp(op)) {
            return parseDateExpression(op, operand);
        }

        if (ArrayOp.isArrayOp(op)) {
            return parseArrayExpression(op, operand);
        }

        // For other operators, throw unsupported
        throw new UnsupportedOperatorException(op);
    }

    private Expression parseStringExpression(String op, Object operand) {
        StringOp stringOp = StringOp.fromMongo(op);

        // Single argument operators (toLower, toUpper, trim, strlen)
        if (stringOp == StringOp.TO_LOWER || stringOp == StringOp.TO_UPPER
            || stringOp == StringOp.TRIM || stringOp == StringOp.LTRIM
            || stringOp == StringOp.RTRIM || stringOp == StringOp.STRLEN) {
            return new StringExpression(stringOp, List.of(parseValue(operand)));
        }

        // Array argument operators (concat, substr)
        if (!(operand instanceof List)) {
            throw new IllegalArgumentException(op + " requires an array of arguments");
        }

        @SuppressWarnings("unchecked")
        List<Object> args = (List<Object>) operand;

        List<Expression> expressions = new ArrayList<>();
        for (Object arg : args) {
            expressions.add(parseValue(arg));
        }

        return new StringExpression(stringOp, expressions);
    }

    private Expression parseArithmeticExpression(String op, Object operand) {
        ArithmeticOp arithmeticOp = ArithmeticOp.fromMongo(op);

        if (!(operand instanceof List)) {
            throw new IllegalArgumentException(op + " requires an array of operands");
        }

        @SuppressWarnings("unchecked")
        List<Object> operands = (List<Object>) operand;

        if (operands.size() < 2) {
            throw new IllegalArgumentException(op + " requires at least 2 operands");
        }

        List<Expression> expressions = new ArrayList<>();
        for (Object o : operands) {
            expressions.add(parseValue(o));
        }

        return new ArithmeticExpression(arithmeticOp, expressions);
    }

    private Expression parseConditionalExpression(String op, Object operand) {
        if ("$cond".equals(op)) {
            return parseCondExpression(operand);
        } else if ("$ifNull".equals(op)) {
            return parseIfNullExpression(operand);
        }
        throw new UnsupportedOperatorException(op);
    }

    private Expression parseCondExpression(Object operand) {
        if (operand instanceof List) {
            // Array form: [$cond: [condition, thenExpr, elseExpr]]
            @SuppressWarnings("unchecked")
            List<Object> args = (List<Object>) operand;
            if (args.size() != 3) {
                throw new IllegalArgumentException("$cond array form requires exactly 3 elements");
            }
            return ConditionalExpression.cond(
                parseValue(args.get(0)),
                parseValue(args.get(1)),
                parseValue(args.get(2))
            );
        } else if (operand instanceof Document doc) {
            // Document form: {$cond: {if: condition, then: thenExpr, else: elseExpr}}
            Object ifExpr = doc.get("if");
            Object thenExpr = doc.get("then");
            Object elseExpr = doc.get("else");

            if (ifExpr == null || thenExpr == null || elseExpr == null) {
                throw new IllegalArgumentException(
                    "$cond document form requires 'if', 'then', and 'else' fields");
            }

            return ConditionalExpression.cond(
                parseValue(ifExpr),
                parseValue(thenExpr),
                parseValue(elseExpr)
            );
        }

        throw new IllegalArgumentException("$cond requires an array or document");
    }

    private Expression parseIfNullExpression(Object operand) {
        if (!(operand instanceof List)) {
            throw new IllegalArgumentException("$ifNull requires an array");
        }

        @SuppressWarnings("unchecked")
        List<Object> args = (List<Object>) operand;
        if (args.size() != 2) {
            throw new IllegalArgumentException("$ifNull requires exactly 2 elements");
        }

        return ConditionalExpression.ifNull(
            parseValue(args.get(0)),
            parseValue(args.get(1))
        );
    }

    private Expression parseDateExpression(String op, Object operand) {
        DateOp dateOp = DateOp.fromMongo(op);
        // Date operators take a single expression argument
        return new DateExpression(dateOp, parseValue(operand));
    }

    private Expression parseArrayExpression(String op, Object operand) {
        ArrayOp arrayOp = ArrayOp.fromMongo(op);

        switch (arrayOp) {
            case ARRAY_ELEM_AT -> {
                // $arrayElemAt: [arrayExpr, indexExpr]
                if (!(operand instanceof List)) {
                    throw new IllegalArgumentException("$arrayElemAt requires an array of [array, index]");
                }
                @SuppressWarnings("unchecked")
                List<Object> args = (List<Object>) operand;
                if (args.size() != 2) {
                    throw new IllegalArgumentException("$arrayElemAt requires exactly 2 arguments");
                }
                return ArrayExpression.arrayElemAt(parseValue(args.get(0)), parseValue(args.get(1)));
            }
            case SIZE -> {
                // $size: arrayExpr
                return ArrayExpression.size(parseValue(operand));
            }
            case FIRST -> {
                // $first: arrayExpr
                return ArrayExpression.first(parseValue(operand));
            }
            case LAST -> {
                // $last: arrayExpr
                return ArrayExpression.last(parseValue(operand));
            }
            default -> throw new UnsupportedOperatorException(op);
        }
    }
}
