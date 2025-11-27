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
import com.oracle.mongodb.translator.ast.expression.TypeConversionExpression;
import com.oracle.mongodb.translator.ast.expression.TypeConversionOp;
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
        "$add", "$subtract", "$multiply", "$divide", "$mod",
        "$round", "$abs", "$ceil", "$floor", "$trunc",
        "$sqrt", "$pow", "$exp", "$ln", "$log10",
        "$max", "$min"
    );

    private static final Set<String> CONDITIONAL_OPS = Set.of(
        "$cond", "$ifNull"
    );

    private static final Set<String> TYPE_CONVERSION_OPS = Set.of(
        "$type", "$toInt", "$toLong", "$toDouble", "$toDecimal",
        "$toString", "$toBool", "$toDate", "$toObjectId", "$convert", "$isNumber"
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
            // $not can take a document (filter context) or expression (expression context)
            if (value instanceof Document) {
                return new LogicalExpression(logicalOp, List.of(parseDocument((Document) value)));
            }
            // Expression context: $not: expr
            return new LogicalExpression(logicalOp, List.of(parseValue(value)));
        }

        if (!(value instanceof List)) {
            throw new IllegalArgumentException(op + " requires an array value");
        }

        @SuppressWarnings("unchecked")
        List<Object> items = (List<Object>) value;
        List<Expression> operands = new ArrayList<>();

        for (Object item : items) {
            // In expression context, items can be expressions (field refs, literals, nested expressions)
            // In filter context, items are documents
            if (item instanceof Document) {
                // Could be a filter document or an expression document
                Document doc = (Document) item;
                if (!doc.isEmpty()) {
                    String firstKey = doc.keySet().iterator().next();
                    if (firstKey.startsWith("$")) {
                        // Expression document like {$eq: [...]}
                        operands.add(parseValue(item));
                    } else {
                        // Filter document like {field: value}
                        operands.add(parseDocument(doc));
                    }
                } else {
                    operands.add(parseDocument(doc));
                }
            } else {
                // Field reference or literal
                operands.add(parseValue(item));
            }
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

        if (TYPE_CONVERSION_OPS.contains(op)) {
            return parseTypeConversionExpression(op, operand);
        }

        if (COMPARISON_OPS.contains(op)) {
            return parseComparisonExpressionValue(op, operand);
        }

        if (LOGICAL_OPS.contains(op)) {
            return parseLogicalOperator(op, operand);
        }

        // For other operators, throw unsupported
        throw new UnsupportedOperatorException(op);
    }

    private Expression parseComparisonExpressionValue(String op, Object operand) {
        // Comparison operators as expressions: {$eq: [expr1, expr2]}
        if (!(operand instanceof List)) {
            throw new IllegalArgumentException(op + " requires an array of two expressions");
        }

        @SuppressWarnings("unchecked")
        List<Object> args = (List<Object>) operand;
        if (args.size() != 2) {
            throw new IllegalArgumentException(op + " requires exactly 2 arguments");
        }

        ComparisonOp comparisonOp = ComparisonOp.fromMongo(op);
        return new ComparisonExpression(comparisonOp, parseValue(args.get(0)), parseValue(args.get(1)));
    }

    private Expression parseStringExpression(String op, Object operand) {
        StringOp stringOp = StringOp.fromMongo(op);

        // Single argument operators (toLower, toUpper, trim, strlen)
        if (stringOp == StringOp.TO_LOWER || stringOp == StringOp.TO_UPPER
            || stringOp == StringOp.TRIM || stringOp == StringOp.LTRIM
            || stringOp == StringOp.RTRIM || stringOp == StringOp.STRLEN) {
            return new StringExpression(stringOp, List.of(parseValue(operand)));
        }

        // Document argument operators (regexMatch, regexFind, replaceOne, replaceAll)
        if (stringOp == StringOp.REGEX_MATCH || stringOp == StringOp.REGEX_FIND) {
            return parseRegexExpression(stringOp, operand);
        }

        if (stringOp == StringOp.REPLACE_ONE || stringOp == StringOp.REPLACE_ALL) {
            return parseReplaceExpression(stringOp, operand);
        }

        // Array argument operators (concat, substr, split, indexOfCP)
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

    private Expression parseRegexExpression(StringOp stringOp, Object operand) {
        if (!(operand instanceof Document doc)) {
            throw new IllegalArgumentException(stringOp.getMongoOperator() + " requires a document argument");
        }

        Object input = doc.get("input");
        Object regex = doc.get("regex");
        Object options = doc.get("options");

        if (input == null || regex == null) {
            throw new IllegalArgumentException(stringOp.getMongoOperator() + " requires 'input' and 'regex' fields");
        }

        List<Expression> args = new ArrayList<>();
        args.add(parseValue(input));
        args.add(parseValue(regex));
        if (options != null) {
            args.add(parseValue(options));
        }

        return new StringExpression(stringOp, args);
    }

    private Expression parseReplaceExpression(StringOp stringOp, Object operand) {
        if (!(operand instanceof Document doc)) {
            throw new IllegalArgumentException(stringOp.getMongoOperator() + " requires a document argument");
        }

        Object input = doc.get("input");
        Object find = doc.get("find");
        Object replacement = doc.get("replacement");

        if (input == null || find == null || replacement == null) {
            throw new IllegalArgumentException(stringOp.getMongoOperator() + " requires 'input', 'find', and 'replacement' fields");
        }

        return new StringExpression(stringOp, List.of(
            parseValue(input),
            parseValue(find),
            parseValue(replacement)
        ));
    }

    private Expression parseArithmeticExpression(String op, Object operand) {
        ArithmeticOp arithmeticOp = ArithmeticOp.fromMongo(op);

        // Handle single-argument functions (e.g., $abs, $ceil, $floor, $sqrt)
        if (arithmeticOp.allowsSingleOperand() && !(operand instanceof List)) {
            return new ArithmeticExpression(arithmeticOp, List.of(parseValue(operand)));
        }

        if (!(operand instanceof List)) {
            throw new IllegalArgumentException(op + " requires an array of operands");
        }

        @SuppressWarnings("unchecked")
        List<Object> operands = (List<Object>) operand;

        if (operands.isEmpty()) {
            throw new IllegalArgumentException(op + " requires at least 1 operand");
        }

        if (operands.size() < 2 && !arithmeticOp.allowsSingleOperand()) {
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
            case CONCAT_ARRAYS -> {
                // $concatArrays: [array1, array2, ...]
                if (!(operand instanceof List)) {
                    throw new IllegalArgumentException("$concatArrays requires an array of arrays");
                }
                @SuppressWarnings("unchecked")
                List<Object> args = (List<Object>) operand;
                List<Expression> arrays = new ArrayList<>();
                for (Object arg : args) {
                    arrays.add(parseValue(arg));
                }
                return ArrayExpression.concatArrays(arrays);
            }
            case SLICE -> {
                // $slice: [array, count] or [array, skip, count]
                if (!(operand instanceof List)) {
                    throw new IllegalArgumentException("$slice requires an array");
                }
                @SuppressWarnings("unchecked")
                List<Object> args = (List<Object>) operand;
                if (args.size() == 2) {
                    return ArrayExpression.slice(parseValue(args.get(0)), parseValue(args.get(1)));
                } else if (args.size() == 3) {
                    return ArrayExpression.sliceWithSkip(parseValue(args.get(0)), parseValue(args.get(1)), parseValue(args.get(2)));
                } else {
                    throw new IllegalArgumentException("$slice requires 2 or 3 arguments");
                }
            }
            case FILTER -> {
                // $filter: {input: array, as: varName, cond: condition}
                if (!(operand instanceof Document doc)) {
                    throw new IllegalArgumentException("$filter requires a document");
                }
                Object input = doc.get("input");
                Object cond = doc.get("cond");
                if (input == null || cond == null) {
                    throw new IllegalArgumentException("$filter requires 'input' and 'cond' fields");
                }
                return ArrayExpression.filter(parseValue(input), parseValue(cond));
            }
            case MAP -> {
                // $map: {input: array, as: varName, in: expression}
                if (!(operand instanceof Document doc)) {
                    throw new IllegalArgumentException("$map requires a document");
                }
                Object input = doc.get("input");
                Object inExpr = doc.get("in");
                if (input == null || inExpr == null) {
                    throw new IllegalArgumentException("$map requires 'input' and 'in' fields");
                }
                return ArrayExpression.map(parseValue(input), parseValue(inExpr));
            }
            case REDUCE -> {
                // $reduce: {input: array, initialValue: value, in: expression}
                if (!(operand instanceof Document doc)) {
                    throw new IllegalArgumentException("$reduce requires a document");
                }
                Object input = doc.get("input");
                Object initialValue = doc.get("initialValue");
                Object inExpr = doc.get("in");
                if (input == null || initialValue == null || inExpr == null) {
                    throw new IllegalArgumentException("$reduce requires 'input', 'initialValue', and 'in' fields");
                }
                return ArrayExpression.reduce(parseValue(input), parseValue(initialValue), parseValue(inExpr));
            }
            default -> throw new UnsupportedOperatorException(op);
        }
    }

    private Expression parseTypeConversionExpression(String op, Object operand) {
        if ("$convert".equals(op)) {
            return parseConvertExpression(operand);
        }

        // All other type conversion operators take a single argument
        TypeConversionOp conversionOp = TypeConversionOp.fromMongoOperator(op);
        return new TypeConversionExpression(conversionOp, parseValue(operand));
    }

    private Expression parseConvertExpression(Object operand) {
        if (!(operand instanceof Document doc)) {
            throw new IllegalArgumentException("$convert requires a document argument");
        }

        Object input = doc.get("input");
        if (input == null) {
            throw new IllegalArgumentException("$convert requires 'input' field");
        }

        Expression inputExpr = parseValue(input);
        Expression onError = doc.containsKey("onError") ? parseValue(doc.get("onError")) : null;
        Expression onNull = doc.containsKey("onNull") ? parseValue(doc.get("onNull")) : null;

        return TypeConversionExpression.convert(inputExpr, onError, onNull);
    }
}
