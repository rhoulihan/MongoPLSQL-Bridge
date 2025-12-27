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
import com.oracle.mongodb.translator.ast.expression.DateArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.DateArithmeticOp;
import com.oracle.mongodb.translator.ast.expression.DateExpression;
import com.oracle.mongodb.translator.ast.expression.DateOp;
import com.oracle.mongodb.translator.ast.expression.DatePartsExpression;
import com.oracle.mongodb.translator.ast.expression.DateStringExpression;
import com.oracle.mongodb.translator.ast.expression.DateStringOp;
import com.oracle.mongodb.translator.ast.expression.DateTruncExpression;
import com.oracle.mongodb.translator.ast.expression.ExistsExpression;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.InExpression;
import com.oracle.mongodb.translator.ast.expression.InlineObjectExpression;
import com.oracle.mongodb.translator.ast.expression.JsonReturnType;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.expression.ObjectExpression;
import com.oracle.mongodb.translator.ast.expression.ObjectOp;
import com.oracle.mongodb.translator.ast.expression.StringExpression;
import com.oracle.mongodb.translator.ast.expression.StringOp;
import com.oracle.mongodb.translator.ast.expression.SwitchExpression;
import com.oracle.mongodb.translator.ast.expression.TypeConversionExpression;
import com.oracle.mongodb.translator.ast.expression.TypeConversionOp;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.bson.Document;

/** Parses MongoDB filter expressions into AST Expression nodes. */
public final class ExpressionParser {

  private static final Set<String> COMPARISON_OPS =
      Set.of("$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin");

  private static final Set<String> LOGICAL_OPS = Set.of("$and", "$or", "$not", "$nor");

  private static final Set<String> ARITHMETIC_OPS =
      Set.of(
          "$add",
          "$subtract",
          "$multiply",
          "$divide",
          "$mod",
          "$round",
          "$abs",
          "$ceil",
          "$floor",
          "$trunc",
          "$sqrt",
          "$pow",
          "$exp",
          "$ln",
          "$log10",
          "$max",
          "$min");

  private static final Set<String> CONDITIONAL_OPS = Set.of("$cond", "$ifNull", "$switch");

  private static final Set<String> TYPE_CONVERSION_OPS =
      Set.of(
          "$type",
          "$toInt",
          "$toLong",
          "$toDouble",
          "$toDecimal",
          "$toString",
          "$toBool",
          "$toDate",
          "$toObjectId",
          "$convert",
          "$isNumber",
          "$isString");

  /** Parses a filter document into an Expression. */
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
    if ("$expr".equals(op)) {
      // $expr allows aggregation expressions in query context
      // The value is an expression document like {$eq: ["$field1", "$field2"]}
      return parseValue(value);
    }
    throw new UnsupportedOperatorException(op);
  }

  private Expression parseLogicalOperator(String op, Object value) {
    LogicalOp logicalOp = LogicalOp.fromMongo(op);

    if (logicalOp == LogicalOp.NOT) {
      return parseNotOperatorAtTopLevel(logicalOp, value);
    }

    if (!(value instanceof List)) {
      throw new IllegalArgumentException(op + " requires an array value");
    }

    @SuppressWarnings("unchecked")
    List<Object> items = (List<Object>) value;
    List<Expression> operands = new ArrayList<>();

    for (Object item : items) {
      operands.add(parseLogicalOperand(item));
    }

    return new LogicalExpression(logicalOp, operands);
  }

  private Expression parseNotOperatorAtTopLevel(LogicalOp logicalOp, Object value) {
    // $not at top level (filter context) requires a document
    // Expression context handling (where $not can take expressions) is done in
    // parseExpressionOperator
    if (!(value instanceof Document)) {
      throw new IllegalArgumentException("$not requires a document operand in filter context");
    }
    return new LogicalExpression(logicalOp, List.of(parseDocument((Document) value)));
  }

  private Expression parseLogicalOperand(Object item) {
    // In expression context, items can be expressions (field refs, literals, nested expressions)
    // In filter context, items are documents
    if (!(item instanceof Document doc)) {
      // Field reference or literal
      return parseValue(item);
    }

    // Could be a filter document or an expression document
    if (doc.isEmpty()) {
      return parseDocument(doc);
    }

    String firstKey = doc.keySet().iterator().next();
    if (firstKey.startsWith("$")) {
      // Expression document like {$eq: [...]}
      return parseValue(item);
    }
    // Filter document like {field: value}
    return parseDocument(doc);
  }

  private Expression parseFieldCondition(String fieldPath, Object value) {
    if (value instanceof Document) {
      return parseFieldOperators(fieldPath, (Document) value);
    }

    // Simple equality: {"status": "active"} or {"deletedAt": null}
    if (value == null) {
      return new ComparisonExpression(
          ComparisonOp.EQ, createFieldPath(fieldPath, null), LiteralExpression.ofNull());
    }

    return new ComparisonExpression(
        ComparisonOp.EQ, createFieldPath(fieldPath, value), LiteralExpression.of(value));
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
      } else if (op.equals("$exists")) {
        conditions.add(parseExistsOperator(fieldPath, value));
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
        comparisonOp, createFieldPath(fieldPath, value), LiteralExpression.of(value));
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
        op == ComparisonOp.NIN);
  }

  private Expression parseNotOperator(String fieldPath, Object value) {
    if (!(value instanceof Document)) {
      throw new IllegalArgumentException("$not requires a document value");
    }

    Expression inner = parseFieldOperators(fieldPath, (Document) value);
    return new LogicalExpression(LogicalOp.NOT, List.of(inner));
  }

  private Expression parseExistsOperator(String fieldPath, Object value) {
    if (!(value instanceof Boolean)) {
      throw new IllegalArgumentException("$exists requires a boolean value");
    }
    return new ExistsExpression(fieldPath, (Boolean) value);
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
   * Parses a value that can be used in $project, $addFields, or $set stages. The value can be:
   *
   * <ul>
   *   <li>A string starting with $ - field reference
   *   <li>A literal value (string, number, boolean, null)
   *   <li>A document containing an expression operator
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
      if (str.startsWith("$$")) {
        // Check if it's a system variable (all uppercase after $$)
        // System variables: "$$KEEP", "$$PRUNE", "$$DESCEND", "$$ROOT", "$$NOW", etc.
        // User variables: "$$item", "$$this", "$$item.qty" - should be field paths
        String afterDollarDollar = str.substring(2);
        int dotPos = afterDollarDollar.indexOf('.');
        String varName = dotPos > 0 ? afterDollarDollar.substring(0, dotPos) : afterDollarDollar;

        // Special handling for $$ROOT - it references the root document
        if (varName.equals("ROOT")) {
          if (dotPos > 0) {
            // $$ROOT.fieldName -> treat as field path to fieldName
            String fieldPath = afterDollarDollar.substring(dotPos + 1);
            return FieldPathExpression.of(fieldPath);
          } else {
            // Just $$ROOT without field access - represents entire document
            return LiteralExpression.of(str);
          }
        }

        if (varName.equals(varName.toUpperCase())
            && varName.length() > 0
            && Character.isLetter(varName.charAt(0))) {
          // System variable like $$KEEP, $$PRUNE - keep as literal
          return LiteralExpression.of(str);
        }
        // User variable reference like $$item or $$this.field - treat as field path
        return FieldPathExpression.of(str.substring(1)); // Remove first $, keep $item.qty
      }
      if (str.startsWith("$")) {
        // Field reference: "$fieldName" or "$nested.field"
        return FieldPathExpression.of(str.substring(1));
      }
      return LiteralExpression.of(str);
    }

    if (value instanceof Number || value instanceof Boolean) {
      return LiteralExpression.of(value);
    }

    if (value instanceof java.util.Date) {
      // Date values are converted to Oracle timestamp literals
      return LiteralExpression.of(value);
    }

    if (value instanceof org.bson.types.ObjectId) {
      // ObjectId is converted to string representation
      return LiteralExpression.of(value.toString());
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

  /** Parses an expression document like {$add: [...]} or {$cond: {...}}. */
  private Expression parseExpressionDocument(Document doc) {
    if (doc.isEmpty()) {
      throw new IllegalArgumentException("Empty expression document");
    }

    // Get the first (and usually only) entry using entrySet for efficiency
    var entry = doc.entrySet().iterator().next();
    String op = entry.getKey();
    Object operand = entry.getValue();

    // If the key doesn't start with $, it's an inline object literal (e.g., {name: "value"})
    // This is used in contexts like $mergeObjects: [{field: expr}, ...]
    if (!op.startsWith("$")) {
      return parseInlineObjectLiteral(doc);
    }

    if (ARITHMETIC_OPS.contains(op)) {
      return parseArithmeticExpression(op, operand);
    }

    if (CONDITIONAL_OPS.contains(op)) {
      return parseConditionalExpression(op, operand);
    }

    if (StringOp.isStringOp(op)) {
      return parseStringExpression(op, operand);
    }

    if (DateArithmeticOp.isDateArithmeticOp(op)) {
      return parseDateArithmeticExpression(op, operand);
    }

    if (DateStringOp.isDateStringOp(op)) {
      return parseDateStringExpression(op, operand);
    }

    if ("$dateTrunc".equals(op)) {
      return parseDateTruncExpression(operand);
    }

    if ("$dateFromParts".equals(op) || "$dateToParts".equals(op)) {
      return parseDatePartsExpression(op, operand);
    }

    if (DateOp.isDateOp(op)) {
      return parseDateExpression(op, operand);
    }

    if (ArrayOp.isArrayOp(op)) {
      return parseArrayExpression(op, operand);
    }

    if (ObjectOp.isObjectOp(op)) {
      return parseObjectExpression(op, operand);
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

    if ("$literal".equals(op)) {
      // $literal returns the value exactly as-is (no field path interpretation)
      return LiteralExpression.of(operand);
    }

    // For other operators, throw unsupported
    throw new UnsupportedOperatorException(op);
  }

  /**
   * Parses an inline object literal document.
   *
   * <p>Handles documents with non-operator field names, like {name: "value", status: "$field"}.
   * These appear in contexts like $mergeObjects, $cond results, etc.
   *
   * @param doc the document containing field-value pairs
   * @return an InlineObjectExpression
   */
  private Expression parseInlineObjectLiteral(Document doc) {
    Map<String, Expression> fields = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : doc.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      fields.put(key, parseValue(value));
    }
    return new InlineObjectExpression(fields);
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

    // Single argument operators (toLower, toUpper, strlen)
    if (stringOp == StringOp.TO_LOWER
        || stringOp == StringOp.TO_UPPER
        || stringOp == StringOp.STRLEN) {
      return new StringExpression(stringOp, List.of(parseValue(operand)));
    }

    // Trim operators: {input: <string>, chars: <string>} or just <string>
    if (stringOp == StringOp.TRIM
        || stringOp == StringOp.LTRIM
        || stringOp == StringOp.RTRIM) {
      return parseTrimExpression(stringOp, operand);
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
      throw new IllegalArgumentException(
          stringOp.getMongoOperator() + " requires a document argument");
    }

    Object input = doc.get("input");
    Object regex = doc.get("regex");

    if (input == null || regex == null) {
      throw new IllegalArgumentException(
          stringOp.getMongoOperator() + " requires 'input' and 'regex' fields");
    }

    List<Expression> args = new ArrayList<>();
    args.add(parseValue(input));
    args.add(parseValue(regex));
    final Object options = doc.get("options");
    if (options != null) {
      args.add(parseValue(options));
    }

    return new StringExpression(stringOp, args);
  }

  private Expression parseReplaceExpression(StringOp stringOp, Object operand) {
    if (!(operand instanceof Document doc)) {
      throw new IllegalArgumentException(
          stringOp.getMongoOperator() + " requires a document argument");
    }

    Object input = doc.get("input");
    Object find = doc.get("find");
    Object replacement = doc.get("replacement");

    if (input == null || find == null || replacement == null) {
      throw new IllegalArgumentException(
          stringOp.getMongoOperator() + " requires 'input', 'find', and 'replacement' fields");
    }

    return new StringExpression(
        stringOp, List.of(parseValue(input), parseValue(find), parseValue(replacement)));
  }

  private Expression parseTrimExpression(StringOp stringOp, Object operand) {
    // $trim, $ltrim, $rtrim can take either:
    // 1. A document: {input: <string>, chars: <string>}
    // 2. A simple value (backward compatibility or field reference)
    if (operand instanceof Document doc) {
      Object input = doc.get("input");
      if (input == null) {
        throw new IllegalArgumentException(
            stringOp.getMongoOperator() + " requires 'input' field when using document syntax");
      }

      List<Expression> args = new ArrayList<>();
      args.add(parseValue(input));

      Object chars = doc.get("chars");
      if (chars != null) {
        args.add(parseValue(chars));
      }

      return new StringExpression(stringOp, args);
    }

    // Simple value (field reference, etc.)
    return new StringExpression(stringOp, List.of(parseValue(operand)));
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
    } else if ("$switch".equals(op)) {
      return parseSwitchExpression(operand);
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
          parseValue(args.get(0)), parseValue(args.get(1)), parseValue(args.get(2)));
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
          parseValue(ifExpr), parseValue(thenExpr), parseValue(elseExpr));
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

    return ConditionalExpression.ifNull(parseValue(args.get(0)), parseValue(args.get(1)));
  }

  private Expression parseSwitchExpression(Object operand) {
    if (!(operand instanceof Document doc)) {
      throw new IllegalArgumentException("$switch requires a document");
    }

    Object branchesObj = doc.get("branches");
    if (!(branchesObj instanceof List)) {
      throw new IllegalArgumentException("$switch requires 'branches' array");
    }

    @SuppressWarnings("unchecked")
    List<Object> branchesList = (List<Object>) branchesObj;
    List<SwitchExpression.SwitchBranch> branches = new ArrayList<>();

    for (Object branchObj : branchesList) {
      if (!(branchObj instanceof Document branchDoc)) {
        throw new IllegalArgumentException("Each branch must be a document");
      }
      Object caseExpr = branchDoc.get("case");
      Object thenExpr = branchDoc.get("then");
      if (caseExpr == null || thenExpr == null) {
        throw new IllegalArgumentException("Each branch requires 'case' and 'then' fields");
      }
      branches.add(new SwitchExpression.SwitchBranch(parseValue(caseExpr), parseValue(thenExpr)));
    }

    Expression defaultExpr = doc.containsKey("default") ? parseValue(doc.get("default")) : null;
    return SwitchExpression.of(branches, defaultExpr);
  }

  private Expression parseDateExpression(String op, Object operand) {
    DateOp dateOp = DateOp.fromMongo(op);
    // Date operators take a single expression argument
    return new DateExpression(dateOp, parseValue(operand));
  }

  private Expression parseDateArithmeticExpression(String op, Object operand) {
    if (!(operand instanceof Document doc)) {
      throw new IllegalArgumentException(op + " requires a document argument");
    }

    DateArithmeticOp dateArithOp = DateArithmeticOp.fromMongo(op);

    if (dateArithOp == DateArithmeticOp.DATE_DIFF) {
      // $dateDiff: { startDate: expr, endDate: expr, unit: string }
      Object startDate = doc.get("startDate");
      Object endDate = doc.get("endDate");
      Object unit = doc.get("unit");

      if (startDate == null || endDate == null || unit == null) {
        throw new IllegalArgumentException(
            "$dateDiff requires 'startDate', 'endDate', and 'unit' fields");
      }

      String unitStr = unit.toString();
      return DateArithmeticExpression.dateDiff(
          parseValue(startDate), parseValue(endDate), unitStr);
    }

    // $dateAdd / $dateSubtract: { startDate: expr, unit: string, amount: number }
    Object startDate = doc.get("startDate");
    Object unit = doc.get("unit");
    Object amount = doc.get("amount");

    if (startDate == null || unit == null || amount == null) {
      throw new IllegalArgumentException(op + " requires 'startDate', 'unit', and 'amount' fields");
    }

    String unitStr = unit.toString();
    Expression amountExpr = parseValue(amount);

    if (dateArithOp == DateArithmeticOp.DATE_ADD) {
      return DateArithmeticExpression.dateAdd(parseValue(startDate), unitStr, amountExpr);
    } else {
      return DateArithmeticExpression.dateSubtract(parseValue(startDate), unitStr, amountExpr);
    }
  }

  private Expression parseDateStringExpression(String op, Object operand) {
    if (!(operand instanceof Document doc)) {
      throw new IllegalArgumentException(op + " requires a document argument");
    }

    DateStringOp dateStringOp = DateStringOp.fromMongo(op);

    if (dateStringOp == DateStringOp.DATE_FROM_STRING) {
      // $dateFromString: { dateString: expr, format: string (optional) }
      Object dateString = doc.get("dateString");
      Object format = doc.get("format");

      if (dateString == null) {
        throw new IllegalArgumentException("$dateFromString requires 'dateString' field");
      }

      String formatStr = format != null ? format.toString() : null;
      return DateStringExpression.dateFromString(parseValue(dateString), formatStr);
    }

    // $dateToString: { date: expr, format: string (optional) }
    Object date = doc.get("date");
    Object format = doc.get("format");

    if (date == null) {
      throw new IllegalArgumentException("$dateToString requires 'date' field");
    }

    String formatStr = format != null ? format.toString() : null;
    return DateStringExpression.dateToString(parseValue(date), formatStr);
  }

  private Expression parseDateTruncExpression(Object operand) {
    if (!(operand instanceof Document doc)) {
      throw new IllegalArgumentException("$dateTrunc requires a document argument");
    }

    // $dateTrunc: { date: expr, unit: string }
    Object date = doc.get("date");
    Object unit = doc.get("unit");

    if (date == null || unit == null) {
      throw new IllegalArgumentException("$dateTrunc requires 'date' and 'unit' fields");
    }

    String unitStr = unit.toString();
    return DateTruncExpression.dateTrunc(parseValue(date), unitStr);
  }

  private Expression parseDatePartsExpression(String op, Object operand) {
    if (!(operand instanceof Document doc)) {
      throw new IllegalArgumentException(op + " requires a document argument");
    }

    if ("$dateFromParts".equals(op)) {
      // $dateFromParts: { year: expr, month: expr, day: expr, hour: expr, minute: expr, second:
      // expr }
      Object year = doc.get("year");
      Object month = doc.get("month");
      Object day = doc.get("day");
      Object hour = doc.get("hour");
      Object minute = doc.get("minute");
      Object second = doc.get("second");

      if (year == null || month == null || day == null) {
        throw new IllegalArgumentException(
            "$dateFromParts requires 'year', 'month', and 'day' fields");
      }

      Expression hourExpr = hour != null ? parseValue(hour) : null;
      Expression minuteExpr = minute != null ? parseValue(minute) : null;
      Expression secondExpr = second != null ? parseValue(second) : null;

      return DatePartsExpression.dateFromParts(
          parseValue(year),
          parseValue(month),
          parseValue(day),
          hourExpr,
          minuteExpr,
          secondExpr);
    }

    // $dateToParts: { date: expr }
    Object date = doc.get("date");
    if (date == null) {
      throw new IllegalArgumentException("$dateToParts requires 'date' field");
    }

    return DatePartsExpression.dateToParts(parseValue(date));
  }

  private Expression parseArrayExpression(String op, Object operand) {
    ArrayOp arrayOp = ArrayOp.fromMongo(op);

    return switch (arrayOp) {
      case ARRAY_ELEM_AT -> parseArrayElemAt(operand);
      case SIZE -> ArrayExpression.size(parseValue(operand));
      case FIRST -> ArrayExpression.first(parseValue(operand));
      case LAST -> ArrayExpression.last(parseValue(operand));
      case CONCAT_ARRAYS -> parseConcatArrays(operand);
      case SLICE -> parseSlice(operand);
      case FILTER -> parseFilter(operand);
      case MAP -> parseMap(operand);
      case REDUCE -> parseReduce(operand);
      case REVERSE_ARRAY -> ArrayExpression.reverseArray(parseValue(operand));
      case SORT_ARRAY -> parseSortArray(operand);
      case IN -> parseIn(operand);
      case IS_ARRAY -> ArrayExpression.isArray(parseValue(operand));
      case INDEX_OF_ARRAY -> parseIndexOfArray(operand);
      case SET_UNION -> parseSetUnion(operand);
      case SET_INTERSECTION -> parseSetIntersection(operand);
      case SET_DIFFERENCE -> parseSetDifference(operand);
      case SET_EQUALS -> parseSetEquals(operand);
      case SET_IS_SUBSET -> parseSetIsSubset(operand);
      case OBJECT_TO_ARRAY -> ObjectExpression.objectToArray(parseValue(operand));
      case ARRAY_TO_OBJECT -> ObjectExpression.arrayToObject(parseValue(operand));
      case SUM_ARRAY -> ArrayExpression.sumArray(parseValue(operand));
      case AVG_ARRAY -> ArrayExpression.avgArray(parseValue(operand));
      case RANGE -> parseRange(operand);
      case ZIP -> parseZip(operand);
      case ANY_ELEMENT_TRUE, ALL_ELEMENTS_TRUE -> throw new UnsupportedOperatorException(op);
      default -> throw new UnsupportedOperatorException(op);
    };
  }

  private Expression parseArrayElemAt(Object operand) {
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

  private Expression parseConcatArrays(Object operand) {
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

  private Expression parseSlice(Object operand) {
    if (!(operand instanceof List)) {
      throw new IllegalArgumentException("$slice requires an array");
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) operand;
    if (args.size() == 2) {
      return ArrayExpression.slice(parseValue(args.get(0)), parseValue(args.get(1)));
    } else if (args.size() == 3) {
      return ArrayExpression.sliceWithSkip(
          parseValue(args.get(0)), parseValue(args.get(1)), parseValue(args.get(2)));
    }
    throw new IllegalArgumentException("$slice requires 2 or 3 arguments");
  }

  private Expression parseFilter(Object operand) {
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

  private Expression parseMap(Object operand) {
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

  private Expression parseReduce(Object operand) {
    if (!(operand instanceof Document doc)) {
      throw new IllegalArgumentException("$reduce requires a document");
    }
    // Use containsKey() to allow null as a valid initialValue
    if (!doc.containsKey("input")
        || !doc.containsKey("initialValue")
        || !doc.containsKey("in")) {
      throw new IllegalArgumentException(
          "$reduce requires 'input', 'initialValue', and 'in' fields");
    }
    Object input = doc.get("input");
    Object initialValue = doc.get("initialValue");
    Object inExpr = doc.get("in");
    return ArrayExpression.reduce(
        parseValue(input), parseValue(initialValue), parseValue(inExpr));
  }

  private Expression parseSortArray(Object operand) {
    if (!(operand instanceof Document doc)) {
      throw new IllegalArgumentException("$sortArray requires a document");
    }
    Object input = doc.get("input");
    Object sortBy = doc.get("sortBy");
    if (input == null || sortBy == null) {
      throw new IllegalArgumentException("$sortArray requires 'input' and 'sortBy' fields");
    }

    // Handle sortBy as a document (field-based sorting)
    // e.g., {$sortArray: {input: "$products", sortBy: {totalRevenue: -1}}}
    if (sortBy instanceof Document sortByDoc) {
      if (sortByDoc.isEmpty()) {
        throw new IllegalArgumentException("$sortArray sortBy document cannot be empty");
      }
      // Get the first (and typically only) field from sortBy document
      Map.Entry<String, Object> sortEntry = sortByDoc.entrySet().iterator().next();
      String sortField = sortEntry.getKey();
      Object sortValue = sortEntry.getValue();
      boolean ascending = true;
      if (sortValue instanceof Number num) {
        ascending = num.intValue() >= 0;
      }
      return ArrayExpression.sortArrayByField(parseValue(input), sortField, ascending);
    }

    // Handle sortBy as a number (simple value-based sorting)
    // e.g., {$sortArray: {input: "$scores", sortBy: 1}}
    boolean ascending = true;
    if (sortBy instanceof Number num) {
      ascending = num.intValue() >= 0;
    }
    return ArrayExpression.sortArray(parseValue(input), ascending);
  }

  private Expression parseIn(Object operand) {
    if (!(operand instanceof List)) {
      throw new IllegalArgumentException("$in requires an array of [value, array]");
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) operand;
    if (args.size() != 2) {
      throw new IllegalArgumentException("$in requires exactly 2 arguments");
    }
    return ArrayExpression.in(parseValue(args.get(0)), parseValue(args.get(1)));
  }

  private Expression parseIndexOfArray(Object operand) {
    if (!(operand instanceof List)) {
      throw new IllegalArgumentException("$indexOfArray requires an array");
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) operand;
    if (args.size() < 2) {
      throw new IllegalArgumentException("$indexOfArray requires at least 2 arguments");
    }
    Expression array = parseValue(args.get(0));
    Expression value = parseValue(args.get(1));

    if (args.size() == 2) {
      return ArrayExpression.indexOfArray(array, value);
    } else if (args.size() >= 4) {
      Expression start = parseValue(args.get(2));
      Expression end = parseValue(args.get(3));
      return ArrayExpression.indexOfArrayWithRange(array, value, start, end);
    } else {
      // 3 arguments - start only
      Expression start = parseValue(args.get(2));
      return ArrayExpression.indexOfArrayWithRange(
          array, value, start, LiteralExpression.of(Integer.MAX_VALUE));
    }
  }

  private Expression parseSetUnion(Object operand) {
    // MongoDB allows $setUnion with:
    // 1. An array of expressions: {$setUnion: ["$arr1", "$arr2"]}
    // 2. A single expression that evaluates to an array: {$setUnion: {$map: {...}}}
    if (operand instanceof List) {
      @SuppressWarnings("unchecked")
      List<Object> args = (List<Object>) operand;
      List<Expression> arrays = new ArrayList<>();
      for (Object arg : args) {
        arrays.add(parseValue(arg));
      }
      return ArrayExpression.setUnion(arrays);
    } else {
      // Single expression case - wrap in a list for setUnion
      return ArrayExpression.setUnion(List.of(parseValue(operand)));
    }
  }

  private Expression parseSetIntersection(Object operand) {
    if (!(operand instanceof List)) {
      throw new IllegalArgumentException("$setIntersection requires an array of arrays");
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) operand;
    List<Expression> arrays = new ArrayList<>();
    for (Object arg : args) {
      arrays.add(parseValue(arg));
    }
    return ArrayExpression.setIntersection(arrays);
  }

  private Expression parseSetDifference(Object operand) {
    if (!(operand instanceof List)) {
      throw new IllegalArgumentException("$setDifference requires an array");
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) operand;
    if (args.size() != 2) {
      throw new IllegalArgumentException("$setDifference requires exactly 2 arguments");
    }
    return ArrayExpression.setDifference(parseValue(args.get(0)), parseValue(args.get(1)));
  }

  private Expression parseSetEquals(Object operand) {
    if (!(operand instanceof List)) {
      throw new IllegalArgumentException("$setEquals requires an array of arrays");
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) operand;
    List<Expression> arrays = new ArrayList<>();
    for (Object arg : args) {
      arrays.add(parseValue(arg));
    }
    return ArrayExpression.setEquals(arrays);
  }

  private Expression parseSetIsSubset(Object operand) {
    if (!(operand instanceof List)) {
      throw new IllegalArgumentException("$setIsSubset requires an array");
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) operand;
    if (args.size() != 2) {
      throw new IllegalArgumentException("$setIsSubset requires exactly 2 arguments");
    }
    return ArrayExpression.setIsSubset(parseValue(args.get(0)), parseValue(args.get(1)));
  }

  private Expression parseRange(Object operand) {
    if (!(operand instanceof List)) {
      throw new IllegalArgumentException("$range requires an array [start, end, step?]");
    }
    @SuppressWarnings("unchecked")
    List<Object> args = (List<Object>) operand;
    if (args.size() < 2 || args.size() > 3) {
      throw new IllegalArgumentException("$range requires 2 or 3 arguments [start, end, step?]");
    }
    Expression start = parseValue(args.get(0));
    Expression end = parseValue(args.get(1));
    if (args.size() == 3) {
      return ArrayExpression.range(start, end, parseValue(args.get(2)));
    }
    return ArrayExpression.range(start, end);
  }

  @SuppressWarnings("unchecked")
  private Expression parseZip(Object operand) {
    if (!(operand instanceof Map)) {
      throw new IllegalArgumentException("$zip requires an object with 'inputs' field");
    }
    Map<String, Object> spec = (Map<String, Object>) operand;

    Object inputsObj = spec.get("inputs");
    if (!(inputsObj instanceof List)) {
      throw new IllegalArgumentException("$zip 'inputs' must be an array of arrays");
    }
    List<Object> inputsList = (List<Object>) inputsObj;
    List<Expression> inputs = new ArrayList<>();
    for (Object input : inputsList) {
      inputs.add(parseValue(input));
    }

    boolean useLongestLength = false;
    if (spec.containsKey("useLongestLength")) {
      Object useLongestObj = spec.get("useLongestLength");
      if (useLongestObj instanceof Boolean) {
        useLongestLength = (Boolean) useLongestObj;
      }
    }

    List<Expression> defaults = null;
    if (spec.containsKey("defaults")) {
      Object defaultsObj = spec.get("defaults");
      if (defaultsObj instanceof List) {
        List<Object> defaultsList = (List<Object>) defaultsObj;
        defaults = new ArrayList<>();
        for (Object def : defaultsList) {
          defaults.add(parseValue(def));
        }
      }
    }

    return ArrayExpression.zip(inputs, useLongestLength, defaults);
  }

  private Expression parseObjectExpression(String op, Object operand) {
    ObjectOp objectOp = ObjectOp.fromMongo(op);

    return switch (objectOp) {
      case MERGE_OBJECTS -> parseMergeObjects(operand);
      case OBJECT_TO_ARRAY -> ObjectExpression.objectToArray(parseValue(operand));
      case ARRAY_TO_OBJECT -> ObjectExpression.arrayToObject(parseValue(operand));
      case GET_FIELD -> parseGetField(operand);
    };
  }

  @SuppressWarnings("unchecked")
  private Expression parseGetField(Object operand) {
    // Two forms:
    // 1. {$getField: {field: <expression>, input: <expression>}}
    // 2. {$getField: "<string>"} - shorthand for field from $$CURRENT
    if (operand instanceof String fieldName) {
      // Shorthand: get field from $$CURRENT (current document)
      return ObjectExpression.getField(
          LiteralExpression.of(fieldName), FieldPathExpression.of("$$CURRENT"));
    }

    if (!(operand instanceof Document)) {
      throw new IllegalArgumentException("$getField requires a document or string");
    }

    Document doc = (Document) operand;
    if (!doc.containsKey("field")) {
      throw new IllegalArgumentException("$getField requires 'field' parameter");
    }

    Expression fieldExpr = parseValue(doc.get("field"));
    Expression inputExpr;
    if (doc.containsKey("input")) {
      inputExpr = parseValue(doc.get("input"));
    } else {
      // Default to $$CURRENT if input is not specified
      inputExpr = FieldPathExpression.of("$$CURRENT");
    }

    return ObjectExpression.getField(fieldExpr, inputExpr);
  }

  private Expression parseMergeObjects(Object operand) {
    // $mergeObjects can take a single object or an array of objects
    if (operand instanceof List) {
      @SuppressWarnings("unchecked")
      List<Object> args = (List<Object>) operand;
      List<Expression> objects = new ArrayList<>();
      for (Object arg : args) {
        objects.add(parseValue(arg));
      }
      return ObjectExpression.mergeObjects(objects);
    } else {
      // Single object
      return ObjectExpression.mergeObjects(List.of(parseValue(operand)));
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
