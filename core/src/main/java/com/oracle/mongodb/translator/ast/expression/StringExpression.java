/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a string expression. Translates to Oracle string functions (CONCAT, LOWER, UPPER,
 * SUBSTR, etc.).
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>{@code {$concat: ["Hello ", "$name"]}} becomes {@code 'Hello ' || name}
 *   <li>{@code {$toLower: "$email"}} becomes {@code LOWER(email)}
 *   <li>{@code {$toUpper: "$code"}} becomes {@code UPPER(code)}
 *   <li>{@code {$substr: ["$text", 0, 5]}} becomes {@code SUBSTR(text, 1, 5)}
 * </ul>
 */
public final class StringExpression implements Expression {

  private final StringOp op;
  private final List<Expression> arguments;

  /**
   * Creates a string expression.
   *
   * @param op the string operator
   * @param arguments the argument expressions
   */
  public StringExpression(StringOp op, List<Expression> arguments) {
    this.op = Objects.requireNonNull(op, "op must not be null");
    this.arguments = arguments != null ? new ArrayList<>(arguments) : new ArrayList<>();
  }

  /** Creates a $concat expression. */
  public static StringExpression concat(List<Expression> parts) {
    return new StringExpression(StringOp.CONCAT, parts);
  }

  /** Creates a $toLower expression. */
  public static StringExpression toLower(Expression argument) {
    return new StringExpression(StringOp.TO_LOWER, List.of(argument));
  }

  /** Creates a $toUpper expression. */
  public static StringExpression toUpper(Expression argument) {
    return new StringExpression(StringOp.TO_UPPER, List.of(argument));
  }

  /**
   * Creates a $substr expression.
   *
   * @param string the source string
   * @param start the starting index (0-based in MongoDB)
   * @param length the number of characters
   */
  public static StringExpression substr(Expression string, Expression start, Expression length) {
    return new StringExpression(StringOp.SUBSTR, List.of(string, start, length));
  }

  /** Creates a $trim expression. */
  public static StringExpression trim(Expression argument) {
    return new StringExpression(StringOp.TRIM, List.of(argument));
  }

  /** Creates a $strLenCp expression (string length in code points). */
  public static StringExpression strlen(Expression argument) {
    return new StringExpression(StringOp.STRLEN, List.of(argument));
  }

  /**
   * Creates a $split expression.
   *
   * @param string the source string to split
   * @param delimiter the delimiter to split on
   */
  public static StringExpression split(Expression string, Expression delimiter) {
    return new StringExpression(StringOp.SPLIT, List.of(string, delimiter));
  }

  /**
   * Creates a $indexOfCp expression with just string and substring.
   *
   * @param string the source string to search
   * @param substring the substring to find
   */
  public static StringExpression indexOfCp(Expression string, Expression substring) {
    return new StringExpression(StringOp.INDEX_OF_CP, List.of(string, substring));
  }

  /**
   * Creates a $indexOfCp expression with start and end indices.
   *
   * @param string the source string to search
   * @param substring the substring to find
   * @param start the starting index (0-based)
   * @param end the ending index (exclusive)
   */
  public static StringExpression indexOfCp(
      Expression string, Expression substring, Expression start, Expression end) {
    return new StringExpression(StringOp.INDEX_OF_CP, List.of(string, substring, start, end));
  }

  /**
   * Creates a $regexMatch expression.
   *
   * @param input the input string
   * @param regex the regex pattern
   */
  public static StringExpression regexMatch(Expression input, Expression regex) {
    return new StringExpression(StringOp.REGEX_MATCH, List.of(input, regex));
  }

  /**
   * Creates a $regexMatch expression with options.
   *
   * @param input the input string
   * @param regex the regex pattern
   * @param options regex options (e.g., "i" for case-insensitive)
   */
  public static StringExpression regexMatch(
      Expression input, Expression regex, Expression options) {
    return new StringExpression(StringOp.REGEX_MATCH, List.of(input, regex, options));
  }

  /**
   * Creates a $regexFind expression.
   *
   * @param input the input string
   * @param regex the regex pattern
   */
  public static StringExpression regexFind(Expression input, Expression regex) {
    return new StringExpression(StringOp.REGEX_FIND, List.of(input, regex));
  }

  /**
   * Creates a $regexFind expression with options.
   *
   * @param input the input string
   * @param regex the regex pattern
   * @param options regex options
   */
  public static StringExpression regexFind(Expression input, Expression regex, Expression options) {
    return new StringExpression(StringOp.REGEX_FIND, List.of(input, regex, options));
  }

  /**
   * Creates a $replaceOne expression.
   *
   * @param input the input string
   * @param find the substring to find
   * @param replacement the replacement string
   */
  public static StringExpression replaceOne(
      Expression input, Expression find, Expression replacement) {
    return new StringExpression(StringOp.REPLACE_ONE, List.of(input, find, replacement));
  }

  /**
   * Creates a $replaceAll expression.
   *
   * @param input the input string
   * @param find the substring to find
   * @param replacement the replacement string
   */
  public static StringExpression replaceAll(
      Expression input, Expression find, Expression replacement) {
    return new StringExpression(StringOp.REPLACE_ALL, List.of(input, find, replacement));
  }

  /** Returns the string operator. */
  public StringOp getOp() {
    return op;
  }

  /** Returns the arguments as an unmodifiable list. */
  public List<Expression> getArguments() {
    return Collections.unmodifiableList(arguments);
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    switch (op) {
      case CONCAT -> renderConcat(ctx);
      case SUBSTR -> renderSubstr(ctx);
      case TRIM -> renderTrim(ctx);
      case SPLIT -> renderSplit(ctx);
      case INDEX_OF_CP -> renderIndexOfCp(ctx);
      case REGEX_MATCH -> renderRegexMatch(ctx);
      case REGEX_FIND -> renderRegexFind(ctx);
      case REPLACE_ONE -> renderReplaceOne(ctx);
      case REPLACE_ALL -> renderReplaceAll(ctx);
      default -> renderSimpleFunction(ctx);
    }
  }

  private void renderConcat(SqlGenerationContext ctx) {
    // Use || for concatenation in Oracle
    ctx.sql("(");
    for (int i = 0; i < arguments.size(); i++) {
      if (i > 0) {
        ctx.sql(" || ");
      }
      ctx.visit(arguments.get(i));
    }
    ctx.sql(")");
  }

  private void renderSubstr(SqlGenerationContext ctx) {
    // MongoDB uses 0-based index, Oracle uses 1-based
    // So we need to add 1 to the start position
    ctx.sql("SUBSTR(");
    ctx.visit(arguments.get(0)); // string
    ctx.sql(", ");

    // Handle start index - need to add 1 for Oracle's 1-based indexing
    Expression startExpr = arguments.get(1);
    if (startExpr instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
      // If it's a literal number, add 1 directly
      ctx.bind(num.intValue() + 1);
    } else {
      // Otherwise, add 1 in SQL
      ctx.sql("(");
      ctx.visit(startExpr);
      ctx.sql(" + 1)");
    }

    ctx.sql(", ");
    ctx.visit(arguments.get(2)); // length
    ctx.sql(")");
  }

  private void renderTrim(SqlGenerationContext ctx) {
    ctx.sql("TRIM(");
    if (!arguments.isEmpty()) {
      ctx.visit(arguments.get(0));
    }
    ctx.sql(")");
  }

  private void renderSimpleFunction(SqlGenerationContext ctx) {
    ctx.sql(op.getSqlFunction());
    ctx.sql("(");
    for (int i = 0; i < arguments.size(); i++) {
      if (i > 0) {
        ctx.sql(", ");
      }
      ctx.visit(arguments.get(i));
    }
    ctx.sql(")");
  }

  private void renderSplit(SqlGenerationContext ctx) {
    // MongoDB: {$split: ["$string", "delimiter"]}
    // Oracle doesn't have direct split, use REGEXP_SUBSTR in a query pattern
    // For simplicity, return a JSON array using REGEXP_SUBSTR to extract all parts
    ctx.sql("(SELECT JSON_ARRAYAGG(REGEXP_SUBSTR(");
    ctx.visit(arguments.get(0));
    ctx.sql(", '[^' || ");
    ctx.visit(arguments.get(1));
    ctx.sql(" || ']+', 1, LEVEL)) FROM DUAL CONNECT BY REGEXP_SUBSTR(");
    ctx.visit(arguments.get(0));
    ctx.sql(", '[^' || ");
    ctx.visit(arguments.get(1));
    ctx.sql(" || ']+', 1, LEVEL) IS NOT NULL)");
  }

  private void renderIndexOfCp(SqlGenerationContext ctx) {
    // MongoDB: {$indexOfCp: ["string", "substring", start, end]}
    // MongoDB returns 0-based index, -1 if not found
    // Oracle INSTR returns 1-based index, 0 if not found
    // Need to convert: if INSTR returns 0, return -1; else return INSTR - 1
    ctx.sql("CASE WHEN INSTR(");
    ctx.visit(arguments.get(0)); // string
    ctx.sql(", ");
    ctx.visit(arguments.get(1)); // substring

    if (arguments.size() >= 3) {
      // Has start position - Oracle uses 1-based, so add 1 to MongoDB's 0-based
      ctx.sql(", ");
      Expression startExpr = arguments.get(2);
      if (startExpr instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
        ctx.bind(num.intValue() + 1);
      } else {
        ctx.sql("(");
        ctx.visit(startExpr);
        ctx.sql(" + 1)");
      }
    }

    ctx.sql(") = 0 THEN -1 ELSE INSTR(");
    ctx.visit(arguments.get(0));
    ctx.sql(", ");
    ctx.visit(arguments.get(1));

    if (arguments.size() >= 3) {
      ctx.sql(", ");
      Expression startExpr = arguments.get(2);
      if (startExpr instanceof LiteralExpression lit && lit.getValue() instanceof Number num) {
        ctx.bind(num.intValue() + 1);
      } else {
        ctx.sql("(");
        ctx.visit(startExpr);
        ctx.sql(" + 1)");
      }
    }

    ctx.sql(") - 1 END");
  }

  private void renderRegexMatch(SqlGenerationContext ctx) {
    // MongoDB: {$regexMatch: {input: "string", regex: "pattern", options: "i"}}
    // Oracle: REGEXP_LIKE(string, pattern, flags)
    // Returns boolean (1/0)
    ctx.sql("CASE WHEN REGEXP_LIKE(");
    ctx.visit(arguments.get(0)); // input
    ctx.sql(", ");
    ctx.visit(arguments.get(1)); // regex

    if (arguments.size() >= 3) {
      ctx.sql(", ");
      ctx.visit(arguments.get(2)); // options
    }

    ctx.sql(") THEN 1 ELSE 0 END");
  }

  private void renderRegexFind(SqlGenerationContext ctx) {
    // MongoDB: {$regexFind: {input: "string", regex: "pattern"}}
    // Returns document with match, idx, captures
    // For simplicity, just return the index (0-based) or -1 if not found
    ctx.sql("CASE WHEN REGEXP_INSTR(");
    ctx.visit(arguments.get(0)); // input
    ctx.sql(", ");
    ctx.visit(arguments.get(1)); // regex

    ctx.sql(") = 0 THEN -1 ELSE REGEXP_INSTR(");
    ctx.visit(arguments.get(0));
    ctx.sql(", ");
    ctx.visit(arguments.get(1));
    ctx.sql(") - 1 END");
  }

  private void renderReplaceOne(SqlGenerationContext ctx) {
    // MongoDB: {$replaceOne: {input: "string", find: "pattern", replacement: "new"}}
    // Oracle: REGEXP_REPLACE with occurrence = 1
    ctx.sql("REGEXP_REPLACE(");
    ctx.visit(arguments.get(0)); // input
    ctx.sql(", ");
    ctx.visit(arguments.get(1)); // find (treat as literal, escape special chars)
    ctx.sql(", ");
    ctx.visit(arguments.get(2)); // replacement
    ctx.sql(", 1, 1)"); // start position 1, replace only 1st occurrence
  }

  private void renderReplaceAll(SqlGenerationContext ctx) {
    // MongoDB: {$replaceAll: {input: "string", find: "pattern", replacement: "new"}}
    // Oracle: REGEXP_REPLACE (replaces all by default)
    ctx.sql("REGEXP_REPLACE(");
    ctx.visit(arguments.get(0)); // input
    ctx.sql(", ");
    ctx.visit(arguments.get(1)); // find
    ctx.sql(", ");
    ctx.visit(arguments.get(2)); // replacement
    ctx.sql(")");
  }

  @Override
  public String toString() {
    return "String(" + op.getMongoOperator() + ", " + arguments + ")";
  }
}
