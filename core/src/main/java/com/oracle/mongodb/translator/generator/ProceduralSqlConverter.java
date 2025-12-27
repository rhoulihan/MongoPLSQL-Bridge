/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts CTE-based SQL to procedural SQL using temporary tables.
 * This allows complex queries to execute without exceeding Oracle's CTE complexity limits.
 *
 * <p>The procedural SQL uses regular tables with a prefix (TMP_PIPE_) that are created,
 * populated, and dropped within the execution. This approach works around Oracle session
 * crashes (ORA-03113) that can occur with deeply nested CTEs.
 */
public final class ProceduralSqlConverter {

  private static final String TABLE_PREFIX = "TMP_PIPE_";
  private static final Pattern CTE_PATTERN =
      Pattern.compile("\"(Q\\d+[^\"]*?)\"\\s*\\([^)]+\\)\\s*AS\\s*\\(", Pattern.CASE_INSENSITIVE);

  /** Threshold number of CTEs above which procedural mode is recommended. */
  private static final int CTE_COMPLEXITY_THRESHOLD = 15;

  private ProceduralSqlConverter() {}

  /**
   * Determines if procedural mode should be used based on query complexity.
   * Returns true if the SQL contains more CTEs than the complexity threshold.
   *
   * @param sql the SQL to analyze
   * @return true if procedural mode is recommended
   */
  public static boolean shouldUseProcedural(String sql) {
    if (sql == null || sql.isBlank()) {
      return false;
    }
    return countCtes(sql) >= CTE_COMPLEXITY_THRESHOLD;
  }

  /**
   * Counts the number of CTEs in a SQL statement.
   *
   * @param sql the SQL to analyze
   * @return the number of CTEs found
   */
  public static int countCtes(String sql) {
    if (sql == null || sql.isBlank()) {
      return 0;
    }

    String trimmed = sql.trim().toUpperCase();
    if (!trimmed.startsWith("WITH ")) {
      return 0;
    }

    // Count occurrences of CTE pattern: "name" (...) AS (
    // This pattern matches: "Q1" ("DATA") AS (
    Pattern ctePattern = Pattern.compile(
        "\"[^\"]+\"\\s*\\([^)]*\\)\\s+AS\\s*\\(",
        Pattern.CASE_INSENSITIVE
    );
    Matcher matcher = ctePattern.matcher(sql);
    int count = 0;
    while (matcher.find()) {
      count++;
    }
    return count;
  }

  /**
   * Converts CTE-based SQL to procedural SQL with CREATE TABLE statements.
   *
   * @param cteSql the CTE-based SQL (WITH ... SELECT ...)
   * @return procedural SQL script that can be executed step by step
   */
  public static String convert(String cteSql) {
    if (cteSql == null || !cteSql.trim().toUpperCase().startsWith("WITH")) {
      return cteSql; // Not a CTE query, return as-is
    }

    List<CteDefinition> ctes = parseCtes(cteSql);
    if (ctes.isEmpty()) {
      return cteSql;
    }

    StringBuilder result = new StringBuilder();

    // Generate cleanup statements
    result.append("-- Procedural execution of complex pipeline\n");
    result.append("-- Auto-generated from CTE-based SQL\n\n");

    result.append("-- Cleanup any leftover tables from previous runs\n");
    for (CteDefinition cte : ctes) {
      result.append("BEGIN EXECUTE IMMEDIATE 'DROP TABLE ")
          .append(TABLE_PREFIX)
          .append(cte.name)
          .append(" PURGE'; EXCEPTION WHEN OTHERS THEN NULL; END;\n/\n");
    }
    result.append("\n");

    // Generate CREATE TABLE statements for each CTE
    for (CteDefinition cte : ctes) {
      result.append("-- ").append(cte.name).append("\n");
      result.append("CREATE TABLE ").append(TABLE_PREFIX).append(cte.name).append(" AS\n");

      // Replace references to other CTEs with table references
      String body = cte.body;
      for (CteDefinition otherCte : ctes) {
        // Replace "Q1" with TMP_PIPE_Q1
        body = body.replaceAll(
            "\"" + Pattern.quote(otherCte.name) + "\"",
            TABLE_PREFIX + otherCte.name);
        // Also replace FROM Q1 (without quotes)
        body = body.replaceAll(
            "FROM\\s+" + Pattern.quote(otherCte.name) + "\\b",
            "FROM " + TABLE_PREFIX + otherCte.name);
      }

      // Add column aliases if the CTE defines columns and the body doesn't have them
      body = addColumnAliases(body, cte.columns);

      result.append(body).append(";\n");

      // Add IS JSON constraint for DATA column to enable dot notation access
      // This is required because CTAS creates CLOB columns which don't support dot notation
      if (cte.columns.contains("DATA")) {
        result.append("ALTER TABLE ").append(TABLE_PREFIX).append(cte.name)
            .append(" ADD CONSTRAINT ").append(cte.name.toLowerCase().replace("-", "_"))
            .append("_json CHECK (\"DATA\" IS JSON);\n");
      }
      result.append("\n");
    }

    // Generate the final SELECT
    String finalSelect = extractFinalSelect(cteSql);
    if (finalSelect != null) {
      // Replace CTE references in final select
      for (CteDefinition cte : ctes) {
        finalSelect = finalSelect.replaceAll(
            "\"" + Pattern.quote(cte.name) + "\"",
            TABLE_PREFIX + cte.name);
      }
      result.append("-- Final result\n");
      result.append(finalSelect).append(";\n\n");
    }

    // Generate cleanup statements
    result.append("-- Cleanup\n");
    for (int i = ctes.size() - 1; i >= 0; i--) {
      result.append("DROP TABLE ")
          .append(TABLE_PREFIX)
          .append(ctes.get(i).name)
          .append(" PURGE;\n");
    }

    return result.toString();
  }

  /**
   * Parses CTE definitions from a WITH clause.
   */
  private static List<CteDefinition> parseCtes(String sql) {
    List<CteDefinition> ctes = new ArrayList<>();

    // Find the WITH clause
    int withStart = sql.toUpperCase().indexOf("WITH ");
    if (withStart < 0) {
      return ctes;
    }

    String withClause = sql.substring(withStart + 5);

    // Parse each CTE definition
    int pos = 0;
    while (pos < withClause.length()) {
      // Skip whitespace
      while (pos < withClause.length() && Character.isWhitespace(withClause.charAt(pos))) {
        pos++;
      }

      // Find CTE name (in quotes)
      if (pos >= withClause.length() || withClause.charAt(pos) != '"') {
        break;
      }
      int nameStart = pos + 1;
      int nameEnd = withClause.indexOf('"', nameStart);
      if (nameEnd < 0) {
        break;
      }
      final String cteName = withClause.substring(nameStart, nameEnd);
      pos = nameEnd + 1;

      // Skip whitespace before column list
      while (pos < withClause.length() && Character.isWhitespace(withClause.charAt(pos))) {
        pos++;
      }

      // Parse column list (between "(" and ") AS (")
      List<String> columns = new ArrayList<>();
      if (pos < withClause.length() && withClause.charAt(pos) == '(') {
        int colStart = pos + 1;
        int colEnd = withClause.indexOf(')', colStart);
        if (colEnd > colStart) {
          String colList = withClause.substring(colStart, colEnd);
          for (String col : colList.split(",")) {
            String trimmed = col.trim();
            // Remove quotes if present
            if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
              trimmed = trimmed.substring(1, trimmed.length() - 1);
            }
            columns.add(trimmed);
          }
          pos = colEnd + 1;
        }
      }

      // Skip to "AS ("
      int asPos = withClause.toUpperCase().indexOf(" AS (", pos);
      if (asPos < 0) {
        break;
      }
      pos = asPos + 5; // Position after "AS ("

      // Find matching closing parenthesis
      int depth = 1;
      int bodyStart = pos;
      while (pos < withClause.length() && depth > 0) {
        char c = withClause.charAt(pos);
        if (c == '(') {
          depth++;
        } else if (c == ')') {
          depth--;
        }
        pos++;
      }
      int bodyEnd = pos - 1;

      String body = withClause.substring(bodyStart, bodyEnd).trim();
      ctes.add(new CteDefinition(cteName, body, columns));

      // Skip comma or break if we hit SELECT
      while (pos < withClause.length() && Character.isWhitespace(withClause.charAt(pos))) {
        pos++;
      }
      if (pos < withClause.length() && withClause.charAt(pos) == ',') {
        pos++;
      } else if (withClause.toUpperCase().substring(pos).startsWith("SELECT")) {
        break;
      }
    }

    return ctes;
  }

  /**
   * Adds column aliases to a SELECT body if needed for CREATE TABLE AS SELECT.
   * Oracle requires column aliases for complex expressions in CTAS.
   */
  private static String addColumnAliases(String body, List<String> columns) {
    if (columns.isEmpty()) {
      return body;
    }

    String trimmed = body.trim().toUpperCase();
    if (!trimmed.startsWith("SELECT")) {
      return body;
    }

    // For single-column CTEs, check if alias is needed
    if (columns.size() == 1) {
      String colName = columns.get(0);
      // Pattern: SELECT expr FROM ... where expr doesn't have AS "colName"
      // We need to add AS "colName" before FROM

      // Find the FROM position (not inside parentheses)
      int fromPos = findFromClause(body);
      if (fromPos < 0) {
        return body;
      }

      String selectPart = body.substring(0, fromPos).trim();
      String fromPart = body.substring(fromPos);

      // Check if already has alias at the end (AS "DATA" or similar)
      String aliasPattern = "(?i)\\s+AS\\s+\"" + Pattern.quote(colName) + "\"\\s*$";
      if (selectPart.matches(".*" + aliasPattern)) {
        return body;
      }

      // Also check for unquoted alias
      if (selectPart.toUpperCase().matches(".*\\s+AS\\s+" + colName.toUpperCase() + "\\s*$")) {
        return body;
      }

      // Add the alias before FROM
      return selectPart + " AS \"" + colName + "\" " + fromPart;
    }

    // For multi-column CTEs, we'd need more complex parsing
    // For now, return as-is and rely on the original query having proper aliases
    return body;
  }

  /**
   * Finds the position of the FROM clause that's not inside parentheses.
   */
  private static int findFromClause(String sql) {
    int depth = 0;
    String upper = sql.toUpperCase();

    for (int i = 0; i < sql.length() - 4; i++) {
      char c = sql.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (depth == 0 && upper.substring(i).startsWith("FROM ")) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Extracts the final SELECT statement after all CTEs.
   */
  private static String extractFinalSelect(String sql) {
    // Find the last SELECT that's not inside a CTE
    int depth = 0;
    int lastSelectPos = -1;

    for (int i = 0; i < sql.length() - 6; i++) {
      char c = sql.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (depth == 0 && sql.substring(i).toUpperCase().startsWith("SELECT")) {
        lastSelectPos = i;
      }
    }

    return lastSelectPos >= 0 ? sql.substring(lastSelectPos).trim() : null;
  }

  /** Represents a single CTE definition. */
  private record CteDefinition(String name, String body, List<String> columns) {}
}
