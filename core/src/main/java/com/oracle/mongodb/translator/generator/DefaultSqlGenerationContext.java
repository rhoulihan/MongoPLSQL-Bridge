/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.LookupSizeExpression;
import com.oracle.mongodb.translator.generator.dialect.Oracle26aiDialect;
import com.oracle.mongodb.translator.generator.dialect.OracleDialect;
import com.oracle.mongodb.translator.util.FieldNameValidator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Default implementation of SqlGenerationContext. */
public class DefaultSqlGenerationContext implements SqlGenerationContext {

  // Oracle identifiers must start with a letter (not underscore) to be unquoted
  // Identifiers starting with underscore, containing special chars, or that are
  // reserved words need to be quoted
  private static final Pattern SIMPLE_IDENTIFIER = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");

  // Oracle reserved words that must be quoted when used as identifiers
  private static final Set<String> RESERVED_WORDS =
      Set.of(
          "ACCESS",
          "ADD",
          "ALL",
          "ALTER",
          "AND",
          "ANY",
          "AS",
          "ASC",
          "AUDIT",
          "BETWEEN",
          "BY",
          "CHAR",
          "CHECK",
          "CLUSTER",
          "COLUMN",
          "COMMENT",
          "COMPRESS",
          "CONNECT",
          "CREATE",
          "CURRENT",
          "DATE",
          "DECIMAL",
          "DEFAULT",
          "DELETE",
          "DESC",
          "DISTINCT",
          "DROP",
          "ELSE",
          "EXCLUSIVE",
          "EXISTS",
          "FILE",
          "FLOAT",
          "FOR",
          "FROM",
          "GRANT",
          "GROUP",
          "HAVING",
          "IDENTIFIED",
          "IMMEDIATE",
          "IN",
          "INCREMENT",
          "INDEX",
          "INITIAL",
          "INSERT",
          "INTEGER",
          "INTERSECT",
          "INTO",
          "IS",
          "LEVEL",
          "LIKE",
          "LOCK",
          "LONG",
          "MAXEXTENTS",
          "MINUS",
          "MLSLABEL",
          "MODE",
          "MODIFY",
          "NOAUDIT",
          "NOCOMPRESS",
          "NOT",
          "NOWAIT",
          "NULL",
          "NUMBER",
          "OF",
          "OFFLINE",
          "ON",
          "ONLINE",
          "OPTION",
          "OR",
          "ORDER",
          "PCTFREE",
          "PRIOR",
          "PRIVILEGES",
          "PUBLIC",
          "RAW",
          "RENAME",
          "RESOURCE",
          "REVOKE",
          "ROW",
          "ROWID",
          "ROWNUM",
          "ROWS",
          "SELECT",
          "SESSION",
          "SET",
          "SHARE",
          "SIZE",
          "SMALLINT",
          "START",
          "SUCCESSFUL",
          "SYNONYM",
          "SYSDATE",
          "TABLE",
          "THEN",
          "TO",
          "TRIGGER",
          "UID",
          "UNION",
          "UNIQUE",
          "UPDATE",
          "USER",
          "VALIDATE",
          "VALUES",
          "VARCHAR",
          "VARCHAR2",
          "VIEW",
          "WHENEVER",
          "WHERE",
          "WITH");

  private final StringBuilder sql = new StringBuilder();
  private final List<Object> bindVariables = new ArrayList<>();
  private final Map<String, Integer> tableAliasCounters = new HashMap<>();
  private final Map<String, Expression> virtualFields = new HashMap<>();
  private final Map<String, SqlGenerationContext.LookupFieldInfo> lookupFields = new HashMap<>();
  private final Set<String> lookupsConsumedBySize = new HashSet<>();
  private final Map<String, String> lookupTableAliases = new HashMap<>();
  private final Map<String, String> pipelineLookupAliases = new HashMap<>();
  private final Map<String, String> unwoundPaths = new HashMap<>();
  private final boolean inlineValues;
  private final OracleDialect dialect;
  private final String baseTableAlias;
  private boolean jsonOutputMode = false;
  private boolean nestedPipeline = false;
  private boolean inCteContext = false;
  private String cteSourceTable = null;
  private Set<String> compoundIdFields = Collections.emptySet();

  // Uses SqlGenerationContext.LookupFieldInfo record for lookup metadata

  public DefaultSqlGenerationContext() {
    this(false, Oracle26aiDialect.INSTANCE, null);
  }

  public DefaultSqlGenerationContext(boolean inlineValues) {
    this(inlineValues, Oracle26aiDialect.INSTANCE, null);
  }

  public DefaultSqlGenerationContext(boolean inlineValues, OracleDialect dialect) {
    this(inlineValues, dialect, null);
  }

  /**
   * Creates a context with all configuration options.
   *
   * @param inlineValues whether to inline literal values in SQL
   * @param dialect the Oracle SQL dialect to use
   * @param baseTableAlias the base table alias for field references
   */
  public DefaultSqlGenerationContext(
      boolean inlineValues, OracleDialect dialect, String baseTableAlias) {
    this.inlineValues = inlineValues;
    this.dialect = dialect != null ? dialect : Oracle26aiDialect.INSTANCE;
    this.baseTableAlias = baseTableAlias; // null means no alias needed
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
    // Always quote in JSON output mode to preserve case for JSON_OBJECT(*)
    // Otherwise: quote if doesn't match simple pattern OR is a reserved word
    if (jsonOutputMode) {
      sql.append("\"").append(name).append("\"");
    } else if (SIMPLE_IDENTIFIER.matcher(name).matches()
        && !RESERVED_WORDS.contains(name.toUpperCase())) {
      sql.append(name);
    } else {
      sql.append("\"").append(name).append("\"");
    }
  }

  @Override
  public void jsonField(String fieldName) {
    // Validate field name to prevent JSON path injection
    FieldNameValidator.validateFieldName(fieldName);
    sql.append(fieldName);
  }

  @Override
  public void tableName(String name) {
    // Validate table name to prevent SQL injection
    FieldNameValidator.validateTableName(name);
    // Table names should not be affected by jsonOutputMode - only quote if needed
    // (starts with underscore, contains special chars, or is a reserved word)
    if (SIMPLE_IDENTIFIER.matcher(name).matches()
        && !RESERVED_WORDS.contains(name.toUpperCase())) {
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

  @Override
  public String generateTableAlias(String tableName) {
    int count = tableAliasCounters.compute(tableName, (k, v) -> v == null ? 1 : v + 1);
    return tableName + "_" + count;
  }

  @Override
  public String getBaseTableAlias() {
    return baseTableAlias;
  }

  @Override
  public SqlGenerationContext createNestedContext() {
    DefaultSqlGenerationContext nested =
        new DefaultSqlGenerationContext(inlineValues, dialect, baseTableAlias);
    // Copy virtual fields, lookup fields, consumed lookups, table aliases, and unwound paths to
    // nested context
    nested.virtualFields.putAll(this.virtualFields);
    nested.lookupFields.putAll(this.lookupFields);
    nested.lookupsConsumedBySize.addAll(this.lookupsConsumedBySize);
    nested.lookupTableAliases.putAll(this.lookupTableAliases);
    nested.pipelineLookupAliases.putAll(this.pipelineLookupAliases);
    nested.unwoundPaths.putAll(this.unwoundPaths);
    nested.jsonOutputMode = this.jsonOutputMode;
    nested.nestedPipeline = this.nestedPipeline;
    nested.inCteContext = this.inCteContext;
    nested.cteSourceTable = this.cteSourceTable;
    nested.compoundIdFields = this.compoundIdFields;
    return nested;
  }

  @Override
  public void registerVirtualField(String fieldName, Expression expression) {
    virtualFields.put(fieldName, expression);
  }

  @Override
  public Expression getVirtualField(String fieldName) {
    return virtualFields.get(fieldName);
  }

  @Override
  public void registerLookupField(
      String asField, String foreignTable, String localField, String foreignField) {
    lookupFields.put(
        asField, new SqlGenerationContext.LookupFieldInfo(foreignTable, localField, foreignField));
  }

  @Override
  public Expression getLookupSizeExpression(String fieldName) {
    SqlGenerationContext.LookupFieldInfo info = lookupFields.get(fieldName);
    if (info != null) {
      // Mark this lookup as consumed by $size - the JOIN won't be needed
      lookupsConsumedBySize.add(fieldName);
      return new LookupSizeExpression(
          info.foreignTable(), info.localField(), info.foreignField());
    }
    return null;
  }

  @Override
  public SqlGenerationContext.LookupFieldInfo getLookupFieldInfo(String fieldName) {
    return lookupFields.get(fieldName);
  }

  @Override
  public boolean isLookupConsumedBySize(String asField) {
    return lookupsConsumedBySize.contains(asField);
  }

  @Override
  public void registerLookupTableAlias(String asField, String tableAlias) {
    lookupTableAliases.put(asField, tableAlias);
  }

  @Override
  public String getLookupTableAlias(String fieldPath) {
    // Check if fieldPath starts with a lookup "as" field
    // e.g., "customer.tier" -> check if "customer" is a lookup field
    for (var entry : lookupTableAliases.entrySet()) {
      String asField = entry.getKey();
      if (fieldPath.equals(asField) || fieldPath.startsWith(asField + ".")) {
        return entry.getValue();
      }
    }
    return null;
  }

  @Override
  public String getLookupTableAliasByAs(String asField) {
    return lookupTableAliases.get(asField);
  }

  @Override
  public void registerPipelineLookupAlias(String asField, String tableAlias) {
    pipelineLookupAliases.put(asField, tableAlias);
  }

  @Override
  public String getPipelineLookupAlias(String asField) {
    return pipelineLookupAliases.get(asField);
  }

  @Override
  public void registerUnwoundPath(String path, String tableAlias) {
    unwoundPaths.put(path, tableAlias);
  }

  @Override
  public UnwindInfo getUnwindInfo(String fieldPath) {
    // Check if fieldPath starts with an unwound path
    // e.g., "items.product" matches unwound path "items"
    for (var entry : unwoundPaths.entrySet()) {
      String unwoundPath = entry.getKey();
      String tableAlias = entry.getValue();
      if (fieldPath.equals(unwoundPath)) {
        // Direct reference to the unwound array element itself
        return new UnwindInfo(tableAlias, "");
      } else if (fieldPath.startsWith(unwoundPath + ".")) {
        // Reference to a field within the unwound element
        String remainingPath = fieldPath.substring(unwoundPath.length() + 1);
        return new UnwindInfo(tableAlias, remainingPath);
      }
    }
    return null;
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

  @Override
  public void setJsonOutputMode(boolean jsonMode) {
    this.jsonOutputMode = jsonMode;
  }

  @Override
  public boolean isJsonOutputMode() {
    return jsonOutputMode;
  }

  @Override
  public void setNestedPipeline(boolean nested) {
    this.nestedPipeline = nested;
  }

  @Override
  public boolean isNestedPipeline() {
    return nestedPipeline;
  }

  @Override
  public void setInCteContext(boolean inCte) {
    this.inCteContext = inCte;
  }

  @Override
  public boolean isInCteContext() {
    return inCteContext;
  }

  @Override
  public void setCteSourceTable(String sourceTable) {
    this.cteSourceTable = sourceTable;
  }

  @Override
  public String getCteSourceTable() {
    return cteSourceTable;
  }

  @Override
  public void registerCompoundIdFields(Set<String> fields) {
    this.compoundIdFields = fields != null ? Set.copyOf(fields) : Collections.emptySet();
  }

  @Override
  public boolean isCompoundIdField(String fieldName) {
    return compoundIdFields.contains(fieldName);
  }
}
