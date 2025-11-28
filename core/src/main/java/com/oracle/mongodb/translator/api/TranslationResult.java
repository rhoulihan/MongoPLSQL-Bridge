/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Result of translating a MongoDB aggregation pipeline to Oracle SQL. */
public final class TranslationResult {

  private final String sql;
  private final List<Object> bindVariables;
  private final List<TranslationWarning> warnings;
  private final TranslationCapability capability;

  private TranslationResult(
      String sql,
      List<Object> bindVariables,
      List<TranslationWarning> warnings,
      TranslationCapability capability) {
    this.sql = Objects.requireNonNull(sql, "sql must not be null");
    this.bindVariables = Collections.unmodifiableList(new ArrayList<>(bindVariables));
    this.warnings = Collections.unmodifiableList(new ArrayList<>(warnings));
    this.capability = Objects.requireNonNull(capability, "capability must not be null");
  }

  /** Creates a translation result with SQL and bind variables. */
  public static TranslationResult of(String sql, List<Object> bindVariables) {
    return new TranslationResult(sql, bindVariables, List.of(), TranslationCapability.FULL_SUPPORT);
  }

  /** Creates a translation result with SQL, bind variables, and warnings. */
  public static TranslationResult of(
      String sql, List<Object> bindVariables, List<TranslationWarning> warnings) {
    return new TranslationResult(sql, bindVariables, warnings, TranslationCapability.FULL_SUPPORT);
  }

  /** Creates a translation result with all attributes. */
  public static TranslationResult of(
      String sql,
      List<Object> bindVariables,
      List<TranslationWarning> warnings,
      TranslationCapability capability) {
    return new TranslationResult(sql, bindVariables, warnings, capability);
  }

  /** Returns the generated Oracle SQL statement. */
  public String sql() {
    return sql;
  }

  /** Returns the bind variables in order. */
  public List<Object> bindVariables() {
    return bindVariables;
  }

  /** Returns any warnings generated during translation. */
  public List<TranslationWarning> warnings() {
    return warnings;
  }

  /** Returns true if there are any warnings. */
  public boolean hasWarnings() {
    return !warnings.isEmpty();
  }

  /** Returns the translation capability level. */
  public TranslationCapability capability() {
    return capability;
  }

  @Override
  public String toString() {
    return "TranslationResult{sql='"
        + sql
        + "', bindVariables="
        + bindVariables
        + ", warnings="
        + warnings
        + ", capability="
        + capability
        + "}";
  }
}
