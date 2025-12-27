/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

/** Options for controlling translation behavior. */
public final class TranslationOptions {

  private static final TranslationOptions DEFAULT = new Builder().build();

  private final boolean inlineBindVariables;
  private final boolean prettyPrint;
  private final boolean includeHints;
  private final boolean strictMode;
  private final String dataColumnName;
  private final boolean procedural;
  private final boolean autoProcedural;

  private TranslationOptions(Builder builder) {
    this.inlineBindVariables = builder.inlineBindVariables;
    this.prettyPrint = builder.prettyPrint;
    this.includeHints = builder.includeHints;
    this.strictMode = builder.strictMode;
    this.dataColumnName = builder.dataColumnName != null ? builder.dataColumnName : "data";
    this.procedural = builder.procedural;
    this.autoProcedural = builder.autoProcedural;
  }

  /** Returns the default translation options. */
  public static TranslationOptions defaults() {
    return DEFAULT;
  }

  /** Returns a new builder for creating custom options. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns true if bind variables should be inlined into the SQL. */
  public boolean inlineBindVariables() {
    return inlineBindVariables;
  }

  /** Returns true if the SQL should be pretty-printed. */
  public boolean prettyPrint() {
    return prettyPrint;
  }

  /** Returns true if optimizer hints should be included. */
  public boolean includeHints() {
    return includeHints;
  }

  /** Returns true if strict mode is enabled (throws on unsupported operators). */
  public boolean strictMode() {
    return strictMode;
  }

  /** Returns the name of the JSON data column. */
  public String dataColumnName() {
    return dataColumnName;
  }

  /**
   * Returns true if procedural mode is enabled.
   * In procedural mode, the SQL is generated as a series of CREATE TABLE statements
   * instead of a single WITH clause. This allows complex queries to execute without
   * exceeding Oracle's CTE complexity limits.
   */
  public boolean procedural() {
    return procedural;
  }

  /**
   * Returns true if auto-procedural mode is enabled.
   * When enabled, the translator will automatically detect complex queries
   * (based on CTE count) and use procedural mode when needed.
   */
  public boolean autoProcedural() {
    return autoProcedural;
  }

  /** Builder for TranslationOptions. */
  public static final class Builder {
    private boolean inlineBindVariables;
    private boolean prettyPrint;
    private boolean includeHints = true;
    private boolean strictMode;
    private String dataColumnName;
    private boolean procedural;
    private boolean autoProcedural;

    private Builder() {}

    public Builder inlineBindVariables(boolean inlineBindVariables) {
      this.inlineBindVariables = inlineBindVariables;
      return this;
    }

    public Builder prettyPrint(boolean prettyPrint) {
      this.prettyPrint = prettyPrint;
      return this;
    }

    public Builder includeHints(boolean includeHints) {
      this.includeHints = includeHints;
      return this;
    }

    public Builder strictMode(boolean strictMode) {
      this.strictMode = strictMode;
      return this;
    }

    public Builder dataColumnName(String dataColumnName) {
      this.dataColumnName = dataColumnName;
      return this;
    }

    /**
     * Enables procedural mode for complex queries.
     * When enabled, generates CREATE TABLE statements instead of CTEs.
     */
    public Builder procedural(boolean procedural) {
      this.procedural = procedural;
      return this;
    }

    /**
     * Enables auto-procedural mode.
     * When enabled, the translator will automatically detect complex queries
     * and use procedural mode when the number of CTEs exceeds the threshold.
     */
    public Builder autoProcedural(boolean autoProcedural) {
      this.autoProcedural = autoProcedural;
      return this;
    }

    public TranslationOptions build() {
      return new TranslationOptions(this);
    }
  }
}
