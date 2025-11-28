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

  private TranslationOptions(Builder builder) {
    this.inlineBindVariables = builder.inlineBindVariables;
    this.prettyPrint = builder.prettyPrint;
    this.includeHints = builder.includeHints;
    this.strictMode = builder.strictMode;
    this.dataColumnName = builder.dataColumnName != null ? builder.dataColumnName : "data";
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

  /** Builder for TranslationOptions. */
  public static final class Builder {
    private boolean inlineBindVariables;
    private boolean prettyPrint;
    private boolean includeHints = true;
    private boolean strictMode;
    private String dataColumnName;

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

    public TranslationOptions build() {
      return new TranslationOptions(this);
    }
  }
}
