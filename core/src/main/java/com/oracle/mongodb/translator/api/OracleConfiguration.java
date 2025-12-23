/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

import java.util.Objects;

/** Configuration for Oracle database connection and translation settings. */
public final class OracleConfiguration {

  private final String collectionName;
  private final String schemaName;
  private final String dataColumnName;
  private final boolean useCteBasedRendering;

  private OracleConfiguration(Builder builder) {
    this.collectionName =
        Objects.requireNonNull(builder.collectionName, "collectionName must not be null");
    this.schemaName = builder.schemaName;
    this.dataColumnName = builder.dataColumnName != null ? builder.dataColumnName : "data";
    this.useCteBasedRendering = builder.useCteBasedRendering;
  }

  /** Returns a new builder. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns the MongoDB collection name (maps to Oracle table). */
  public String collectionName() {
    return collectionName;
  }

  /** Returns the Oracle schema name, or null if default. */
  public String schemaName() {
    return schemaName;
  }

  /** Returns the name of the JSON data column. */
  public String dataColumnName() {
    return dataColumnName;
  }

  /**
   * Returns true if CTE-based rendering should be used.
   *
   * <p>CTE-based rendering follows Oracle MongoDB API patterns:
   * <ul>
   *   <li>Uses WITH clause where each pipeline stage becomes a CTE</li>
   *   <li>Uses JSON_EXISTS with type-safe predicates (stringOnly, numberOnly, etc.)</li>
   *   <li>Uses json_transform for projections (KEEP, SET, REMOVE)</li>
   *   <li>Returns DATA column preserving JSON types</li>
   * </ul>
   */
  public boolean useCteBasedRendering() {
    return useCteBasedRendering;
  }

  /** Returns the fully qualified table name (schema.collection or just collection). */
  public String qualifiedTableName() {
    if (schemaName != null) {
      return schemaName + "." + collectionName;
    }
    return collectionName;
  }

  /** Builder for OracleConfiguration. */
  public static final class Builder {
    private String collectionName;
    private String schemaName;
    private String dataColumnName;
    private boolean useCteBasedRendering;

    private Builder() {}

    public Builder collectionName(String collectionName) {
      this.collectionName = collectionName;
      return this;
    }

    public Builder schemaName(String schemaName) {
      this.schemaName = schemaName;
      return this;
    }

    public Builder dataColumnName(String dataColumnName) {
      this.dataColumnName = dataColumnName;
      return this;
    }

    /**
     * Enables CTE-based rendering following Oracle MongoDB API patterns.
     *
     * @param useCteBasedRendering true to enable CTE-based rendering
     * @return this builder
     */
    public Builder useCteBasedRendering(boolean useCteBasedRendering) {
      this.useCteBasedRendering = useCteBasedRendering;
      return this;
    }

    public OracleConfiguration build() {
      return new OracleConfiguration(this);
    }
  }
}
