/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.api;

import java.util.Objects;

/**
 * Configuration for Oracle database connection and translation settings.
 */
public final class OracleConfiguration {

    private final String collectionName;
    private final String schemaName;
    private final String dataColumnName;

    private OracleConfiguration(Builder builder) {
        this.collectionName = Objects.requireNonNull(
            builder.collectionName, "collectionName must not be null");
        this.schemaName = builder.schemaName;
        this.dataColumnName = builder.dataColumnName != null ? builder.dataColumnName : "data";
    }

    /**
     * Returns a new builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the MongoDB collection name (maps to Oracle table).
     */
    public String collectionName() {
        return collectionName;
    }

    /**
     * Returns the Oracle schema name, or null if default.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * Returns the name of the JSON data column.
     */
    public String dataColumnName() {
        return dataColumnName;
    }

    /**
     * Returns the fully qualified table name (schema.collection or just collection).
     */
    public String qualifiedTableName() {
        if (schemaName != null) {
            return schemaName + "." + collectionName;
        }
        return collectionName;
    }

    /**
     * Builder for OracleConfiguration.
     */
    public static final class Builder {
        private String collectionName;
        private String schemaName;
        private String dataColumnName;

        private Builder() {
        }

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

        public OracleConfiguration build() {
            return new OracleConfiguration(this);
        }
    }
}
