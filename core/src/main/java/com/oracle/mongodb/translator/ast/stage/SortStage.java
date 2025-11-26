/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a $sort stage that orders documents.
 * Translates to Oracle's ORDER BY clause.
 */
public final class SortStage implements Stage {

    private final List<SortField> sortFields;
    private final Integer limitHint;

    /**
     * Creates a sort stage with the given sort fields.
     *
     * @param sortFields the fields to sort by, in order
     */
    public SortStage(List<SortField> sortFields) {
        this(sortFields, null);
    }

    /**
     * Creates a sort stage with the given sort fields and optional limit hint.
     *
     * @param sortFields the fields to sort by, in order
     * @param limitHint optional limit hint for Top-N optimization
     */
    public SortStage(List<SortField> sortFields, Integer limitHint) {
        this.sortFields = sortFields != null
            ? new ArrayList<>(sortFields)
            : new ArrayList<>();
        this.limitHint = limitHint;
    }

    /**
     * Creates a sort stage from a Map specification (convenience method).
     *
     * @param sortSpec map of field names to sort direction (1 for ASC, -1 for DESC)
     */
    public SortStage(java.util.Map<String, Integer> sortSpec) {
        this.sortFields = new ArrayList<>();
        for (java.util.Map.Entry<String, Integer> entry : sortSpec.entrySet()) {
            this.sortFields.add(new SortField(
                FieldPathExpression.of(entry.getKey()),
                SortDirection.fromMongo(entry.getValue())
            ));
        }
        this.limitHint = null;
    }

    /**
     * Returns the sort fields as an unmodifiable list.
     */
    public List<SortField> getSortFields() {
        return Collections.unmodifiableList(sortFields);
    }

    /**
     * Returns the limit hint for Top-N optimization, or null if none.
     */
    public Integer getLimitHint() {
        return limitHint;
    }

    /**
     * Creates a copy of this sort stage with the specified limit hint.
     */
    public SortStage withLimitHint(Integer limit) {
        return new SortStage(this.sortFields, limit);
    }

    @Override
    public String getOperatorName() {
        return "$sort";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("ORDER BY ");

        boolean first = true;
        for (SortField field : sortFields) {
            if (!first) {
                ctx.sql(", ");
            }
            ctx.visit(field.getFieldPath());
            if (field.getDirection() == SortDirection.DESC) {
                ctx.sql(" DESC");
            }
            first = false;
        }
    }

    @Override
    public String toString() {
        return "SortStage(" + sortFields + ")";
    }

    /**
     * Represents a single field in a sort specification.
     */
    public static final class SortField {
        private final FieldPathExpression fieldPath;
        private final SortDirection direction;

        public SortField(FieldPathExpression fieldPath, SortDirection direction) {
            this.fieldPath = Objects.requireNonNull(fieldPath, "fieldPath must not be null");
            this.direction = Objects.requireNonNull(direction, "direction must not be null");
        }

        public FieldPathExpression getFieldPath() {
            return fieldPath;
        }

        public SortDirection getDirection() {
            return direction;
        }

        @Override
        public String toString() {
            return fieldPath + " " + direction;
        }
    }

    /**
     * Sort direction.
     */
    public enum SortDirection {
        ASC(1),
        DESC(-1);

        private final int mongoValue;

        SortDirection(int mongoValue) {
            this.mongoValue = mongoValue;
        }

        public int getMongoValue() {
            return mongoValue;
        }

        public static SortDirection fromMongo(int value) {
            if (value >= 1) {
                return ASC;
            }
            if (value <= -1) {
                return DESC;
            }
            throw new IllegalArgumentException("Invalid sort direction: " + value);
        }
    }
}
