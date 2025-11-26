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

    /**
     * Creates a sort stage with the given sort fields.
     *
     * @param sortFields the fields to sort by, in order
     */
    public SortStage(List<SortField> sortFields) {
        this.sortFields = sortFields != null
            ? new ArrayList<>(sortFields)
            : new ArrayList<>();
    }

    /**
     * Returns the sort fields as an unmodifiable list.
     */
    public List<SortField> getSortFields() {
        return Collections.unmodifiableList(sortFields);
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
