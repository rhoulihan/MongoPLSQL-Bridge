/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage.ProjectionField;
import org.bson.Document;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parser for $project stage documents.
 */
public final class ProjectStageParser {

    /**
     * Parses a $project stage document.
     *
     * @param projectDoc the $project document
     * @return ProjectStage AST node
     */
    public ProjectStage parse(Document projectDoc) {
        Map<String, ProjectionField> projections = new LinkedHashMap<>();
        boolean hasExclusion = false;
        boolean hasInclusion = false;

        for (Map.Entry<String, Object> entry : projectDoc.entrySet()) {
            String fieldName = entry.getKey();
            Object value = entry.getValue();

            ProjectionField field = parseProjectionValue(fieldName, value);
            projections.put(fieldName, field);

            if (field.isExcluded()) {
                hasExclusion = true;
            } else {
                hasInclusion = true;
            }
        }

        // MongoDB doesn't allow mixing inclusion and exclusion except for _id
        boolean isExclusionMode = hasExclusion && !hasInclusion;

        return new ProjectStage(projections, isExclusionMode);
    }

    private ProjectionField parseProjectionValue(String fieldName, Object value) {
        // Handle numeric inclusion/exclusion: 1, 0, true, false
        if (value instanceof Number numVal) {
            int intVal = numVal.intValue();
            if (intVal == 0) {
                return ProjectionField.exclude();
            }
            // Include the field as-is
            return ProjectionField.include(FieldPathExpression.of(fieldName));
        }

        if (value instanceof Boolean boolVal) {
            if (!boolVal) {
                return ProjectionField.exclude();
            }
            return ProjectionField.include(FieldPathExpression.of(fieldName));
        }

        // Handle string field reference: "$existingField"
        if (value instanceof String strVal) {
            if (strVal.startsWith("$")) {
                return ProjectionField.include(FieldPathExpression.of(strVal.substring(1)));
            }
            // Literal string value
            return ProjectionField.include(LiteralExpression.of(strVal));
        }

        // Handle expression document
        if (value instanceof Document) {
            Expression expr = parseExpression((Document) value);
            return ProjectionField.include(expr);
        }

        throw new IllegalArgumentException(
            "Unsupported projection value for field '" + fieldName + "': " + value);
    }

    private Expression parseExpression(Document exprDoc) {
        // For now, support simple field references and literals
        // More complex expressions (arithmetic, conditionals) will be added later
        if (exprDoc.size() == 1) {
            Map.Entry<String, Object> entry = exprDoc.entrySet().iterator().next();
            String op = entry.getKey();
            Object arg = entry.getValue();

            // Check if it's a known expression operator
            if (op.startsWith("$")) {
                // TODO: Support arithmetic expressions ($add, $subtract, etc.)
                // TODO: Support conditional expressions ($cond, $ifNull, etc.)
                throw new IllegalArgumentException(
                    "Expression operator not yet supported: " + op);
            }
        }

        throw new IllegalArgumentException("Unsupported expression: " + exprDoc);
    }
}
