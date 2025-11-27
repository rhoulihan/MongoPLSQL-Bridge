/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.MergeStage;
import com.oracle.mongodb.translator.ast.stage.MergeStage.WhenMatched;
import com.oracle.mongodb.translator.ast.stage.MergeStage.WhenNotMatched;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;

/**
 * Parser for the MongoDB $merge stage.
 *
 * <p>Expected format:
 * <pre>
 * // Simple form
 * { $merge: "targetCollection" }
 *
 * // Document form
 * {
 *   $merge: {
 *     into: "targetCollection",
 *     on: "_id",
 *     whenMatched: "replace",
 *     whenNotMatched: "insert"
 *   }
 * }
 * </pre>
 */
public final class MergeStageParser {

    /**
     * Parses a $merge stage from the given value.
     *
     * @param value the stage value (String or Document)
     * @return the parsed MergeStage
     * @throws IllegalArgumentException if the value is invalid
     */
    public MergeStage parse(Object value) {
        if (value instanceof String targetCollection) {
            return new MergeStage(targetCollection);
        }

        if (value instanceof Document doc) {
            return parseDocumentForm(doc);
        }

        throw new IllegalArgumentException(
            "$merge requires a string or document, got: "
                + (value == null ? "null" : value.getClass().getSimpleName()));
    }

    private MergeStage parseDocumentForm(Document doc) {
        // Parse 'into' field - required
        String targetCollection = parseIntoField(doc);

        // Parse 'on' field - optional (defaults to "_id")
        List<String> onFields = parseOnField(doc);

        // Parse 'whenMatched' field - optional
        WhenMatched whenMatched = parseWhenMatched(doc);

        // Parse 'whenNotMatched' field - optional
        WhenNotMatched whenNotMatched = parseWhenNotMatched(doc);

        return new MergeStage(targetCollection, onFields, whenMatched, whenNotMatched);
    }

    private String parseIntoField(Document doc) {
        Object into = doc.get("into");
        if (into == null) {
            throw new IllegalArgumentException("$merge requires 'into' field");
        }

        if (into instanceof String) {
            return (String) into;
        }

        if (into instanceof Document intoDoc) {
            Object coll = intoDoc.get("coll");
            if (coll instanceof String) {
                return (String) coll;
            }
            throw new IllegalArgumentException("$merge 'into.coll' must be a string");
        }

        throw new IllegalArgumentException(
            "$merge 'into' must be a string or document, got: " + into.getClass().getSimpleName());
    }

    private List<String> parseOnField(Document doc) {
        Object on = doc.get("on");
        if (on == null) {
            return List.of("_id");
        }

        if (on instanceof String) {
            return List.of((String) on);
        }

        if (on instanceof List<?> list) {
            List<String> result = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof String) {
                    result.add((String) item);
                } else {
                    throw new IllegalArgumentException(
                        "$merge 'on' array must contain strings, got: " + item.getClass().getSimpleName());
                }
            }
            return result;
        }

        throw new IllegalArgumentException(
            "$merge 'on' must be a string or array, got: " + on.getClass().getSimpleName());
    }

    private WhenMatched parseWhenMatched(Document doc) {
        Object value = doc.get("whenMatched");
        if (value == null) {
            return WhenMatched.MERGE;
        }

        if (value instanceof String str) {
            return switch (str.toLowerCase()) {
                case "replace" -> WhenMatched.REPLACE;
                case "keepexisting" -> WhenMatched.KEEP_EXISTING;
                case "merge" -> WhenMatched.MERGE;
                case "fail" -> WhenMatched.FAIL;
                default -> throw new IllegalArgumentException("Unknown whenMatched value: " + str);
            };
        }

        // Pipeline form not supported yet
        throw new IllegalArgumentException(
            "$merge 'whenMatched' pipeline not yet supported");
    }

    private WhenNotMatched parseWhenNotMatched(Document doc) {
        Object value = doc.get("whenNotMatched");
        if (value == null) {
            return WhenNotMatched.INSERT;
        }

        if (value instanceof String str) {
            return switch (str.toLowerCase()) {
                case "insert" -> WhenNotMatched.INSERT;
                case "discard" -> WhenNotMatched.DISCARD;
                case "fail" -> WhenNotMatched.FAIL;
                default -> throw new IllegalArgumentException("Unknown whenNotMatched value: " + str);
            };
        }

        throw new IllegalArgumentException(
            "$merge 'whenNotMatched' must be a string, got: " + value.getClass().getSimpleName());
    }
}
