/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registry of stage parsers by operator name.
 */
public final class StageParserRegistry {

    private final Map<String, StageParser<?>> parsers = new HashMap<>();

    public StageParserRegistry() {
        registerBuiltInParsers();
    }

    private void registerBuiltInParsers() {
        // Tier 1 simple stages
        register("$limit", value -> new LimitStage(toInt(value)));
        register("$skip", value -> new SkipStage(toInt(value)));
    }

    /**
     * Registers a stage parser for the given operator name.
     */
    public void register(String operatorName, StageParser<?> parser) {
        parsers.put(operatorName, parser);
    }

    /**
     * Returns the parser for the given operator name, or null if not found.
     */
    public StageParser<?> getParser(String operatorName) {
        return parsers.get(operatorName);
    }

    /**
     * Returns true if a parser is registered for the given operator name.
     */
    public boolean hasParser(String operatorName) {
        return parsers.containsKey(operatorName);
    }

    /**
     * Returns the set of registered operator names.
     */
    public Set<String> getRegisteredOperators() {
        return Collections.unmodifiableSet(parsers.keySet());
    }

    private static int toInt(Object value) {
        if (value instanceof Integer intVal) {
            return intVal;
        }
        if (value instanceof Long longVal) {
            return longVal.intValue();
        }
        if (value instanceof Number numVal) {
            return numVal.intValue();
        }
        throw new IllegalArgumentException(
            "Expected numeric value, got: " + (value == null ? "null" : value.getClass().getName()));
    }
}
