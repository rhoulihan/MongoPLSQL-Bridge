/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.Stage;

/**
 * Interface for stage-specific parsers.
 *
 * @param <T> the type of stage this parser produces
 */
@FunctionalInterface
public interface StageParser<T extends Stage> {

    /**
     * Parses the stage value into an AST node.
     *
     * @param value the value from the BSON document
     * @return the parsed stage AST node
     */
    T parse(Object value);
}
