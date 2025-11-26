/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.AstNode;

/**
 * Base interface for all MongoDB aggregation pipeline stages.
 * Each stage translates to a specific SQL clause or subquery.
 */
public interface Stage extends AstNode {

    /**
     * Returns the MongoDB operator name for this stage (e.g., "$match", "$group").
     */
    String getOperatorName();
}
