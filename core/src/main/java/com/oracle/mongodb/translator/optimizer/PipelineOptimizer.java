/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.optimizer;

import com.oracle.mongodb.translator.ast.stage.Pipeline;

/**
 * Interface for pipeline optimizers.
 *
 * <p>Pipeline optimizers transform aggregation pipelines to improve
 * their execution performance. Each optimizer focuses on a specific
 * optimization strategy.
 */
public interface PipelineOptimizer {

    /**
     * Optimizes the given pipeline.
     *
     * @param pipeline the pipeline to optimize
     * @return the optimized pipeline (may be the same instance if no changes)
     */
    Pipeline optimize(Pipeline pipeline);

    /**
     * Returns the name of this optimizer for logging/debugging.
     */
    String getName();
}
