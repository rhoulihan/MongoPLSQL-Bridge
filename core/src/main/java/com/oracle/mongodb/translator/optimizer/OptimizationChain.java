/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.optimizer;

import com.oracle.mongodb.translator.ast.stage.Pipeline;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Chains multiple pipeline optimizers together.
 *
 * <p>The optimization chain applies each optimizer in sequence, passing the output of one optimizer
 * as input to the next.
 *
 * <p>Example usage:
 *
 * <pre>
 * OptimizationChain chain = OptimizationChain.standard();
 * Pipeline optimized = chain.optimize(pipeline);
 * </pre>
 */
public class OptimizationChain implements PipelineOptimizer {

  private final List<PipelineOptimizer> optimizers;

  private OptimizationChain(List<PipelineOptimizer> optimizers) {
    this.optimizers = new ArrayList<>(optimizers);
  }

  /**
   * Creates the standard optimization chain with all built-in optimizers.
   *
   * <p>The standard chain includes:
   *
   * <ol>
   *   <li>Predicate pushdown - moves $match stages early
   *   <li>Sort-limit combination - optimizes Top-N queries
   * </ol>
   */
  public static OptimizationChain standard() {
    return builder().add(new PredicatePushdownOptimizer()).add(new SortLimitOptimizer()).build();
  }

  /** Creates a builder for constructing a custom optimization chain. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public Pipeline optimize(Pipeline pipeline) {
    Pipeline result = pipeline;
    for (PipelineOptimizer optimizer : optimizers) {
      result = optimizer.optimize(result);
    }
    return result;
  }

  @Override
  public String getName() {
    return "chain(" + getOptimizerNames().stream().collect(Collectors.joining(", ")) + ")";
  }

  /** Returns the names of all optimizers in this chain. */
  public List<String> getOptimizerNames() {
    return optimizers.stream().map(PipelineOptimizer::getName).collect(Collectors.toList());
  }

  /** Builder for constructing optimization chains. */
  public static class Builder {
    private final List<PipelineOptimizer> optimizers = new ArrayList<>();

    private Builder() {}

    /** Adds an optimizer to the chain. */
    public Builder add(PipelineOptimizer optimizer) {
      optimizers.add(optimizer);
      return this;
    }

    /** Builds the optimization chain. */
    public OptimizationChain build() {
      return new OptimizationChain(optimizers);
    }
  }
}
