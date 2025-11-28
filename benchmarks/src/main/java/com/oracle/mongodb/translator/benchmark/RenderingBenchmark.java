/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.benchmark;

import com.oracle.mongodb.translator.ast.expression.AccumulatorExpression;
import com.oracle.mongodb.translator.ast.expression.AccumulatorOp;
import com.oracle.mongodb.translator.ast.expression.ComparisonExpression;
import com.oracle.mongodb.translator.ast.expression.ComparisonOp;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalExpression;
import com.oracle.mongodb.translator.ast.expression.LogicalOp;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * JMH benchmarks for SQL rendering performance.
 *
 * <p>These benchmarks measure the time to render AST nodes to SQL strings.
 *
 * <p>Run with: ./gradlew :benchmarks:jmh
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class RenderingBenchmark {

  private ComparisonExpression simpleComparison;
  private LogicalExpression complexLogical;
  private MatchStage matchStage;
  private GroupStage groupStage;
  private Pipeline fullPipeline;

  /** Initializes AST nodes for benchmark tests. */
  @Setup
  public void setup() {
    // Simple comparison: status = 'active'
    simpleComparison =
        new ComparisonExpression(
            ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active"));

    // Complex logical expression with nested conditions
    complexLogical =
        new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(
                    ComparisonOp.EQ,
                    FieldPathExpression.of("status"),
                    LiteralExpression.of("active")),
                new LogicalExpression(
                    LogicalOp.OR,
                    List.of(
                        new ComparisonExpression(
                            ComparisonOp.GT,
                            FieldPathExpression.of("amount"),
                            LiteralExpression.of(100)),
                        new ComparisonExpression(
                            ComparisonOp.EQ,
                            FieldPathExpression.of("priority"),
                            LiteralExpression.of("high")))),
                new ComparisonExpression(
                    ComparisonOp.NE,
                    FieldPathExpression.of("archived"),
                    LiteralExpression.of(true))));

    // Match stage with the complex expression
    matchStage = new MatchStage(complexLogical);

    // Group stage with multiple accumulators
    groupStage =
        new GroupStage(
            FieldPathExpression.of("category"),
            Map.of(
                "total",
                    new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("amount")),
                "avg",
                    new AccumulatorExpression(AccumulatorOp.AVG, FieldPathExpression.of("amount")),
                "count", new AccumulatorExpression(AccumulatorOp.COUNT, null),
                "min",
                    new AccumulatorExpression(AccumulatorOp.MIN, FieldPathExpression.of("amount")),
                "max",
                    new AccumulatorExpression(
                        AccumulatorOp.MAX, FieldPathExpression.of("amount"))));

    // Full pipeline
    fullPipeline =
        Pipeline.of(
            "benchmark_collection",
            matchStage,
            groupStage,
            new SortStage(
                List.of(
                    new SortStage.SortField(
                        FieldPathExpression.of("total"), SortStage.SortDirection.DESC))),
            new LimitStage(10));
  }

  /** Benchmarks rendering a simple comparison expression. */
  @Benchmark
  public void renderSimpleComparison(Blackhole bh) {
    var context = new DefaultSqlGenerationContext();
    simpleComparison.render(context);
    bh.consume(context.toSql());
  }

  /** Benchmarks rendering a complex logical expression. */
  @Benchmark
  public void renderComplexLogical(Blackhole bh) {
    var context = new DefaultSqlGenerationContext();
    complexLogical.render(context);
    bh.consume(context.toSql());
  }

  /** Benchmarks rendering a match stage. */
  @Benchmark
  public void renderMatchStage(Blackhole bh) {
    var context = new DefaultSqlGenerationContext();
    matchStage.render(context);
    bh.consume(context.toSql());
  }

  /** Benchmarks rendering a group stage. */
  @Benchmark
  public void renderGroupStage(Blackhole bh) {
    var context = new DefaultSqlGenerationContext();
    groupStage.render(context);
    bh.consume(context.toSql());
  }

  /** Benchmarks rendering a full pipeline. */
  @Benchmark
  public void renderFullPipeline(Blackhole bh) {
    var context = new DefaultSqlGenerationContext();
    fullPipeline.render(context);
    bh.consume(context.toSql());
  }
}
