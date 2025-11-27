/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.benchmark;

import com.oracle.mongodb.translator.ast.expression.*;
import com.oracle.mongodb.translator.ast.stage.*;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for SQL rendering performance.
 *
 * <p>These benchmarks measure the time to render AST nodes to SQL strings.</p>
 *
 * <p>Run with: ./gradlew :benchmarks:jmh</p>
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

    @Setup
    public void setup() {
        // Simple comparison: status = 'active'
        simpleComparison = new ComparisonExpression(
            ComparisonOp.EQ,
            FieldPathExpression.of("status"),
            LiteralExpression.of("active")
        );

        // Complex logical expression with nested conditions
        complexLogical = new LogicalExpression(
            LogicalOp.AND,
            List.of(
                new ComparisonExpression(ComparisonOp.EQ, FieldPathExpression.of("status"), LiteralExpression.of("active")),
                new LogicalExpression(LogicalOp.OR, List.of(
                    new ComparisonExpression(ComparisonOp.GT, FieldPathExpression.of("amount"), LiteralExpression.of(100)),
                    new ComparisonExpression(ComparisonOp.EQ, FieldPathExpression.of("priority"), LiteralExpression.of("high"))
                )),
                new ComparisonExpression(ComparisonOp.NE, FieldPathExpression.of("archived"), LiteralExpression.of(true))
            )
        );

        // Match stage with the complex expression
        matchStage = new MatchStage(complexLogical);

        // Group stage with multiple accumulators
        groupStage = new GroupStage(
            FieldPathExpression.of("category"),
            Map.of(
                "total", new AccumulatorExpression(AccumulatorOp.SUM, FieldPathExpression.of("amount")),
                "avg", new AccumulatorExpression(AccumulatorOp.AVG, FieldPathExpression.of("amount")),
                "count", new AccumulatorExpression(AccumulatorOp.COUNT, null),
                "min", new AccumulatorExpression(AccumulatorOp.MIN, FieldPathExpression.of("amount")),
                "max", new AccumulatorExpression(AccumulatorOp.MAX, FieldPathExpression.of("amount"))
            )
        );

        // Full pipeline
        fullPipeline = Pipeline.of("benchmark_collection",
            matchStage,
            groupStage,
            new SortStage(List.of(
                new SortStage.SortField(FieldPathExpression.of("total"), SortStage.SortDirection.DESC)
            )),
            new LimitStage(10)
        );
    }

    @Benchmark
    public void renderSimpleComparison(Blackhole bh) {
        var context = new DefaultSqlGenerationContext();
        simpleComparison.render(context);
        bh.consume(context.toSql());
    }

    @Benchmark
    public void renderComplexLogical(Blackhole bh) {
        var context = new DefaultSqlGenerationContext();
        complexLogical.render(context);
        bh.consume(context.toSql());
    }

    @Benchmark
    public void renderMatchStage(Blackhole bh) {
        var context = new DefaultSqlGenerationContext();
        matchStage.render(context);
        bh.consume(context.toSql());
    }

    @Benchmark
    public void renderGroupStage(Blackhole bh) {
        var context = new DefaultSqlGenerationContext();
        groupStage.render(context);
        bh.consume(context.toSql());
    }

    @Benchmark
    public void renderFullPipeline(Blackhole bh) {
        var context = new DefaultSqlGenerationContext();
        fullPipeline.render(context);
        bh.consume(context.toSql());
    }
}
