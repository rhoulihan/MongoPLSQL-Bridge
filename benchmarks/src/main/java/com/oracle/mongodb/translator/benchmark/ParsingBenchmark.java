/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.benchmark;

import com.oracle.mongodb.translator.parser.ExpressionParser;
import com.oracle.mongodb.translator.parser.PipelineParser;
import com.oracle.mongodb.translator.parser.StageParserRegistry;
import org.bson.Document;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for expression and pipeline parsing performance.
 *
 * <p>These benchmarks measure the parsing time for various expression and pipeline patterns.</p>
 *
 * <p>Run with: ./gradlew :benchmarks:jmh</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class ParsingBenchmark {

    private ExpressionParser expressionParser;
    private PipelineParser pipelineParser;

    private Document simpleExpression;
    private Document comparisonExpression;
    private Document logicalExpression;
    private Document nestedExpression;
    private List<Document> fullPipeline;

    @Setup
    public void setup() {
        expressionParser = new ExpressionParser();
        pipelineParser = new PipelineParser(new StageParserRegistry());

        // Simple field equality
        simpleExpression = Document.parse("{\"status\": \"active\"}");

        // Comparison expression
        comparisonExpression = Document.parse("""
            {
                "amount": {"$gt": 100},
                "quantity": {"$lte": 50},
                "category": {"$in": ["electronics", "clothing", "food"]}
            }
            """);

        // Logical expression with multiple conditions
        logicalExpression = Document.parse("""
            {
                "$and": [
                    {"status": "active"},
                    {"$or": [
                        {"amount": {"$gt": 100}},
                        {"priority": "high"}
                    ]},
                    {"category": {"$ne": "archived"}}
                ]
            }
            """);

        // Deeply nested expression
        nestedExpression = Document.parse("""
            {
                "$and": [
                    {"$or": [
                        {"$and": [{"a": 1}, {"b": 2}]},
                        {"$and": [{"c": 3}, {"d": 4}]}
                    ]},
                    {"$or": [
                        {"$and": [{"e": 5}, {"f": 6}]},
                        {"$and": [{"g": 7}, {"h": 8}]}
                    ]}
                ]
            }
            """);

        // Full pipeline
        fullPipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\", \"amount\": {\"$gt\": 100}}}"),
            Document.parse("{\"$project\": {\"category\": 1, \"amount\": 1, \"computed\": {\"$add\": [\"$amount\", 10]}}}"),
            Document.parse("{\"$group\": {\"_id\": \"$category\", \"total\": {\"$sum\": \"$amount\"}}}"),
            Document.parse("{\"$sort\": {\"total\": -1}}"),
            Document.parse("{\"$limit\": 10}")
        );
    }

    @Benchmark
    public void parseSimpleExpression(Blackhole bh) {
        bh.consume(expressionParser.parse(simpleExpression));
    }

    @Benchmark
    public void parseComparisonExpression(Blackhole bh) {
        bh.consume(expressionParser.parse(comparisonExpression));
    }

    @Benchmark
    public void parseLogicalExpression(Blackhole bh) {
        bh.consume(expressionParser.parse(logicalExpression));
    }

    @Benchmark
    public void parseNestedExpression(Blackhole bh) {
        bh.consume(expressionParser.parse(nestedExpression));
    }

    @Benchmark
    public void parsePipeline(Blackhole bh) {
        bh.consume(pipelineParser.parse("benchmark_collection", fullPipeline));
    }
}
