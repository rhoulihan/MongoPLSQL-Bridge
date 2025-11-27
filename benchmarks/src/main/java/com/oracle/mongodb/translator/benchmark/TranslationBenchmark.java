/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.benchmark;

import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.api.TranslationOptions;
import org.bson.Document;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for pipeline translation performance.
 *
 * <p>These benchmarks measure the translation time (excluding database execution)
 * for various MongoDB aggregation pipeline patterns.</p>
 *
 * <p>Run with: ./gradlew :benchmarks:jmh</p>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class TranslationBenchmark {

    private AggregationTranslator translator;
    private List<Document> simplePipeline;
    private List<Document> mediumPipeline;
    private List<Document> complexPipeline;
    private List<Document> joinPipeline;
    private List<Document> windowFunctionPipeline;

    @Setup
    public void setup() {
        var config = OracleConfiguration.builder()
            .collectionName("benchmark_collection")
            .build();

        var options = TranslationOptions.builder()
            .inlineBindVariables(false)
            .build();

        translator = AggregationTranslator.create(config, options);

        // Simple pipeline: match + limit
        simplePipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\"}}"),
            Document.parse("{\"$limit\": 10}")
        );

        // Medium pipeline: match + group + sort + limit
        mediumPipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\", \"amount\": {\"$gt\": 100}}}"),
            Document.parse("{\"$group\": {\"_id\": \"$category\", \"total\": {\"$sum\": \"$amount\"}, \"count\": {\"$count\": {}}}}"),
            Document.parse("{\"$sort\": {\"total\": -1}}"),
            Document.parse("{\"$limit\": 10}")
        );

        // Complex pipeline: match + project + group + sort + limit
        complexPipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\", \"createdAt\": {\"$gte\": \"2024-01-01\"}}}"),
            Document.parse("{\"$project\": {\"category\": 1, \"amount\": 1, \"year\": {\"$year\": \"$createdAt\"}, \"month\": {\"$month\": \"$createdAt\"}, \"upperName\": {\"$toUpper\": \"$name\"}}}"),
            Document.parse("{\"$group\": {\"_id\": {\"category\": \"$category\", \"year\": \"$year\"}, \"total\": {\"$sum\": \"$amount\"}, \"avg\": {\"$avg\": \"$amount\"}, \"min\": {\"$min\": \"$amount\"}, \"max\": {\"$max\": \"$amount\"}}}"),
            Document.parse("{\"$sort\": {\"total\": -1}}"),
            Document.parse("{\"$limit\": 20}")
        );

        // Join pipeline: lookup + unwind + match
        joinPipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\"}}"),
            Document.parse("{\"$lookup\": {\"from\": \"orders\", \"localField\": \"_id\", \"foreignField\": \"customerId\", \"as\": \"orders\"}}"),
            Document.parse("{\"$unwind\": \"$orders\"}"),
            Document.parse("{\"$match\": {\"orders.amount\": {\"$gt\": 100}}}"),
            Document.parse("{\"$group\": {\"_id\": \"$_id\", \"orderCount\": {\"$count\": {}}, \"totalAmount\": {\"$sum\": \"$orders.amount\"}}}"),
            Document.parse("{\"$sort\": {\"totalAmount\": -1}}"),
            Document.parse("{\"$limit\": 10}")
        );

        // Window function pipeline
        windowFunctionPipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\"}}"),
            Document.parse("{\"$setWindowFields\": {\"partitionBy\": \"$category\", \"sortBy\": {\"amount\": -1}, \"output\": {\"rank\": {\"$rank\": {}}, \"denseRank\": {\"$denseRank\": {}}, \"runningTotal\": {\"$sum\": \"$amount\", \"window\": {\"documents\": [\"unbounded\", \"current\"]}}}}}"),
            Document.parse("{\"$match\": {\"rank\": {\"$lte\": 3}}}"),
            Document.parse("{\"$limit\": 50}")
        );
    }

    @Benchmark
    public void translateSimplePipeline(Blackhole bh) {
        bh.consume(translator.translate(simplePipeline));
    }

    @Benchmark
    public void translateMediumPipeline(Blackhole bh) {
        bh.consume(translator.translate(mediumPipeline));
    }

    @Benchmark
    public void translateComplexPipeline(Blackhole bh) {
        bh.consume(translator.translate(complexPipeline));
    }

    @Benchmark
    public void translateJoinPipeline(Blackhole bh) {
        bh.consume(translator.translate(joinPipeline));
    }

    @Benchmark
    public void translateWindowFunctionPipeline(Blackhole bh) {
        bh.consume(translator.translate(windowFunctionPipeline));
    }
}
