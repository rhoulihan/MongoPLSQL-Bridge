# Performance Benchmarks

This module contains JMH (Java Microbenchmark Harness) benchmarks for measuring the performance of the MongoDB to Oracle SQL translator.

## Running Benchmarks

### Full Benchmark Suite

Run all benchmarks with standard warmup and measurement iterations:

```bash
./gradlew :benchmarks:jmh
```

Results will be written to `benchmarks/build/reports/jmh/results.json`.

### Quick Benchmark

For development/testing, run with minimal warmup:

```bash
./gradlew :benchmarks:benchmarkQuick
```

### Single Benchmark

Run a specific benchmark class:

```bash
./gradlew :benchmarks:jmh -Pjmh.includes="TranslationBenchmark"
```

## Benchmark Categories

### TranslationBenchmark

Measures end-to-end translation time from BSON pipeline to Oracle SQL:

| Benchmark | Description |
|-----------|-------------|
| `translateSimplePipeline` | Simple $match + $limit |
| `translateMediumPipeline` | $match + $group + $sort + $limit |
| `translateComplexPipeline` | Multiple stages with projections and date operators |
| `translateJoinPipeline` | $lookup + $unwind + grouping |
| `translateWindowFunctionPipeline` | $setWindowFields with multiple window functions |

### ParsingBenchmark

Measures BSON parsing and AST construction time:

| Benchmark | Description |
|-----------|-------------|
| `parseSimpleExpression` | Single field equality |
| `parseComparisonExpression` | Multiple comparisons ($gt, $lte, $in) |
| `parseLogicalExpression` | Nested $and/$or logic |
| `parseNestedExpression` | Deeply nested boolean expressions |
| `parsePipeline` | Full 5-stage pipeline |

### RenderingBenchmark

Measures SQL generation time from AST nodes:

| Benchmark | Description |
|-----------|-------------|
| `renderSimpleComparison` | Single comparison expression |
| `renderComplexLogical` | Nested logical expression |
| `renderMatchStage` | WHERE clause generation |
| `renderGroupStage` | GROUP BY with multiple accumulators |
| `renderFullPipeline` | Complete pipeline rendering |

## Understanding Results

JMH output includes:

- **Score**: Average time per operation (lower is better)
- **Error**: 99.9% confidence interval
- **Units**: Time unit (μs = microseconds, ns = nanoseconds)

Example output:
```
Benchmark                                       Mode  Cnt    Score    Error  Units
TranslationBenchmark.translateSimplePipeline    avgt    5   45.234 ±  2.156  us/op
TranslationBenchmark.translateMediumPipeline    avgt    5  123.567 ±  5.432  us/op
```

## Performance Targets

Typical translation times on modern hardware:

| Pipeline Complexity | Target |
|---------------------|--------|
| Simple (2 stages) | < 100 μs |
| Medium (4 stages) | < 200 μs |
| Complex (5+ stages) | < 500 μs |
| With joins | < 1 ms |

## Profiling

To generate flame graphs, use async-profiler:

```bash
./gradlew :benchmarks:jmh -Pjmh.profilers="async:libPath=/path/to/libasyncProfiler.so;output=flamegraph"
```
