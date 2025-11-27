plugins {
    id("java")
    id("me.champeau.jmh") version "0.7.2"
}

dependencies {
    implementation(project(":core"))

    // MongoDB BSON
    implementation("org.mongodb:bson:5.0.0")

    // JMH - needed for both main and jmh source sets
    implementation("org.openjdk.jmh:jmh-core:1.37")
    annotationProcessor("org.openjdk.jmh:jmh-generator-annprocess:1.37")
}

jmh {
    warmupIterations.set(2)
    iterations.set(5)
    fork.set(1)
    includes.set(listOf(".*Benchmark"))
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("reports/jmh/results.json"))
}

tasks.register("benchmarkQuick") {
    group = "benchmark"
    description = "Run benchmarks with minimal warmup for quick testing"
    dependsOn("jmh")
    doFirst {
        jmh.warmupIterations.set(1)
        jmh.iterations.set(2)
    }
}
