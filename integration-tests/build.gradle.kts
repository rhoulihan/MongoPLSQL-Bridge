plugins {
    id("java")
}

dependencies {
    implementation(project(":core"))

    testImplementation(platform("org.junit:junit-bom:5.10.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.testcontainers:oracle-free:1.19.3")
    testImplementation("org.testcontainers:junit-jupiter:1.19.3")
    testImplementation("com.oracle.database.jdbc:ojdbc11:23.3.0.23.09")
    testImplementation("org.mongodb:bson:5.0.0")
}

tasks.test {
    useJUnitPlatform()

    // Integration tests need more time
    systemProperty("junit.jupiter.execution.timeout.default", "5m")

    // Testcontainers configuration
    systemProperty("testcontainers.reuse.enable", "true")

    // Only run if explicitly requested or in CI
    onlyIf {
        project.hasProperty("runIntegrationTests") || System.getenv("CI") != null
    }
}

// Disable coverage verification for integration tests (different thresholds)
tasks.jacocoTestCoverageVerification {
    enabled = false
}
