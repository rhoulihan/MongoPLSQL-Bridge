plugins {
    id("java")
}

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.0")
    implementation("com.github.spullara.mustache.java:compiler:0.9.11")

    testImplementation(platform("org.junit:junit-bom:5.10.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.24.2")
}

// Disable coverage verification for generator (utility module)
tasks.jacocoTestCoverageVerification {
    enabled = false
}
