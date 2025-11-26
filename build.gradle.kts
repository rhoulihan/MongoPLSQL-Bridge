plugins {
    id("java-library")
    id("jacoco")
    id("checkstyle")
    id("com.github.spotbugs") version "6.0.0" apply false
    id("org.owasp.dependencycheck") version "12.1.0" apply false
}

allprojects {
    group = "com.oracle.mongodb"
    version = "1.0.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "jacoco")
    apply(plugin = "checkstyle")

    java {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-processing", "-Werror"))
    }

    tasks.test {
        useJUnitPlatform()
        finalizedBy(tasks.jacocoTestReport)
    }

    tasks.jacocoTestReport {
        dependsOn(tasks.test)
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    tasks.jacocoTestCoverageVerification {
        violationRules {
            rule {
                element = "BUNDLE"
                limit {
                    counter = "LINE"
                    value = "COVEREDRATIO"
                    minimum = "0.80".toBigDecimal()
                }
                limit {
                    counter = "BRANCH"
                    value = "COVEREDRATIO"
                    minimum = "0.75".toBigDecimal()
                }
            }
        }
    }

    checkstyle {
        toolVersion = "10.12.5"
        configFile = file("${rootProject.projectDir}/config/checkstyle/google_checks.xml")
        isIgnoreFailures = false
        maxWarnings = 0
    }

    tasks.withType<Checkstyle> {
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }
}

// Root project tasks
tasks.register("checkAll") {
    dependsOn(subprojects.map { it.tasks.named("check") })
    description = "Run all checks across all subprojects"
    group = "verification"
}
