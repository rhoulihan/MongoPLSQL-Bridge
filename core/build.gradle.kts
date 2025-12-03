plugins {
    id("java-library")
    id("com.github.spotbugs")
}

dependencies {
    // Oracle JDBC
    implementation("com.oracle.database.jdbc:ojdbc11:23.3.0.23.09")

    // MongoDB BSON for document handling
    implementation("org.mongodb:bson:5.0.0")

    // JSON processing
    implementation("com.fasterxml.jackson.core:jackson-databind:2.16.0")

    // Annotations (needed for both main and test compilation)
    compileOnly("com.github.spotbugs:spotbugs-annotations:4.8.3")
    testCompileOnly("com.github.spotbugs:spotbugs-annotations:4.8.3")

    // Testing
    testImplementation(platform("org.junit:junit-bom:5.10.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testImplementation("org.mockito:mockito-core:5.8.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.8.0")
}

spotbugs {
    effort.set(com.github.spotbugs.snom.Effort.MAX)
    reportLevel.set(com.github.spotbugs.snom.Confidence.MEDIUM)
    excludeFilter.set(file("${rootProject.projectDir}/config/spotbugs/exclude.xml"))
}

tasks.spotbugsMain {
    reports.create("html") {
        required.set(true)
    }
    reports.create("xml") {
        required.set(true)
    }
}

tasks.spotbugsTest {
    enabled = false
}

dependencies {
    spotbugsPlugins("com.h3xstream.findsecbugs:findsecbugs-plugin:1.12.0")
}

// CLI main class configuration
val cliMainClass = "com.oracle.mongodb.translator.cli.TranslatorCli"

// Fat JAR for standalone CLI usage
tasks.register<Jar>("fatJar") {
    group = "build"
    description = "Creates a fat JAR with all dependencies for CLI usage"
    archiveClassifier.set("all")
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    manifest {
        attributes["Main-Class"] = cliMainClass
    }

    from(sourceSets.main.get().output)
    dependsOn(configurations.runtimeClasspath)
    from({
        configurations.runtimeClasspath.get()
            .filter { it.name.endsWith("jar") }
            .map { zipTree(it) }
    })
}

// Task to run the CLI from Gradle
tasks.register<JavaExec>("translate") {
    group = "application"
    description = "Translate MongoDB aggregation pipelines to Oracle SQL"
    mainClass.set(cliMainClass)
    classpath = sourceSets["main"].runtimeClasspath
    standardInput = System.`in`

    // Pass all project properties as arguments
    doFirst {
        val argsList = mutableListOf<String>()

        // Support both old and new argument styles
        project.findProperty("file")?.toString()?.let { argsList.add(it) }
        project.findProperty("collection")?.toString()?.let {
            argsList.add("--collection")
            argsList.add(it)
        }
        if (project.findProperty("inline")?.toString()?.toBoolean() == true) {
            argsList.add("--inline")
        }
        if (project.findProperty("pretty")?.toString()?.toBoolean() == true) {
            argsList.add("--pretty")
        }
        project.findProperty("output")?.toString()?.let {
            argsList.add("--output")
            argsList.add(it)
        }

        args = argsList
    }
}
