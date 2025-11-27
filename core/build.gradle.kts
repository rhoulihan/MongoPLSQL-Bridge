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

    // Annotations
    compileOnly("com.github.spotbugs:spotbugs-annotations:4.8.3")

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

// Task to translate a pipeline from command line
tasks.register<JavaExec>("translatePipeline") {
    group = "application"
    description = "Translate a MongoDB pipeline to Oracle SQL"
    mainClass.set("com.oracle.mongodb.translator.cli.TranslateCli")
    classpath = sourceSets["main"].runtimeClasspath

    doFirst {
        val argsList = mutableListOf<String>()
        val collectionName = project.findProperty("collectionName")?.toString() ?: ""
        val pipelineFile = project.findProperty("pipelineFile")?.toString() ?: ""
        val pipelineJson = project.findProperty("pipelineJson")?.toString() ?: ""

        if (collectionName.isNotEmpty()) {
            argsList.add(collectionName)
        }
        if (pipelineFile.isNotEmpty()) {
            argsList.add("--file")
            argsList.add(pipelineFile)
        } else if (pipelineJson.isNotEmpty()) {
            argsList.add(pipelineJson)
        }
        args = argsList
    }
}
