# MongoPLSQL-Bridge Implementation Plan

## Executive Summary

This document provides a comprehensive, executable implementation plan for the MongoDB to Oracle SQL Translation Library. The plan follows strict Test-Driven Development (TDD) methodology and is structured as sequential implementation tickets that can be executed by Claude Code.

**Target Outcome:** A production-ready Java library that translates MongoDB aggregation pipelines to Oracle SQL/JSON statements for execution on Oracle 26ai JSON Collections.

---

# Phase 1: Project Initialization

## Entry Criteria
- Empty project directory (or minimal existing files)
- Developer has JDK 17+ installed
- Docker available for local Oracle development

## Exit Criteria
- Complete Gradle multi-module build compiles successfully
- CI/CD pipeline passes on GitHub Actions
- Docker Compose spins up Oracle 26ai-free locally
- Pre-commit hooks installed and functional

---

## IMPL-001: Gradle Multi-Module Project Structure

**Phase:** 1
**Complexity:** M
**Dependencies:** None

### Description
Create the complete Gradle multi-module project structure with three modules: `core` (main library), `integration-tests` (Oracle integration tests), and `generator` (code generation from specs).

### Acceptance Criteria
- [ ] Test: `./gradlew build` compiles successfully
- [ ] Test: All three submodules are recognized
- [ ] Test: Dependencies resolve correctly
- [ ] Code coverage >= 80% configured

### Implementation

#### Step 1: Create Root build.gradle.kts

```kotlin
// build.gradle.kts (root)
plugins {
    id("java-library")
    id("jacoco")
    id("checkstyle")
    id("com.github.spotbugs") version "6.0.0"
    id("org.owasp.dependencycheck") version "12.1.0"
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
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(17))
        }
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
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
        toolVersion = "10.25.0"
        configFile = file("${rootProject.projectDir}/config/checkstyle/google_checks.xml")
        isIgnoreFailures = false
    }
}
```

#### Step 2: Create settings.gradle.kts

```kotlin
// settings.gradle.kts
rootProject.name = "mongo-oracle-translator"

include("core")
include("integration-tests")
include("generator")
```

#### Step 3: Create core module build.gradle.kts

```kotlin
// core/build.gradle.kts
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
}

tasks.spotbugsMain {
    reports.create("html") {
        required.set(true)
    }
}
```

#### Step 4: Create integration-tests module

```kotlin
// integration-tests/build.gradle.kts
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
}

tasks.test {
    useJUnitPlatform()

    // Integration tests need more time
    systemProperty("junit.jupiter.execution.timeout.default", "5m")

    // Testcontainers configuration
    systemProperty("testcontainers.reuse.enable", "true")
}
```

#### Step 5: Create generator module

```kotlin
// generator/build.gradle.kts
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
```

### Files
- CREATE: `build.gradle.kts`
- CREATE: `settings.gradle.kts`
- CREATE: `core/build.gradle.kts`
- CREATE: `integration-tests/build.gradle.kts`
- CREATE: `generator/build.gradle.kts`

### Definition of Done
- [ ] `./gradlew build` succeeds
- [ ] All modules compile
- [ ] No SpotBugs warnings

---

## IMPL-002: Directory Structure and Package Organization

**Phase:** 1
**Complexity:** S
**Dependencies:** IMPL-001

### Description
Create the complete directory structure matching the specification's package organization.

### Acceptance Criteria
- [ ] Test: All source directories exist
- [ ] Test: Package structure matches specification
- [ ] Placeholder classes compile

### Implementation

#### Directory Structure to Create

```
mongo-oracle-translator/
├── config/
│   └── checkstyle/
│       └── google_checks.xml
├── specs/
│   ├── operators.json
│   ├── type-mappings.json
│   ├── error-codes.json
│   ├── test-cases/
│   │   ├── match-operator.json
│   │   ├── group-operator.json
│   │   └── complex-pipelines.json
│   └── schemas/
│       └── operator-schema.json
├── core/
│   └── src/
│       ├── main/
│       │   └── java/
│       │       └── com/oracle/mongodb/translator/
│       │           ├── api/
│       │           ├── parser/
│       │           ├── ast/
│       │           │   ├── stage/
│       │           │   └── expression/
│       │           ├── optimizer/
│       │           ├── generator/
│       │           │   └── dialect/
│       │           ├── executor/
│       │           ├── validation/
│       │           └── exception/
│       └── test/
│           └── java/
│               └── com/oracle/mongodb/translator/
│                   ├── api/
│                   ├── parser/
│                   ├── ast/
│                   ├── optimizer/
│                   ├── generator/
│                   ├── executor/
│                   └── validation/
├── integration-tests/
│   └── src/
│       └── test/
│           ├── java/
│           │   └── com/oracle/mongodb/translator/
│           │       └── integration/
│           └── resources/
│               └── testdata/
├── generator/
│   └── src/
│       ├── main/
│       │   ├── java/
│       │   └── resources/
│       │       └── templates/
│       │           ├── java/
│       │           ├── nodejs/
│       │           └── python/
│       └── test/
│           └── java/
└── docs/
```

### Files
- CREATE: All directories as shown
- CREATE: `config/checkstyle/google_checks.xml` (Google Java Style)
- CREATE: Placeholder `package-info.java` files

### Definition of Done
- [ ] All directories created
- [ ] Checkstyle config present
- [ ] `./gradlew check` passes

---

## IMPL-003: Google Checkstyle Configuration

**Phase:** 1
**Complexity:** S
**Dependencies:** IMPL-002

### Description
Add the Google Java Style checkstyle configuration and verify it works.

### Acceptance Criteria
- [ ] Test: `./gradlew checkstyleMain` passes on empty project
- [ ] Test: Checkstyle catches style violations

### Implementation

Download the Google Checks XML from the official Checkstyle repository and place at `config/checkstyle/google_checks.xml`.

### Files
- CREATE: `config/checkstyle/google_checks.xml`
- CREATE: `config/checkstyle/suppressions.xml`

### Definition of Done
- [ ] Checkstyle configured
- [ ] Sample violation detected correctly

---

## IMPL-004: SpotBugs with FindSecBugs Configuration

**Phase:** 1
**Complexity:** S
**Dependencies:** IMPL-001

### Description
Configure SpotBugs with the FindSecBugs security plugin for static analysis.

### Acceptance Criteria
- [ ] Test: `./gradlew spotbugsMain` runs successfully
- [ ] Test: Security vulnerabilities are detected

### Implementation

#### Add to root build.gradle.kts

```kotlin
subprojects {
    apply(plugin = "com.github.spotbugs")

    dependencies {
        spotbugsPlugins("com.h3xstream.findsecbugs:findsecbugs-plugin:1.12.0")
    }
}
```

#### Create SpotBugs exclusion file

```xml
<!-- config/spotbugs/exclude.xml -->
<?xml version="1.0" encoding="UTF-8"?>
<FindBugsFilter>
    <!-- Exclude generated code -->
    <Match>
        <Package name="~com\.oracle\.mongodb\.translator\.generated\..*"/>
    </Match>
</FindBugsFilter>
```

### Files
- MODIFY: `build.gradle.kts`
- CREATE: `config/spotbugs/exclude.xml`

### Definition of Done
- [ ] SpotBugs runs
- [ ] FindSecBugs plugin active

---

## IMPL-005: OWASP Dependency Check Configuration

**Phase:** 1
**Complexity:** S
**Dependencies:** IMPL-001

### Description
Configure OWASP Dependency Check to scan for vulnerable dependencies.

### Acceptance Criteria
- [ ] Test: `./gradlew dependencyCheckAnalyze` completes
- [ ] Test: Report generated in build/reports

### Implementation

#### Add to root build.gradle.kts

```kotlin
dependencyCheck {
    failBuildOnCVSS = 7.0f
    suppressionFile = "config/owasp/suppressions.xml"

    analyzers {
        assemblyEnabled = false
        nodeEnabled = false
    }
}
```

### Files
- MODIFY: `build.gradle.kts`
- CREATE: `config/owasp/suppressions.xml`

### Definition of Done
- [ ] Dependency check runs
- [ ] No high-severity vulnerabilities

---

## IMPL-006: Pre-commit Hook Configuration

**Phase:** 1
**Complexity:** S
**Dependencies:** IMPL-003, IMPL-004

### Description
Set up pre-commit hooks for code quality checks before each commit.

### Acceptance Criteria
- [ ] Test: pre-commit install succeeds
- [ ] Test: Checkstyle runs on commit
- [ ] Test: SpotBugs runs on commit

### Implementation

#### Create .pre-commit-config.yaml

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: checkstyle
        name: Checkstyle
        entry: ./gradlew checkstyleMain checkstyleTest --no-daemon
        language: system
        pass_filenames: false
        types: [java]

      - id: spotbugs
        name: SpotBugs Security Check
        entry: ./gradlew spotbugsMain --no-daemon
        language: system
        pass_filenames: false
        types: [java]

      - id: unit-tests
        name: Unit Tests
        entry: ./gradlew test -x integrationTest --no-daemon
        language: system
        pass_filenames: false
        types: [java]

      - id: trailing-whitespace
        name: Trim Trailing Whitespace
        entry: trailing-whitespace-fixer
        language: python
        types: [text]

      - id: end-of-file-fixer
        name: Fix End of Files
        entry: end-of-file-fixer
        language: python
        types: [text]
```

### Files
- CREATE: `.pre-commit-config.yaml`

### Definition of Done
- [ ] Pre-commit hooks installed
- [ ] Hooks run on commit

---

## IMPL-007: GitHub Actions CI/CD Workflow

**Phase:** 1
**Complexity:** M
**Dependencies:** IMPL-001 through IMPL-006

### Description
Create the complete GitHub Actions workflow for CI/CD with Oracle Testcontainers.

### Acceptance Criteria
- [ ] Test: Workflow syntax valid
- [ ] Test: Matrix builds for Java 17 and 21
- [ ] Test: Oracle service container starts
- [ ] Test: All quality gates run

### Implementation

#### Create .github/workflows/ci.yml

```yaml
# .github/workflows/ci.yml
name: CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  JAVA_VERSION: '17'
  GRADLE_OPTS: "-Dorg.gradle.daemon=false"

jobs:
  validate-specs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install jsonschema
        run: pip install jsonschema

      - name: Validate specification files
        run: |
          python -c "
          import json
          import jsonschema
          from pathlib import Path

          # Validate operators.json exists and is valid JSON
          ops = json.loads(Path('specs/operators.json').read_text())
          print(f'Loaded {len(ops.get(\"operators\", {}))} operators')
          "

  build:
    needs: validate-specs
    runs-on: ubuntu-latest
    strategy:
      matrix:
        java-version: ['17', '21']

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK ${{ matrix.java-version }}
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: ${{ matrix.java-version }}
          cache: 'gradle'

      - name: Grant execute permission for gradlew
        run: chmod +x gradlew

      - name: Build with Gradle
        run: ./gradlew build -x test

      - name: Run Checkstyle
        run: ./gradlew checkstyleMain checkstyleTest

      - name: Run SpotBugs
        run: ./gradlew spotbugsMain

      - name: Upload build artifacts
        uses: actions/upload-artifact@v4
        with:
          name: build-artifacts-java${{ matrix.java-version }}
          path: core/build/libs/*.jar

  unit-tests:
    needs: build
    runs-on: ubuntu-latest
    strategy:
      matrix:
        java-version: ['17', '21']

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK ${{ matrix.java-version }}
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: ${{ matrix.java-version }}
          cache: 'gradle'

      - name: Run unit tests
        run: ./gradlew :core:test

      - name: Generate coverage report
        run: ./gradlew :core:jacocoTestReport

      - name: Verify code coverage
        run: ./gradlew :core:jacocoTestCoverageVerification

      - name: Upload coverage to Codecov
        uses: codecov/codecov-action@v4
        with:
          files: core/build/reports/jacoco/test/jacocoTestReport.xml
          fail_ci_if_error: false

      - name: Upload test results
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: test-results-java${{ matrix.java-version }}
          path: core/build/reports/tests/

  integration-tests:
    needs: unit-tests
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '17'
          cache: 'gradle'

      - name: Run integration tests with Testcontainers
        run: ./gradlew :integration-tests:test
        env:
          TESTCONTAINERS_RYUK_DISABLED: true

      - name: Upload integration test results
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: integration-test-results
          path: integration-tests/build/reports/tests/

  security-scan:
    needs: build
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 17
        uses: actions/setup-java@v4
        with:
          distribution: 'temurin'
          java-version: '17'
          cache: 'gradle'

      - name: Run OWASP Dependency Check
        run: ./gradlew dependencyCheckAnalyze
        continue-on-error: true
        env:
          NVD_API_KEY: ${{ secrets.NVD_API_KEY }}

      - name: Upload dependency check report
        uses: actions/upload-artifact@v4
        with:
          name: dependency-check-report
          path: build/reports/dependency-check-report.html

  quality-gate:
    needs: [unit-tests, integration-tests, security-scan]
    runs-on: ubuntu-latest

    steps:
      - name: Quality Gate Passed
        run: echo "All quality checks passed!"
```

### Files
- CREATE: `.github/workflows/ci.yml`

### Definition of Done
- [ ] Workflow file valid YAML
- [ ] All jobs defined
- [ ] Oracle integration working

---

## IMPL-008: Docker Compose for Local Development

**Phase:** 1
**Complexity:** M
**Dependencies:** None

### Description
Create Docker Compose configuration for running Oracle 26ai-free locally for development.

### Acceptance Criteria
- [ ] Test: `docker-compose up` starts Oracle
- [ ] Test: JDBC connection works
- [ ] Test: JSON Collection can be created

### Implementation

#### Create docker-compose.yml

```yaml
# docker-compose.yml
version: '3.8'

services:
  oracle:
    image: gvenzl/oracle-free:23.6-slim-faststart
    container_name: mongo-translator-oracle
    environment:
      ORACLE_PASSWORD: oracle
      APP_USER: translator
      APP_USER_PASSWORD: translator123
    ports:
      - "1521:1521"
    volumes:
      - oracle-data:/opt/oracle/oradata
      - ./docker/oracle/init:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD", "healthcheck.sh"]
      interval: 30s
      timeout: 10s
      retries: 10
      start_period: 60s

volumes:
  oracle-data:
```

#### Create initialization script

```sql
-- docker/oracle/init/01_setup.sql
-- Enable JSON features and create test schema

-- Create JSON collection table for testing
CREATE TABLE test_customers (
    id RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create JSON collection using SODA-style DDL (Oracle 23ai)
-- This creates a proper JSON collection
BEGIN
    EXECUTE IMMEDIATE '
        CREATE JSON COLLECTION TABLE orders
    ';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -955 THEN
            RAISE;
        END IF;
END;
/

-- Insert sample data
INSERT INTO test_customers (data) VALUES
    ('{"name": "John Doe", "email": "john@example.com", "status": "active", "age": 30}');
INSERT INTO test_customers (data) VALUES
    ('{"name": "Jane Smith", "email": "jane@example.com", "status": "active", "age": 25}');
INSERT INTO test_customers (data) VALUES
    ('{"name": "Bob Wilson", "email": "bob@example.com", "status": "inactive", "age": 45}');

COMMIT;
```

### Files
- CREATE: `docker-compose.yml`
- CREATE: `docker/oracle/init/01_setup.sql`
- CREATE: `.dockerignore`

### Definition of Done
- [ ] Oracle starts successfully
- [ ] JSON collections work
- [ ] Test data seeded

---

## IMPL-009: Gradle Wrapper and .gitignore

**Phase:** 1
**Complexity:** S
**Dependencies:** IMPL-001

### Description
Set up Gradle wrapper for consistent builds and proper .gitignore.

### Acceptance Criteria
- [ ] Test: `./gradlew --version` works
- [ ] Test: Build artifacts ignored
- [ ] Test: IDE files ignored

### Implementation

#### Run Gradle wrapper generation

```bash
gradle wrapper --gradle-version 8.5
```

#### Create .gitignore

```gitignore
# .gitignore

# Gradle
.gradle/
build/
!gradle/wrapper/gradle-wrapper.jar

# IDE
.idea/
*.iml
.project
.classpath
.settings/
.vscode/

# OS
.DS_Store
Thumbs.db

# Logs
*.log

# Test output
out/
target/

# Environment
.env
.env.local
*.env

# Docker
oracle-data/
```

### Files
- CREATE: `gradle/wrapper/gradle-wrapper.jar`
- CREATE: `gradle/wrapper/gradle-wrapper.properties`
- CREATE: `gradlew`
- CREATE: `gradlew.bat`
- CREATE: `.gitignore`

### Definition of Done
- [ ] Wrapper works
- [ ] Proper files ignored

---

## IMPL-010: README and Setup Documentation

**Phase:** 1
**Complexity:** S
**Dependencies:** All IMPL-001 through IMPL-009

### Description
Create README with setup instructions for developers.

### Acceptance Criteria
- [ ] Test: Instructions are complete and accurate
- [ ] Test: Quick start works as documented

### Implementation

#### Create README.md

```markdown
# MongoDB to Oracle SQL Translator

A Java library that translates MongoDB aggregation framework pipelines into
equivalent Oracle SQL/JSON statements for execution on Oracle 26ai JSON Collections.

## Quick Start

### Prerequisites

- JDK 17 or higher
- Docker and Docker Compose
- Git

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/mongo-oracle-translator.git
   cd mongo-oracle-translator
   ```

2. Start Oracle database:
   ```bash
   docker-compose up -d
   ```

3. Wait for Oracle to be ready (check logs):
   ```bash
   docker-compose logs -f oracle
   ```

4. Run the build:
   ```bash
   ./gradlew build
   ```

5. Run tests:
   ```bash
   ./gradlew test
   ```

### Pre-commit Hooks (Optional but Recommended)

```bash
pip install pre-commit
pre-commit install
```

## Usage

```java
import com.oracle.mongodb.translator.api.AggregationTranslator;
import org.bson.Document;

var translator = AggregationTranslator.create(config);

var pipeline = List.of(
    Document.parse("{\"$match\": {\"status\": \"active\"}}"),
    Document.parse("{\"$group\": {\"_id\": \"$category\", \"total\": {\"$sum\": \"$amount\"}}}")
);

var result = translator.translate(pipeline);
System.out.println(result.sql());
// Output: SELECT JSON_VALUE(data, '$.category') AS _id, ...
```

## Project Structure

```
├── core/               # Main translation library
├── integration-tests/  # Oracle integration tests
├── generator/          # Code generation from specs
├── specs/              # Operator specifications
└── docs/               # Documentation
```

## License

Apache 2.0
```

### Files
- CREATE: `README.md`

### Definition of Done
- [ ] README complete
- [ ] Quick start verified

---

# Phase 2: Core Infrastructure

## Entry Criteria
- Phase 1 complete (project builds and CI/CD works)
- Developer can run Oracle locally

## Exit Criteria
- AST node hierarchy implemented
- Pipeline parsing works
- SQL generation context functional
- Basic integration test passes

---

## IMPL-011: Core Exception Hierarchy

**Phase:** 2
**Complexity:** S
**Dependencies:** IMPL-001

### Description
Create the exception hierarchy for the translation library. These are needed by all other components.

### Acceptance Criteria
- [ ] Test: TranslationException can be thrown and caught
- [ ] Test: UnsupportedOperatorException contains operator name
- [ ] Test: ValidationException contains validation errors
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/exception/TranslationExceptionTest.java
package com.oracle.mongodb.translator.exception;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class TranslationExceptionTest {

    @Test
    void shouldCreateWithMessage() {
        var exception = new TranslationException("Translation failed");

        assertThat(exception.getMessage()).isEqualTo("Translation failed");
    }

    @Test
    void shouldCreateWithCause() {
        var cause = new RuntimeException("root cause");
        var exception = new TranslationException("Translation failed", cause);

        assertThat(exception.getMessage()).isEqualTo("Translation failed");
        assertThat(exception.getCause()).isSameAs(cause);
    }
}

// core/src/test/java/com/oracle/mongodb/translator/exception/UnsupportedOperatorExceptionTest.java
package com.oracle.mongodb.translator.exception;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class UnsupportedOperatorExceptionTest {

    @Test
    void shouldContainOperatorName() {
        var exception = new UnsupportedOperatorException("$graphLookup");

        assertThat(exception.getOperatorName()).isEqualTo("$graphLookup");
        assertThat(exception.getMessage()).contains("$graphLookup");
    }

    @Test
    void shouldIndicateIfPartiallySupported() {
        var exception = new UnsupportedOperatorException("$lookup", true);

        assertThat(exception.isPartiallySupported()).isTrue();
    }
}

// core/src/test/java/com/oracle/mongodb/translator/exception/ValidationExceptionTest.java
package com.oracle.mongodb.translator.exception;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class ValidationExceptionTest {

    @Test
    void shouldContainValidationErrors() {
        var errors = List.of(
            new ValidationError("MISSING_FIELD", "_id field required in $group"),
            new ValidationError("INVALID_TYPE", "Expected object, got array")
        );

        var exception = new ValidationException(errors);

        assertThat(exception.getErrors()).hasSize(2);
        assertThat(exception.getMessage()).contains("_id field required");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/exception/TranslationException.java
package com.oracle.mongodb.translator.exception;

/**
 * Base exception for all translation errors.
 */
public class TranslationException extends RuntimeException {

    public TranslationException(String message) {
        super(message);
    }

    public TranslationException(String message, Throwable cause) {
        super(message, cause);
    }
}

// core/src/main/java/com/oracle/mongodb/translator/exception/UnsupportedOperatorException.java
package com.oracle.mongodb.translator.exception;

/**
 * Thrown when an unsupported MongoDB operator is encountered.
 */
public class UnsupportedOperatorException extends TranslationException {

    private final String operatorName;
    private final boolean partiallySupported;

    public UnsupportedOperatorException(String operatorName) {
        this(operatorName, false);
    }

    public UnsupportedOperatorException(String operatorName, boolean partiallySupported) {
        super(buildMessage(operatorName, partiallySupported));
        this.operatorName = operatorName;
        this.partiallySupported = partiallySupported;
    }

    private static String buildMessage(String operatorName, boolean partiallySupported) {
        if (partiallySupported) {
            return "Operator " + operatorName + " is only partially supported";
        }
        return "Operator " + operatorName + " is not supported";
    }

    public String getOperatorName() {
        return operatorName;
    }

    public boolean isPartiallySupported() {
        return partiallySupported;
    }
}

// core/src/main/java/com/oracle/mongodb/translator/exception/ValidationError.java
package com.oracle.mongodb.translator.exception;

/**
 * Represents a single validation error.
 */
public record ValidationError(String code, String message) {
}

// core/src/main/java/com/oracle/mongodb/translator/exception/ValidationException.java
package com.oracle.mongodb.translator.exception;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Thrown when pipeline validation fails.
 */
public class ValidationException extends TranslationException {

    private final List<ValidationError> errors;

    public ValidationException(List<ValidationError> errors) {
        super(buildMessage(errors));
        this.errors = List.copyOf(errors);
    }

    private static String buildMessage(List<ValidationError> errors) {
        return "Validation failed: " + errors.stream()
            .map(ValidationError::message)
            .collect(Collectors.joining("; "));
    }

    public List<ValidationError> getErrors() {
        return errors;
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/exception/TranslationException.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/exception/UnsupportedOperatorException.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/exception/ValidationError.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/exception/ValidationException.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/exception/TranslationExceptionTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/exception/UnsupportedOperatorExceptionTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/exception/ValidationExceptionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-012: AST Node Base Interface

**Phase:** 2
**Complexity:** S
**Dependencies:** IMPL-011

### Description
Create the base AST node interface and the SQL generation context interface. This is the foundation of the jOOQ-inspired rendering pattern.

### Acceptance Criteria
- [ ] Test: AstNode interface defines render method
- [ ] Test: SqlGenerationContext provides sql(), visit(), bind() methods
- [ ] Test: Simple node can render SQL
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/AstNodeTest.java
package com.oracle.mongodb.translator.ast;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AstNodeTest {

    @Test
    void shouldRenderSimpleNode() {
        AstNode node = ctx -> ctx.sql("SELECT 1 FROM DUAL");

        var context = new DefaultSqlGenerationContext();
        node.render(context);

        assertThat(context.toSql()).isEqualTo("SELECT 1 FROM DUAL");
    }

    @Test
    void shouldSupportNestedRendering() {
        AstNode inner = ctx -> ctx.sql("42");
        AstNode outer = ctx -> {
            ctx.sql("SELECT ");
            ctx.visit(inner);
            ctx.sql(" FROM DUAL");
        };

        var context = new DefaultSqlGenerationContext();
        outer.render(context);

        assertThat(context.toSql()).isEqualTo("SELECT 42 FROM DUAL");
    }
}

// core/src/test/java/com/oracle/mongodb/translator/generator/DefaultSqlGenerationContextTest.java
package com.oracle.mongodb.translator.generator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DefaultSqlGenerationContextTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldAppendSqlFragments() {
        context.sql("SELECT ");
        context.sql("* ");
        context.sql("FROM table");

        assertThat(context.toSql()).isEqualTo("SELECT * FROM table");
    }

    @Test
    void shouldCollectBindVariables() {
        context.sql("WHERE status = ");
        context.bind("active");

        assertThat(context.toSql()).isEqualTo("WHERE status = :1");
        assertThat(context.getBindVariables()).containsExactly("active");
    }

    @Test
    void shouldNumberBindVariablesSequentially() {
        context.sql("WHERE status = ");
        context.bind("active");
        context.sql(" AND age > ");
        context.bind(21);

        assertThat(context.toSql()).isEqualTo("WHERE status = :1 AND age > :2");
        assertThat(context.getBindVariables()).containsExactly("active", 21);
    }

    @Test
    void shouldQuoteIdentifiers() {
        context.sql("SELECT ");
        context.identifier("user-name");
        context.sql(" FROM ");
        context.identifier("my-table");

        assertThat(context.toSql()).isEqualTo("SELECT \"user-name\" FROM \"my-table\"");
    }

    @Test
    void shouldNotQuoteSimpleIdentifiers() {
        context.sql("SELECT ");
        context.identifier("username");

        assertThat(context.toSql()).isEqualTo("SELECT username");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/AstNode.java
package com.oracle.mongodb.translator.ast;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Base interface for all AST nodes. Nodes render themselves to SQL
 * via the context pattern (jOOQ-inspired).
 */
@FunctionalInterface
public interface AstNode {

    /**
     * Renders this node to SQL using the provided context.
     *
     * @param ctx the SQL generation context
     */
    void render(SqlGenerationContext ctx);
}

// core/src/main/java/com/oracle/mongodb/translator/generator/SqlGenerationContext.java
package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.generator.dialect.OracleDialect;
import java.util.List;

/**
 * Context for SQL generation. AST nodes use this interface to build SQL.
 */
public interface SqlGenerationContext {

    /**
     * Appends a raw SQL fragment.
     */
    void sql(String fragment);

    /**
     * Recursively renders a child AST node.
     */
    void visit(AstNode node);

    /**
     * Adds a bind variable and appends the placeholder.
     */
    void bind(Object value);

    /**
     * Appends an identifier, quoting if necessary.
     */
    void identifier(String name);

    /**
     * Returns whether values should be inlined (for debugging).
     */
    boolean inline();

    /**
     * Returns the target Oracle dialect.
     */
    OracleDialect dialect();

    /**
     * Returns the generated SQL string.
     */
    String toSql();

    /**
     * Returns the collected bind variables.
     */
    List<Object> getBindVariables();
}

// core/src/main/java/com/oracle/mongodb/translator/generator/DefaultSqlGenerationContext.java
package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.ast.AstNode;
import com.oracle.mongodb.translator.generator.dialect.OracleDialect;
import com.oracle.mongodb.translator.generator.dialect.Oracle26aiDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Default implementation of SqlGenerationContext.
 */
public class DefaultSqlGenerationContext implements SqlGenerationContext {

    private static final Pattern SIMPLE_IDENTIFIER = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    private final StringBuilder sql = new StringBuilder();
    private final List<Object> bindVariables = new ArrayList<>();
    private final boolean inlineValues;
    private final OracleDialect dialect;

    public DefaultSqlGenerationContext() {
        this(false, Oracle26aiDialect.INSTANCE);
    }

    public DefaultSqlGenerationContext(boolean inlineValues, OracleDialect dialect) {
        this.inlineValues = inlineValues;
        this.dialect = dialect;
    }

    @Override
    public void sql(String fragment) {
        sql.append(fragment);
    }

    @Override
    public void visit(AstNode node) {
        node.render(this);
    }

    @Override
    public void bind(Object value) {
        if (inlineValues) {
            sql.append(formatInlineValue(value));
        } else {
            bindVariables.add(value);
            sql.append(":").append(bindVariables.size());
        }
    }

    @Override
    public void identifier(String name) {
        if (SIMPLE_IDENTIFIER.matcher(name).matches()) {
            sql.append(name);
        } else {
            sql.append("\"").append(name).append("\"");
        }
    }

    @Override
    public boolean inline() {
        return inlineValues;
    }

    @Override
    public OracleDialect dialect() {
        return dialect;
    }

    @Override
    public String toSql() {
        return sql.toString();
    }

    @Override
    public List<Object> getBindVariables() {
        return List.copyOf(bindVariables);
    }

    private String formatInlineValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + ((String) value).replace("'", "''") + "'";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        return "'" + value.toString().replace("'", "''") + "'";
    }
}

// core/src/main/java/com/oracle/mongodb/translator/generator/dialect/OracleDialect.java
package com.oracle.mongodb.translator.generator.dialect;

/**
 * Represents an Oracle database dialect version.
 */
public interface OracleDialect {

    /**
     * Returns the dialect name.
     */
    String name();

    /**
     * Returns true if JSON_VALUE supports RETURNING clause.
     */
    boolean supportsJsonValueReturning();

    /**
     * Returns true if JSON_TABLE supports NESTED PATH.
     */
    boolean supportsNestedPath();

    /**
     * Returns true if JSON COLLECTION TABLEs are supported.
     */
    boolean supportsJsonCollectionTables();
}

// core/src/main/java/com/oracle/mongodb/translator/generator/dialect/Oracle26aiDialect.java
package com.oracle.mongodb.translator.generator.dialect;

/**
 * Oracle 26ai dialect with full JSON support.
 */
public final class Oracle26aiDialect implements OracleDialect {

    public static final Oracle26aiDialect INSTANCE = new Oracle26aiDialect();

    private Oracle26aiDialect() {}

    @Override
    public String name() {
        return "Oracle 26ai";
    }

    @Override
    public boolean supportsJsonValueReturning() {
        return true;
    }

    @Override
    public boolean supportsNestedPath() {
        return true;
    }

    @Override
    public boolean supportsJsonCollectionTables() {
        return true;
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/AstNode.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/generator/SqlGenerationContext.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/generator/DefaultSqlGenerationContext.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/generator/dialect/OracleDialect.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/generator/dialect/Oracle26aiDialect.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/AstNodeTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/generator/DefaultSqlGenerationContextTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-013: Expression Base Classes

**Phase:** 2
**Complexity:** M
**Dependencies:** IMPL-012

### Description
Create the Expression AST hierarchy including FieldPathExpression, LiteralExpression, and the Expression sealed interface.

### Acceptance Criteria
- [ ] Test: FieldPathExpression renders to JSON_VALUE path
- [ ] Test: LiteralExpression renders to bind variable
- [ ] Test: Nested paths work correctly
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/expression/FieldPathExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.*;

class FieldPathExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderSimpleFieldPath() {
        var expr = FieldPathExpression.of("status");

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.status')");
    }

    @Test
    void shouldRenderNestedFieldPath() {
        var expr = FieldPathExpression.of("customer.address.city");

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.customer.address.city')");
    }

    @Test
    void shouldHandleDollarPrefixedPath() {
        var expr = FieldPathExpression.of("$status");

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.status')");
    }

    @ParameterizedTest
    @CsvSource({
        "name, $.name",
        "$name, $.name",
        "user.email, $.user.email",
        "$user.email, $.user.email",
        "items.0.price, $.items[0].price"
    })
    void shouldConvertToJsonPath(String input, String expectedPath) {
        var expr = FieldPathExpression.of(input);

        assertThat(expr.getJsonPath()).isEqualTo(expectedPath);
    }

    @Test
    void shouldRenderWithReturningClauseForNumbers() {
        var expr = FieldPathExpression.of("amount", JsonReturnType.NUMBER);

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("JSON_VALUE(data, '$.amount' RETURNING NUMBER)");
    }
}

// core/src/test/java/com/oracle/mongodb/translator/ast/expression/LiteralExpressionTest.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.stream.Stream;
import static org.assertj.core.api.Assertions.*;

class LiteralExpressionTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldRenderStringLiteralAsBindVariable() {
        var expr = LiteralExpression.of("active");

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(":1");
        assertThat(context.getBindVariables()).containsExactly("active");
    }

    @Test
    void shouldRenderNumberLiteral() {
        var expr = LiteralExpression.of(42);

        expr.render(context);

        assertThat(context.toSql()).isEqualTo(":1");
        assertThat(context.getBindVariables()).containsExactly(42);
    }

    @Test
    void shouldRenderNullLiteral() {
        var expr = LiteralExpression.ofNull();

        expr.render(context);

        assertThat(context.toSql()).isEqualTo("NULL");
        assertThat(context.getBindVariables()).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideLiteralValues")
    void shouldHandleVariousTypes(Object value, Class<?> expectedType) {
        var expr = LiteralExpression.of(value);

        assertThat(expr.getValue()).isEqualTo(value);
        assertThat(expr.getValue()).isInstanceOf(expectedType);
    }

    static Stream<Arguments> provideLiteralValues() {
        return Stream.of(
            Arguments.of("string", String.class),
            Arguments.of(42, Integer.class),
            Arguments.of(3.14, Double.class),
            Arguments.of(true, Boolean.class),
            Arguments.of(100L, Long.class)
        );
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/expression/Expression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.ast.AstNode;

/**
 * Sealed interface for all expression types in the AST.
 */
public sealed interface Expression extends AstNode
    permits FieldPathExpression, LiteralExpression, ComparisonExpression,
            LogicalExpression, ArithmeticExpression, ConditionalExpression,
            ArrayExpression, AccumulatorExpression {
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/JsonReturnType.java
package com.oracle.mongodb.translator.ast.expression;

/**
 * JSON_VALUE RETURNING clause types.
 */
public enum JsonReturnType {
    VARCHAR("VARCHAR2(4000)"),
    NUMBER("NUMBER"),
    DATE("DATE"),
    TIMESTAMP("TIMESTAMP"),
    BOOLEAN("VARCHAR2(5)"), // Oracle returns 'true'/'false'
    JSON("JSON");

    private final String oracleSyntax;

    JsonReturnType(String oracleSyntax) {
        this.oracleSyntax = oracleSyntax;
    }

    public String getOracleSyntax() {
        return oracleSyntax;
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/FieldPathExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents a field path reference like "$status" or "$customer.address.city".
 */
public final class FieldPathExpression implements Expression {

    private static final Pattern ARRAY_INDEX = Pattern.compile("\\.(\\d+)\\.");

    private final String path;
    private final JsonReturnType returnType;
    private final String dataColumn;

    private FieldPathExpression(String path, JsonReturnType returnType, String dataColumn) {
        this.path = Objects.requireNonNull(path, "path must not be null");
        this.returnType = returnType;
        this.dataColumn = dataColumn;
    }

    public static FieldPathExpression of(String path) {
        return new FieldPathExpression(path, null, "data");
    }

    public static FieldPathExpression of(String path, JsonReturnType returnType) {
        return new FieldPathExpression(path, returnType, "data");
    }

    public static FieldPathExpression of(String path, JsonReturnType returnType, String dataColumn) {
        return new FieldPathExpression(path, returnType, dataColumn);
    }

    /**
     * Returns the JSON path for this field (e.g., "$.status").
     */
    public String getJsonPath() {
        String normalizedPath = path.startsWith("$") ? path.substring(1) : path;
        if (normalizedPath.startsWith(".")) {
            normalizedPath = normalizedPath.substring(1);
        }

        // Convert dot-notation array indices to bracket notation
        // e.g., "items.0.price" -> "items[0].price"
        String jsonPath = "$." + normalizedPath;
        jsonPath = ARRAY_INDEX.matcher(jsonPath).replaceAll("[$1].");

        // Handle trailing array index
        if (jsonPath.matches(".*\\.\\d+$")) {
            int lastDot = jsonPath.lastIndexOf('.');
            String index = jsonPath.substring(lastDot + 1);
            jsonPath = jsonPath.substring(0, lastDot) + "[" + index + "]";
        }

        return jsonPath;
    }

    public String getPath() {
        return path;
    }

    public JsonReturnType getReturnType() {
        return returnType;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("JSON_VALUE(");
        ctx.sql(dataColumn);
        ctx.sql(", '");
        ctx.sql(getJsonPath());
        ctx.sql("'");

        if (returnType != null) {
            ctx.sql(" RETURNING ");
            ctx.sql(returnType.getOracleSyntax());
        }

        ctx.sql(")");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldPathExpression that = (FieldPathExpression) o;
        return Objects.equals(path, that.path) && returnType == that.returnType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, returnType);
    }

    @Override
    public String toString() {
        return "FieldPath($" + path + ")";
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/expression/LiteralExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;
import java.util.Objects;

/**
 * Represents a literal constant value.
 */
public final class LiteralExpression implements Expression {

    private final Object value;
    private final boolean isNull;

    private LiteralExpression(Object value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public static LiteralExpression of(Object value) {
        return new LiteralExpression(value, false);
    }

    public static LiteralExpression ofNull() {
        return new LiteralExpression(null, true);
    }

    public Object getValue() {
        return value;
    }

    public boolean isNull() {
        return isNull;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        if (isNull) {
            ctx.sql("NULL");
        } else {
            ctx.bind(value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LiteralExpression that = (LiteralExpression) o;
        return isNull == that.isNull && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, isNull);
    }

    @Override
    public String toString() {
        return isNull ? "Literal(NULL)" : "Literal(" + value + ")";
    }
}
```

#### Step 3: Create stub implementations for sealed permits

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/expression/ComparisonExpression.java
package com.oracle.mongodb.translator.ast.expression;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Placeholder - will be fully implemented in IMPL-016.
 */
public final class ComparisonExpression implements Expression {
    @Override
    public void render(SqlGenerationContext ctx) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}

// Similar stubs for LogicalExpression, ArithmeticExpression, ConditionalExpression,
// ArrayExpression, AccumulatorExpression
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/Expression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/JsonReturnType.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/FieldPathExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/LiteralExpression.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ComparisonExpression.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/LogicalExpression.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArithmeticExpression.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ConditionalExpression.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/AccumulatorExpression.java` (stub)
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/FieldPathExpressionTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/expression/LiteralExpressionTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-014: Stage Base Classes

**Phase:** 2
**Complexity:** M
**Dependencies:** IMPL-012

### Description
Create the Stage sealed interface and Pipeline class that holds a sequence of stages.

### Acceptance Criteria
- [ ] Test: Pipeline can hold multiple stages
- [ ] Test: Pipeline renders stages in sequence
- [ ] Test: Pipeline validates non-empty
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/ast/PipelineTest.java
package com.oracle.mongodb.translator.ast;

import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class PipelineTest {

    @Test
    void shouldCreatePipelineWithStages() {
        var stages = List.<Stage>of(
            new LimitStage(10)
        );

        var pipeline = Pipeline.of("orders", stages);

        assertThat(pipeline.getCollectionName()).isEqualTo("orders");
        assertThat(pipeline.getStages()).hasSize(1);
    }

    @Test
    void shouldRejectEmptyPipeline() {
        assertThatThrownBy(() -> Pipeline.of("orders", List.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("at least one stage");
    }

    @Test
    void shouldRejectNullCollectionName() {
        var stages = List.<Stage>of(new LimitStage(10));

        assertThatThrownBy(() -> Pipeline.of(null, stages))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRenderBasicSelect() {
        var pipeline = Pipeline.of("customers", List.of(
            new LimitStage(5)
        ));

        var context = new DefaultSqlGenerationContext();
        pipeline.render(context);

        assertThat(context.toSql())
            .contains("SELECT")
            .contains("FROM customers")
            .contains("FETCH FIRST 5 ROWS ONLY");
    }
}

// core/src/test/java/com/oracle/mongodb/translator/ast/stage/LimitStageTest.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

class LimitStageTest {

    @Test
    void shouldRenderLimitClause() {
        var stage = new LimitStage(10);
        var context = new DefaultSqlGenerationContext();

        stage.render(context);

        assertThat(context.toSql()).isEqualTo("FETCH FIRST 10 ROWS ONLY");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1, -100})
    void shouldRejectInvalidLimit(int limit) {
        assertThatThrownBy(() -> new LimitStage(limit))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldReturnLimit() {
        var stage = new LimitStage(25);

        assertThat(stage.getLimit()).isEqualTo(25);
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/ast/stage/Stage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.ast.AstNode;

/**
 * Sealed interface for all aggregation pipeline stages.
 */
public sealed interface Stage extends AstNode
    permits MatchStage, GroupStage, ProjectStage, SortStage,
            LimitStage, SkipStage, LookupStage, UnwindStage,
            AddFieldsStage, CountStage {

    /**
     * Returns the MongoDB stage operator name (e.g., "$match").
     */
    String getOperatorName();
}

// core/src/main/java/com/oracle/mongodb/translator/ast/stage/LimitStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Represents a $limit stage.
 */
public final class LimitStage implements Stage {

    private final int limit;

    public LimitStage(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        this.limit = limit;
    }

    public int getLimit() {
        return limit;
    }

    @Override
    public String getOperatorName() {
        return "$limit";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("FETCH FIRST ");
        ctx.sql(String.valueOf(limit));
        ctx.sql(" ROWS ONLY");
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/Pipeline.java
package com.oracle.mongodb.translator.ast;

import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.ast.stage.ProjectStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.generator.SqlGenerationContext;

import java.util.List;
import java.util.Objects;

/**
 * Represents a complete aggregation pipeline.
 */
public final class Pipeline implements AstNode {

    private final String collectionName;
    private final List<Stage> stages;

    private Pipeline(String collectionName, List<Stage> stages) {
        this.collectionName = Objects.requireNonNull(collectionName, "collectionName must not be null");
        this.stages = List.copyOf(stages);

        if (this.stages.isEmpty()) {
            throw new IllegalArgumentException("Pipeline must contain at least one stage");
        }
    }

    public static Pipeline of(String collectionName, List<Stage> stages) {
        return new Pipeline(collectionName, stages);
    }

    public String getCollectionName() {
        return collectionName;
    }

    public List<Stage> getStages() {
        return stages;
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        // Determine what columns to select based on stages
        renderSelect(ctx);
        ctx.sql(" FROM ");
        ctx.identifier(collectionName);

        // Render each stage
        for (Stage stage : stages) {
            ctx.sql(" ");
            stage.render(ctx);
        }
    }

    private void renderSelect(SqlGenerationContext ctx) {
        // Check if there's a $group or $project stage that defines output
        boolean hasGroupOrProject = stages.stream()
            .anyMatch(s -> s instanceof GroupStage || s instanceof ProjectStage);

        if (hasGroupOrProject) {
            // Group/Project stages will define their own SELECT
            ctx.sql("SELECT ");
            renderProjection(ctx);
        } else {
            // Default: select the JSON data column
            ctx.sql("SELECT data");
        }
    }

    private void renderProjection(SqlGenerationContext ctx) {
        // For now, just select data - will be refined in later tickets
        ctx.sql("data");
    }
}

// Stub implementations for other stages (will be fully implemented later)
// core/src/main/java/com/oracle/mongodb/translator/ast/stage/MatchStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

public final class MatchStage implements Stage {
    @Override
    public String getOperatorName() { return "$match"; }

    @Override
    public void render(SqlGenerationContext ctx) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}

// Similar stubs for GroupStage, ProjectStage, SortStage, SkipStage,
// LookupStage, UnwindStage, AddFieldsStage, CountStage
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/Stage.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/LimitStage.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/Pipeline.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/MatchStage.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/GroupStage.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/ProjectStage.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/SortStage.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/SkipStage.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/LookupStage.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/UnwindStage.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/AddFieldsStage.java` (stub)
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/CountStage.java` (stub)
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/PipelineTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/ast/stage/LimitStageTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-015: Public API Classes

**Phase:** 2
**Complexity:** M
**Dependencies:** IMPL-013, IMPL-014

### Description
Create the public API classes: AggregationTranslator, TranslationResult, TranslationOptions, and MongoCollection facade.

### Acceptance Criteria
- [ ] Test: AggregationTranslator.create() returns instance
- [ ] Test: translate() returns TranslationResult
- [ ] Test: TranslationResult contains SQL and bind variables
- [ ] Test: TranslationOptions builder works
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/api/TranslationOptionsTest.java
package com.oracle.mongodb.translator.api;

import com.oracle.mongodb.translator.generator.dialect.OracleDialectVersion;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class TranslationOptionsTest {

    @Test
    void shouldCreateDefaultOptions() {
        var options = TranslationOptions.defaults();

        assertThat(options.inlineBindVariables()).isFalse();
        assertThat(options.prettyPrint()).isFalse();
        assertThat(options.includeHints()).isTrue();
        assertThat(options.targetDialect()).isEqualTo(OracleDialectVersion.ORACLE_26AI);
        assertThat(options.strictMode()).isFalse();
    }

    @Test
    void shouldBuildCustomOptions() {
        var options = TranslationOptions.builder()
            .inlineBindVariables(true)
            .prettyPrint(true)
            .strictMode(true)
            .build();

        assertThat(options.inlineBindVariables()).isTrue();
        assertThat(options.prettyPrint()).isTrue();
        assertThat(options.strictMode()).isTrue();
    }
}

// core/src/test/java/com/oracle/mongodb/translator/api/TranslationResultTest.java
package com.oracle.mongodb.translator.api;

import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class TranslationResultTest {

    @Test
    void shouldCreateResult() {
        var result = new TranslationResult(
            "SELECT * FROM orders WHERE JSON_VALUE(data, '$.status') = :1",
            List.of("active"),
            List.of(),
            CapabilityReport.fullSupport()
        );

        assertThat(result.sql()).contains("SELECT");
        assertThat(result.bindVariables()).containsExactly("active");
        assertThat(result.hasWarnings()).isFalse();
    }

    @Test
    void shouldIndicateWarnings() {
        var result = new TranslationResult(
            "SELECT * FROM orders",
            List.of(),
            List.of(new TranslationWarning("PERF", "Consider adding index")),
            CapabilityReport.fullSupport()
        );

        assertThat(result.hasWarnings()).isTrue();
    }
}

// core/src/test/java/com/oracle/mongodb/translator/api/AggregationTranslatorTest.java
package com.oracle.mongodb.translator.api;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class AggregationTranslatorTest {

    private AggregationTranslator translator;

    @BeforeEach
    void setUp() {
        translator = AggregationTranslator.create(
            OracleConfiguration.builder()
                .collectionName("orders")
                .build()
        );
    }

    @Test
    void shouldTranslateSimplePipeline() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 10}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.sql())
            .contains("SELECT")
            .contains("FROM orders")
            .contains("FETCH FIRST 10 ROWS ONLY");
    }

    @Test
    void shouldReturnBindVariables() {
        var pipeline = List.of(
            Document.parse("{\"$match\": {\"status\": \"active\"}}")
        );

        var result = translator.translate(pipeline);

        assertThat(result.bindVariables()).contains("active");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/api/OracleConfiguration.java
package com.oracle.mongodb.translator.api;

import com.oracle.mongodb.translator.generator.dialect.OracleDialectVersion;
import java.sql.Connection;
import java.util.Objects;

/**
 * Configuration for Oracle database connection and translation.
 */
public final class OracleConfiguration {

    private final Connection connection;
    private final String collectionName;
    private final OracleDialectVersion dialect;
    private final String dataColumnName;

    private OracleConfiguration(Builder builder) {
        this.connection = builder.connection;
        this.collectionName = Objects.requireNonNull(builder.collectionName, "collectionName required");
        this.dialect = builder.dialect != null ? builder.dialect : OracleDialectVersion.ORACLE_26AI;
        this.dataColumnName = builder.dataColumnName != null ? builder.dataColumnName : "data";
    }

    public static Builder builder() {
        return new Builder();
    }

    public Connection getConnection() {
        return connection;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public OracleDialectVersion getDialect() {
        return dialect;
    }

    public String getDataColumnName() {
        return dataColumnName;
    }

    public static final class Builder {
        private Connection connection;
        private String collectionName;
        private OracleDialectVersion dialect;
        private String dataColumnName;

        public Builder connection(Connection connection) {
            this.connection = connection;
            return this;
        }

        public Builder collectionName(String collectionName) {
            this.collectionName = collectionName;
            return this;
        }

        public Builder dialect(OracleDialectVersion dialect) {
            this.dialect = dialect;
            return this;
        }

        public Builder dataColumnName(String dataColumnName) {
            this.dataColumnName = dataColumnName;
            return this;
        }

        public OracleConfiguration build() {
            return new OracleConfiguration(this);
        }
    }
}

// core/src/main/java/com/oracle/mongodb/translator/api/TranslationOptions.java
package com.oracle.mongodb.translator.api;

import com.oracle.mongodb.translator.generator.dialect.OracleDialectVersion;
import java.util.Set;

/**
 * Configuration options for translation behavior.
 */
public record TranslationOptions(
    boolean inlineBindVariables,
    boolean prettyPrint,
    boolean includeHints,
    OracleDialectVersion targetDialect,
    Set<String> allowedOperators,
    boolean strictMode
) {
    public static TranslationOptions defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean inlineBindVariables = false;
        private boolean prettyPrint = false;
        private boolean includeHints = true;
        private OracleDialectVersion targetDialect = OracleDialectVersion.ORACLE_26AI;
        private Set<String> allowedOperators = null;
        private boolean strictMode = false;

        public Builder inlineBindVariables(boolean value) {
            this.inlineBindVariables = value;
            return this;
        }

        public Builder prettyPrint(boolean value) {
            this.prettyPrint = value;
            return this;
        }

        public Builder includeHints(boolean value) {
            this.includeHints = value;
            return this;
        }

        public Builder targetDialect(OracleDialectVersion value) {
            this.targetDialect = value;
            return this;
        }

        public Builder allowedOperators(Set<String> value) {
            this.allowedOperators = value;
            return this;
        }

        public Builder strictMode(boolean value) {
            this.strictMode = value;
            return this;
        }

        public TranslationOptions build() {
            return new TranslationOptions(
                inlineBindVariables, prettyPrint, includeHints,
                targetDialect, allowedOperators, strictMode
            );
        }
    }
}

// core/src/main/java/com/oracle/mongodb/translator/api/TranslationWarning.java
package com.oracle.mongodb.translator.api;

/**
 * Represents a warning generated during translation.
 */
public record TranslationWarning(String code, String message) {
}

// core/src/main/java/com/oracle/mongodb/translator/api/CapabilityReport.java
package com.oracle.mongodb.translator.api;

import java.util.List;
import java.util.Map;

/**
 * Reports the capability level of translation.
 */
public record CapabilityReport(
    TranslationCapability overallCapability,
    Map<String, TranslationCapability> operatorCapabilities,
    List<String> clientSideStages
) {
    public static CapabilityReport fullSupport() {
        return new CapabilityReport(TranslationCapability.FULL_SUPPORT, Map.of(), List.of());
    }

    public boolean hasClientSideStages() {
        return !clientSideStages.isEmpty();
    }
}

// core/src/main/java/com/oracle/mongodb/translator/api/TranslationCapability.java
package com.oracle.mongodb.translator.api;

/**
 * Capability levels for translation.
 */
public enum TranslationCapability {
    FULL_SUPPORT,
    EMULATED,
    PARTIAL,
    CLIENT_SIDE_ONLY,
    UNSUPPORTED
}

// core/src/main/java/com/oracle/mongodb/translator/api/TranslationResult.java
package com.oracle.mongodb.translator.api;

import java.util.List;

/**
 * Translation result containing SQL and execution metadata.
 */
public record TranslationResult(
    String sql,
    List<Object> bindVariables,
    List<TranslationWarning> warnings,
    CapabilityReport capabilities
) {
    public boolean hasWarnings() {
        return !warnings.isEmpty();
    }

    public boolean requiresClientProcessing() {
        return capabilities.hasClientSideStages();
    }
}

// core/src/main/java/com/oracle/mongodb/translator/api/AggregationTranslator.java
package com.oracle.mongodb.translator.api;

import com.oracle.mongodb.translator.parser.PipelineParser;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import com.oracle.mongodb.translator.generator.dialect.Oracle26aiDialect;
import org.bson.Document;

import java.util.List;

/**
 * Primary API for translating MongoDB aggregation pipelines to Oracle SQL.
 */
public interface AggregationTranslator {

    /**
     * Translates a MongoDB aggregation pipeline to Oracle SQL.
     */
    TranslationResult translate(List<Document> pipeline);

    /**
     * Translates with custom options.
     */
    TranslationResult translate(List<Document> pipeline, TranslationOptions options);

    /**
     * Factory method with Oracle connection configuration.
     */
    static AggregationTranslator create(OracleConfiguration config) {
        return new DefaultAggregationTranslator(config);
    }
}

// core/src/main/java/com/oracle/mongodb/translator/api/DefaultAggregationTranslator.java
package com.oracle.mongodb.translator.api;

import com.oracle.mongodb.translator.ast.Pipeline;
import com.oracle.mongodb.translator.parser.PipelineParser;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import com.oracle.mongodb.translator.generator.dialect.Oracle26aiDialect;
import org.bson.Document;

import java.util.List;

/**
 * Default implementation of AggregationTranslator.
 */
final class DefaultAggregationTranslator implements AggregationTranslator {

    private final OracleConfiguration config;
    private final PipelineParser parser;

    DefaultAggregationTranslator(OracleConfiguration config) {
        this.config = config;
        this.parser = new PipelineParser();
    }

    @Override
    public TranslationResult translate(List<Document> pipeline) {
        return translate(pipeline, TranslationOptions.defaults());
    }

    @Override
    public TranslationResult translate(List<Document> pipeline, TranslationOptions options) {
        // Parse pipeline to AST
        Pipeline ast = parser.parse(config.getCollectionName(), pipeline);

        // Generate SQL
        var context = new DefaultSqlGenerationContext(
            options.inlineBindVariables(),
            Oracle26aiDialect.INSTANCE
        );

        ast.render(context);

        return new TranslationResult(
            context.toSql(),
            context.getBindVariables(),
            List.of(),
            CapabilityReport.fullSupport()
        );
    }
}

// core/src/main/java/com/oracle/mongodb/translator/generator/dialect/OracleDialectVersion.java
package com.oracle.mongodb.translator.generator.dialect;

/**
 * Enumeration of supported Oracle dialect versions.
 */
public enum OracleDialectVersion {
    ORACLE_21C,
    ORACLE_23AI,
    ORACLE_26AI
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/api/OracleConfiguration.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/api/TranslationOptions.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/api/TranslationWarning.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/api/CapabilityReport.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/api/TranslationCapability.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/api/TranslationResult.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/api/AggregationTranslator.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/api/DefaultAggregationTranslator.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/generator/dialect/OracleDialectVersion.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/api/TranslationOptionsTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/api/TranslationResultTest.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/api/AggregationTranslatorTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-016: Pipeline Parser Foundation

**Phase:** 2
**Complexity:** L
**Dependencies:** IMPL-013, IMPL-014

### Description
Create the PipelineParser that converts BSON Documents to AST nodes. Start with $limit only, then expand.

### Acceptance Criteria
- [ ] Test: Parser handles $limit stage
- [ ] Test: Parser throws on unknown stage
- [ ] Test: Parser handles multiple stages
- [ ] Code coverage >= 80%

### Test-First Implementation

#### Step 1: Write Failing Tests

```java
// core/src/test/java/com/oracle/mongodb/translator/parser/PipelineParserTest.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.Pipeline;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class PipelineParserTest {

    private PipelineParser parser;

    @BeforeEach
    void setUp() {
        parser = new PipelineParser();
    }

    @Test
    void shouldParseLimitStage() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 10}")
        );

        var result = parser.parse("orders", pipeline);

        assertThat(result.getStages()).hasSize(1);
        assertThat(result.getStages().get(0)).isInstanceOf(LimitStage.class);
        assertThat(((LimitStage) result.getStages().get(0)).getLimit()).isEqualTo(10);
    }

    @Test
    void shouldParseSkipStage() {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 5}")
        );

        var result = parser.parse("orders", pipeline);

        assertThat(result.getStages()).hasSize(1);
        assertThat(result.getStages().get(0)).isInstanceOf(SkipStage.class);
        assertThat(((SkipStage) result.getStages().get(0)).getSkip()).isEqualTo(5);
    }

    @Test
    void shouldParseMultipleStages() {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 10}"),
            Document.parse("{\"$limit\": 5}")
        );

        var result = parser.parse("orders", pipeline);

        assertThat(result.getStages()).hasSize(2);
    }

    @Test
    void shouldThrowOnUnknownStage() {
        var pipeline = List.of(
            Document.parse("{\"$unknownOperator\": {}}")
        );

        assertThatThrownBy(() -> parser.parse("orders", pipeline))
            .isInstanceOf(UnsupportedOperatorException.class)
            .hasMessageContaining("$unknownOperator");
    }

    @Test
    void shouldSetCollectionName() {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 1}")
        );

        var result = parser.parse("customers", pipeline);

        assertThat(result.getCollectionName()).isEqualTo("customers");
    }
}
```

#### Step 2: Implement to Pass

```java
// core/src/main/java/com/oracle/mongodb/translator/parser/PipelineParser.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.Pipeline;
import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Parses MongoDB aggregation pipeline documents into AST.
 */
public class PipelineParser {

    private final StageParserRegistry stageRegistry;

    public PipelineParser() {
        this.stageRegistry = new StageParserRegistry();
    }

    /**
     * Parses a pipeline of BSON documents into an AST Pipeline.
     */
    public Pipeline parse(String collectionName, List<Document> pipeline) {
        List<Stage> stages = new ArrayList<>();

        for (Document stageDoc : pipeline) {
            stages.add(parseStage(stageDoc));
        }

        return Pipeline.of(collectionName, stages);
    }

    private Stage parseStage(Document stageDoc) {
        if (stageDoc.size() != 1) {
            throw new IllegalArgumentException(
                "Stage document must have exactly one key, got: " + stageDoc.keySet());
        }

        String operatorName = stageDoc.keySet().iterator().next();
        Object operatorValue = stageDoc.get(operatorName);

        StageParser<?> parser = stageRegistry.getParser(operatorName);
        if (parser == null) {
            throw new UnsupportedOperatorException(operatorName);
        }

        return parser.parse(operatorValue);
    }
}

// core/src/main/java/com/oracle/mongodb/translator/parser/StageParser.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.Stage;

/**
 * Interface for stage-specific parsers.
 */
@FunctionalInterface
public interface StageParser<T extends Stage> {

    /**
     * Parses the stage value into an AST node.
     */
    T parse(Object value);
}

// core/src/main/java/com/oracle/mongodb/translator/parser/StageParserRegistry.java
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry of stage parsers by operator name.
 */
public class StageParserRegistry {

    private final Map<String, StageParser<?>> parsers = new HashMap<>();

    public StageParserRegistry() {
        registerBuiltInParsers();
    }

    private void registerBuiltInParsers() {
        // Tier 1 simple stages
        register("$limit", value -> new LimitStage(toInt(value)));
        register("$skip", value -> new SkipStage(toInt(value)));

        // More parsers will be added in subsequent tickets
    }

    public void register(String operatorName, StageParser<?> parser) {
        parsers.put(operatorName, parser);
    }

    public StageParser<?> getParser(String operatorName) {
        return parsers.get(operatorName);
    }

    private static int toInt(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Long) {
            return ((Long) value).intValue();
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new IllegalArgumentException("Expected number, got: " + value.getClass());
    }
}

// core/src/main/java/com/oracle/mongodb/translator/ast/stage/SkipStage.java
package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Represents a $skip stage.
 */
public final class SkipStage implements Stage {

    private final int skip;

    public SkipStage(int skip) {
        if (skip < 0) {
            throw new IllegalArgumentException("Skip must be non-negative, got: " + skip);
        }
        this.skip = skip;
    }

    public int getSkip() {
        return skip;
    }

    @Override
    public String getOperatorName() {
        return "$skip";
    }

    @Override
    public void render(SqlGenerationContext ctx) {
        ctx.sql("OFFSET ");
        ctx.sql(String.valueOf(skip));
        ctx.sql(" ROWS");
    }
}
```

### Files
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/PipelineParser.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/StageParser.java`
- CREATE: `core/src/main/java/com/oracle/mongodb/translator/parser/StageParserRegistry.java`
- MODIFY: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/SkipStage.java`
- CREATE: `core/src/test/java/com/oracle/mongodb/translator/parser/PipelineParserTest.java`

### Definition of Done
- [ ] All tests pass
- [ ] Code coverage >= 80%
- [ ] No SpotBugs warnings

---

## IMPL-017: Basic Integration Test Infrastructure

**Phase:** 2
**Complexity:** M
**Dependencies:** IMPL-015, IMPL-016

### Description
Create the integration test infrastructure using Testcontainers with Oracle Free.

### Acceptance Criteria
- [ ] Test: Oracle container starts successfully
- [ ] Test: JSON collection can be created
- [ ] Test: Simple translation executes against Oracle
- [ ] Integration test runs in CI

### Test-First Implementation

#### Step 1: Write Integration Test

```java
// integration-tests/src/test/java/com/oracle/mongodb/translator/integration/OracleIntegrationTest.java
package com.oracle.mongodb.translator.integration;

import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.*;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OracleIntegrationTest {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23.6-slim-faststart")
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass")
        .withStartupTimeoutSeconds(300);

    private Connection connection;
    private AggregationTranslator translator;

    @BeforeAll
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(
            oracle.getJdbcUrl(),
            oracle.getUsername(),
            oracle.getPassword()
        );

        // Create test table
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE orders (
                    id RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
                    data JSON
                )
            """);

            // Insert test data
            stmt.execute("""
                INSERT INTO orders (data) VALUES
                ('{"orderId": 1, "status": "active", "amount": 100}')
            """);
            stmt.execute("""
                INSERT INTO orders (data) VALUES
                ('{"orderId": 2, "status": "active", "amount": 200}')
            """);
            stmt.execute("""
                INSERT INTO orders (data) VALUES
                ('{"orderId": 3, "status": "completed", "amount": 150}')
            """);
        }

        translator = AggregationTranslator.create(
            OracleConfiguration.builder()
                .connection(connection)
                .collectionName("orders")
                .build()
        );
    }

    @AfterAll
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void shouldConnectToOracle() throws SQLException {
        assertThat(connection.isValid(5)).isTrue();
    }

    @Test
    void shouldQueryJsonData() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT JSON_VALUE(data, '$.status') FROM orders")) {

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isNotNull();
        }
    }

    @Test
    void shouldTranslateAndExecuteLimitPipeline() throws SQLException {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 2}")
        );

        var result = translator.translate(pipeline);

        try (PreparedStatement ps = connection.prepareStatement(result.sql());
             ResultSet rs = ps.executeQuery()) {

            int count = 0;
            while (rs.next()) {
                count++;
            }

            assertThat(count).isEqualTo(2);
        }
    }

    @Test
    void shouldTranslateAndExecuteSkipLimitPipeline() throws SQLException {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 1}"),
            Document.parse("{\"$limit\": 1}")
        );

        var result = translator.translate(pipeline);

        try (PreparedStatement ps = connection.prepareStatement(result.sql());
             ResultSet rs = ps.executeQuery()) {

            int count = 0;
            while (rs.next()) {
                count++;
            }

            assertThat(count).isEqualTo(1);
        }
    }
}
```

### Files
- CREATE: `integration-tests/src/test/java/com/oracle/mongodb/translator/integration/OracleIntegrationTest.java`
- CREATE: `integration-tests/src/test/resources/testdata/orders.json`

### Definition of Done
- [ ] Integration test passes locally
- [ ] Integration test passes in CI
- [ ] Test data properly seeded

---
