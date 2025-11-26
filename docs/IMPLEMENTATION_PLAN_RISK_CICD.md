# Risk Register and CI/CD Pipeline Details

---

# Risk Register

## Technical Risks

### RISK-001: Oracle JSON Function Limitations

**Severity:** High
**Probability:** Medium
**Impact:** Some MongoDB operators may not have direct Oracle equivalents

**Description:**
Certain MongoDB aggregation operators have complex semantics that may not translate cleanly to Oracle SQL/JSON functions. Key areas of concern:

1. **$accumulator** - Custom JavaScript accumulators (deprecated in MongoDB 8.0, marked UNSUPPORTED)
2. **$function** - Arbitrary JavaScript execution (cannot translate, CLIENT_SIDE_ONLY)
3. **$graphLookup** - Recursive graph traversal requires complex recursive CTEs
4. **$geoNear** - Geospatial operations require Oracle Spatial licensing

**Mitigation Strategies:**
- Implement `TranslationCapability` enum to clearly indicate support levels
- Document limitations in operator specifications
- Provide `CLIENT_SIDE_ONLY` fallback for untranslatable operators
- Create hybrid execution mode that processes some stages in application code

**Contingency:**
- Prioritize EMULATED implementations for common use cases
- Consider Oracle Spatial add-on for geo operators if customer demand exists

---

### RISK-002: JSON_TABLE Performance

**Severity:** Medium
**Probability:** Medium
**Impact:** Complex pipelines with $unwind may have suboptimal performance

**Description:**
JSON_TABLE operations, especially with nested paths, can be slower than native MongoDB array operations. Each $unwind adds a CROSS APPLY-like operation.

**Mitigation Strategies:**
- Implement index recommendation engine for JSON paths
- Add `/*+ HINT */` comments to generated SQL
- Document performance implications in API docs
- Create benchmark suite to identify bottleneck patterns

**Contingency:**
- Implement query plan analysis to warn about expensive operations
- Consider materialized view suggestions for complex, repeated queries

---

### RISK-003: Type Coercion Edge Cases

**Severity:** Medium
**Probability:** High
**Impact:** Subtle type mismatches between MongoDB and Oracle

**Description:**
MongoDB is loosely typed; Oracle is strongly typed. Edge cases include:
- MongoDB numbers (int32, int64, double, decimal128) vs Oracle NUMBER
- MongoDB dates (ISODate, Timestamp) vs Oracle DATE/TIMESTAMP
- MongoDB ObjectId vs Oracle RAW(12)
- Null handling differences

**Mitigation Strategies:**
- Create comprehensive type mapping in `specs/type-mappings.json`
- Add `RETURNING` clause to JSON_VALUE for explicit type conversion
- Implement type inference from sample documents
- Add warnings for potential type issues

**Contingency:**
- Allow user configuration of type mappings
- Provide strict mode that fails on ambiguous types

---

### RISK-004: Large Result Set Handling

**Severity:** Medium
**Probability:** Medium
**Impact:** Memory issues with large aggregation results

**Description:**
MongoDB cursors are streaming; JDBC ResultSets can buffer entire results. Pipelines processing millions of documents could cause OOM errors.

**Mitigation Strategies:**
- Implement streaming execution via `aggregateAsStream()`
- Use JDBC fetch size hints
- Add warning for pipelines without $limit
- Document best practices for large datasets

**Contingency:**
- Implement pagination helper
- Add automatic $limit injection option

---

### RISK-005: Transaction Semantics

**Severity:** Low
**Probability:** Low
**Impact:** Different isolation/consistency guarantees

**Description:**
MongoDB aggregations are point-in-time snapshots. Oracle queries may see concurrent modifications unless using serializable isolation.

**Mitigation Strategies:**
- Document isolation level requirements
- Recommend transaction boundaries for sensitive operations
- Consider Oracle FLASHBACK for point-in-time queries

**Contingency:**
- Add `snapshotTime` option using Oracle SCN

---

## Schedule Risks

### RISK-006: $lookup Complexity

**Severity:** Medium
**Probability:** Medium
**Impact:** Tier 2 timeline may slip

**Description:**
The pipeline form of $lookup (with nested `pipeline` parameter) is complex to translate. It essentially requires translating a sub-pipeline.

**Mitigation Strategies:**
- Implement simple localField/foreignField form first
- Defer pipeline $lookup to Tier 3
- Track effort separately in tickets

**Contingency:**
- Mark pipeline $lookup as PARTIAL support initially
- Complete in separate release

---

### RISK-007: Integration Testing Infrastructure

**Severity:** Medium
**Probability:** Low
**Impact:** CI/CD pipeline delays

**Description:**
Oracle Testcontainers require significant resources (~2GB RAM) and startup time (~60-120 seconds). GitHub Actions may timeout.

**Mitigation Strategies:**
- Use gvenzl/oracle-free:slim-faststart image (fastest startup)
- Enable Testcontainers reuse in CI
- Separate unit tests from integration tests
- Use self-hosted runners if needed

**Contingency:**
- Use Oracle Cloud Free Tier for CI testing
- Implement mock Oracle dialect for unit tests

---

### RISK-008: Polyglot Port Effort

**Severity:** Low
**Probability:** Medium
**Impact:** Node.js/Python ports may not reach parity

**Description:**
Specification-driven code generation reduces effort, but Node.js and Python implementations need manual work for edge cases.

**Mitigation Strategies:**
- Focus on Java as primary implementation
- Generate skeleton code only for ports
- Share test fixtures via specs/test-cases/
- Community contributions welcome for ports

**Contingency:**
- Ports can be separate phase/release
- Consider TypeScript-first for Node.js (better type safety)

---

## Operational Risks

### RISK-009: Oracle Version Compatibility

**Severity:** Medium
**Probability:** Low
**Impact:** Library may not work on older Oracle versions

**Description:**
Oracle JSON features evolved significantly:
- Oracle 12c: Basic JSON functions
- Oracle 19c: JSON_TABLE improvements
- Oracle 21c: Native JSON data type
- Oracle 23ai/26ai: JSON Relational Duality, enhanced functions

**Mitigation Strategies:**
- Implement dialect-specific rendering
- Test against Oracle 21c, 23ai, 26ai
- Document minimum version requirements
- Use feature detection where possible

**Contingency:**
- Drop support for Oracle < 21c if feature gaps too large
- Maintain separate dialect branches if needed

---

### RISK-010: Security Vulnerabilities

**Severity:** High
**Probability:** Low
**Impact:** SQL injection or data exposure

**Description:**
Translating user-provided MongoDB queries to SQL creates injection risk if not properly parameterized.

**Mitigation Strategies:**
- ALWAYS use bind variables (never inline user data)
- Validate operator names against whitelist
- Use FindSecBugs for static analysis
- Regular OWASP dependency checks
- Security-focused code review for all PRs

**Contingency:**
- Security audit before 1.0 release
- Bug bounty program consideration

---

# Risk Probability/Impact Matrix

```
         │ Low Impact │ Medium Impact │ High Impact │
─────────┼────────────┼───────────────┼─────────────┤
High     │            │ RISK-003      │             │
Prob.    │            │               │             │
─────────┼────────────┼───────────────┼─────────────┤
Medium   │ RISK-008   │ RISK-002      │ RISK-001    │
Prob.    │            │ RISK-004      │             │
         │            │ RISK-006      │             │
─────────┼────────────┼───────────────┼─────────────┤
Low      │ RISK-005   │ RISK-007      │ RISK-010    │
Prob.    │            │ RISK-009      │             │
─────────┴────────────┴───────────────┴─────────────┘
```

---

# CI/CD Pipeline Details

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    GitHub Actions Workflow                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐                                               │
│  │ validate-    │                                               │
│  │ specs        │                                               │
│  └──────┬───────┘                                               │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────┐                                               │
│  │ build        │──────────────────────────────────────┐        │
│  │ (matrix)     │                                      │        │
│  │ Java 17, 21  │                                      │        │
│  └──────┬───────┘                                      │        │
│         │                                              │        │
│         ├────────────────────┬─────────────────┐       │        │
│         ▼                    ▼                 ▼       ▼        │
│  ┌──────────────┐    ┌──────────────┐  ┌──────────────┐        │
│  │ unit-tests   │    │ integration  │  │ security-    │        │
│  │ (matrix)     │    │ -tests       │  │ scan         │        │
│  └──────┬───────┘    └──────┬───────┘  └──────┬───────┘        │
│         │                   │                  │                 │
│         └───────────────────┴──────────────────┘                │
│                             │                                    │
│                             ▼                                    │
│                    ┌──────────────┐                              │
│                    │ quality-gate │                              │
│                    └──────┬───────┘                              │
│                           │                                      │
│                           ▼                                      │
│                    ┌──────────────┐                              │
│                    │ publish      │ (on release tag)             │
│                    │ artifacts    │                              │
│                    └──────────────┘                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Job Details

### Job: validate-specs

**Trigger:** All pushes and PRs
**Runner:** ubuntu-latest
**Duration:** ~30 seconds

**Steps:**
1. Checkout code
2. Set up Python 3.11
3. Install jsonschema library
4. Validate operators.json against schema
5. Validate type-mappings.json against schema
6. Check test fixture JSON syntax

**Success Criteria:**
- All JSON files valid
- Schema validation passes
- No duplicate operator definitions

**Failure Actions:**
- Block PR merge
- Notify author via GitHub comment

---

### Job: build

**Trigger:** After validate-specs passes
**Runner:** ubuntu-latest
**Matrix:** Java 17, Java 21
**Duration:** ~3 minutes

**Steps:**
1. Checkout code
2. Set up JDK (Temurin distribution)
3. Cache Gradle dependencies
4. Run `./gradlew build -x test`
5. Run `./gradlew checkstyleMain checkstyleTest`
6. Run `./gradlew spotbugsMain`
7. Upload build artifacts (JARs)

**Success Criteria:**
- Compilation succeeds
- No checkstyle violations
- No SpotBugs warnings (medium+ severity)

**Failure Actions:**
- Block subsequent jobs
- Upload checkstyle report as artifact

---

### Job: unit-tests

**Trigger:** After build passes
**Runner:** ubuntu-latest
**Matrix:** Java 17, Java 21
**Duration:** ~2 minutes

**Steps:**
1. Checkout code
2. Set up JDK
3. Restore Gradle cache
4. Run `./gradlew :core:test`
5. Generate JaCoCo coverage report
6. Verify coverage thresholds (80% line, 75% branch)
7. Upload coverage to Codecov
8. Upload test results as artifact

**Success Criteria:**
- All unit tests pass
- Coverage thresholds met

**Failure Actions:**
- Mark build as failed
- Publish test failure report
- Coverage delta shown in PR

---

### Job: integration-tests

**Trigger:** After unit-tests passes
**Runner:** ubuntu-latest
**Duration:** ~5 minutes

**Environment:**
- Oracle container (gvenzl/oracle-free:23.6-slim-faststart)
- ORACLE_JDBC_URL, ORACLE_USERNAME, ORACLE_PASSWORD set

**Steps:**
1. Checkout code
2. Set up JDK 17
3. Restore Gradle cache
4. Wait for Oracle container health check
5. Run `./gradlew :integration-tests:test`
6. Upload test results as artifact

**Oracle Container Configuration:**
```yaml
services:
  oracle:
    image: gvenzl/oracle-free:23.6-slim-faststart
    env:
      ORACLE_PASSWORD: testpassword
      APP_USER: testuser
      APP_USER_PASSWORD: testpass
    ports:
      - 1521:1521
    options: >-
      --health-cmd healthcheck.sh
      --health-interval 10s
      --health-timeout 5s
      --health-retries 20
```

**Success Criteria:**
- All integration tests pass
- Oracle queries execute successfully
- Results match expected values

**Failure Actions:**
- Mark build as failed
- Upload Oracle logs as artifact

---

### Job: security-scan

**Trigger:** After build passes (parallel with tests)
**Runner:** ubuntu-latest
**Duration:** ~5 minutes

**Steps:**
1. Checkout code
2. Set up JDK 17
3. Run OWASP Dependency Check
   - Fail build on CVSS >= 7.0
   - Use NVD API key for rate limit bypass
4. Run SpotBugs with FindSecBugs plugin
5. Upload dependency-check report as artifact

**Success Criteria:**
- No high-severity CVEs in dependencies
- No security bugs detected by FindSecBugs

**Failure Actions:**
- Mark build as failed
- Create security advisory if real vulnerability found
- Notify security team for triage

---

### Job: quality-gate

**Trigger:** After unit-tests, integration-tests, security-scan all pass
**Runner:** ubuntu-latest
**Duration:** ~30 seconds

**Steps:**
1. Checkout code
2. Verify all required checks passed
3. Post status to PR

**Success Criteria:**
- All previous jobs green
- PR approval required if changing core/

---

### Job: publish (Release Only)

**Trigger:** On tag push matching `v*`
**Runner:** ubuntu-latest
**Duration:** ~2 minutes

**Steps:**
1. Checkout code with full history
2. Set up JDK 17
3. Build release artifacts
4. Sign artifacts with GPG
5. Publish to Maven Central (via Sonatype)
6. Create GitHub Release
7. Upload artifacts to release

**Environment Variables:**
- SONATYPE_USERNAME
- SONATYPE_PASSWORD
- GPG_SIGNING_KEY
- GPG_PASSPHRASE

---

## Gradle Task Reference

### Core Tasks

| Task | Description | Command |
|------|-------------|---------|
| build | Compile and package | `./gradlew build` |
| test | Run unit tests | `./gradlew :core:test` |
| integrationTest | Run integration tests | `./gradlew :integration-tests:test` |
| check | Run all quality checks | `./gradlew check` |
| jacocoTestReport | Generate coverage report | `./gradlew jacocoTestReport` |
| jacocoTestCoverageVerification | Verify coverage thresholds | `./gradlew jacocoTestCoverageVerification` |

### Quality Tasks

| Task | Description | Command |
|------|-------------|---------|
| checkstyleMain | Check main source style | `./gradlew checkstyleMain` |
| checkstyleTest | Check test source style | `./gradlew checkstyleTest` |
| spotbugsMain | Run SpotBugs analysis | `./gradlew spotbugsMain` |
| dependencyCheckAnalyze | OWASP dependency scan | `./gradlew dependencyCheckAnalyze` |

### Utility Tasks

| Task | Description | Command |
|------|-------------|---------|
| clean | Remove build outputs | `./gradlew clean` |
| dependencies | Show dependency tree | `./gradlew dependencies` |
| tasks | List available tasks | `./gradlew tasks` |

---

## Environment Variables

### Required for CI

| Variable | Description | Where Set |
|----------|-------------|-----------|
| JAVA_VERSION | Target Java version | Workflow file |
| GRADLE_OPTS | Gradle JVM options | Workflow file |
| TESTCONTAINERS_RYUK_DISABLED | Disable Ryuk (CI) | Workflow env |

### Secrets Required

| Secret | Description | Usage |
|--------|-------------|-------|
| CODECOV_TOKEN | Codecov upload | Coverage reporting |
| NVD_API_KEY | NVD database key | Dependency check |
| SONATYPE_USERNAME | Maven Central | Release publishing |
| SONATYPE_PASSWORD | Maven Central | Release publishing |
| GPG_SIGNING_KEY | Artifact signing | Release publishing |
| GPG_PASSPHRASE | Artifact signing | Release publishing |

---

## Local Development Workflow

### Pre-commit Hooks

Install pre-commit hooks:
```bash
pip install pre-commit
pre-commit install
```

Hooks run on each commit:
1. Checkstyle (Java style)
2. SpotBugs (security)
3. Unit tests (core module)
4. Trailing whitespace fix
5. End-of-file fix

### Running Tests Locally

```bash
# Unit tests only
./gradlew :core:test

# Integration tests (requires Docker)
docker-compose up -d
./gradlew :integration-tests:test

# All tests with coverage
./gradlew test jacocoTestReport

# View coverage report
open core/build/reports/jacoco/test/html/index.html
```

### IDE Setup (IntelliJ IDEA)

1. Import as Gradle project
2. Enable annotation processing
3. Install plugins:
   - Checkstyle-IDEA
   - SpotBugs
4. Configure code style:
   - Import `config/checkstyle/google_checks.xml`
   - Set to Google Java Style
5. Set SDK to Java 17+

---

## Branch Strategy

```
main
  │
  ├── develop (default branch)
  │     │
  │     ├── feature/IMPL-001-gradle-setup
  │     ├── feature/IMPL-002-directory-structure
  │     ├── ...
  │     │
  │     └── release/v1.0.0
  │           │
  │           └── tag: v1.0.0
  │
  └── hotfix/security-patch-001 (rare)
```

### Branch Rules

| Branch | Protection | Requirements |
|--------|------------|--------------|
| main | Yes | PR required, 2 approvals, CI pass |
| develop | Yes | PR required, 1 approval, CI pass |
| feature/* | No | - |
| release/* | Yes | PR to main required, CI pass |

---

## Release Process

1. **Prepare Release**
   ```bash
   git checkout develop
   git pull
   git checkout -b release/v1.0.0
   # Update version in build.gradle.kts
   # Update CHANGELOG.md
   git commit -m "Prepare release v1.0.0"
   git push -u origin release/v1.0.0
   ```

2. **Create PR to main**
   - Title: "Release v1.0.0"
   - Get approvals
   - CI must pass

3. **Merge and Tag**
   ```bash
   # After PR merged
   git checkout main
   git pull
   git tag -s v1.0.0 -m "Release v1.0.0"
   git push origin v1.0.0
   ```

4. **Verify Publication**
   - GitHub Release auto-created
   - Maven Central artifact available (~2 hours)

5. **Post-Release**
   ```bash
   git checkout develop
   git merge main
   # Bump to next snapshot version
   git commit -m "Start development on v1.1.0-SNAPSHOT"
   git push
   ```

---
