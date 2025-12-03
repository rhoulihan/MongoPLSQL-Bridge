#!/usr/bin/env python3
"""
Generates the test-catalog.md documentation file.
Called automatically by run-tests.sh after each test run.

Reads test definitions from test-cases.json, test results from test-report-latest.json,
generates fresh SQL using the translator, and outputs docs/test-catalog.md.
"""

import json
import subprocess
import sys
import os
from datetime import datetime
from collections import defaultdict

try:
    import sqlparse
    HAS_SQLPARSE = True
except ImportError:
    HAS_SQLPARSE = False

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
QUERY_TESTS_DIR = os.path.dirname(SCRIPT_DIR)
PROJECT_ROOT = os.path.dirname(QUERY_TESTS_DIR)
TEST_CASES_FILE = os.path.join(QUERY_TESTS_DIR, "tests", "test-cases.json")
TEST_RESULTS_FILE = os.path.join(QUERY_TESTS_DIR, "results", "test-report-latest.json")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "docs", "test-catalog.md")

# Cache for generated SQL to avoid regenerating
sql_cache = {}


def format_sql(sql: str) -> str:
    """Format SQL for readability using sqlparse if available."""
    if not HAS_SQLPARSE or not sql or sql.startswith("--"):
        return sql
    try:
        formatted = sqlparse.format(
            sql,
            reindent=True,
            keyword_case='upper',
            indent_width=2
        )
        return formatted
    except Exception:
        return sql


def generate_sql(collection: str, pipeline: list) -> str:
    """Generate Oracle SQL using the translator with inline mode."""
    cache_key = f"{collection}:{json.dumps(pipeline)}"
    if cache_key in sql_cache:
        return sql_cache[cache_key]

    pipeline_json = json.dumps(pipeline)

    cmd = [
        "./gradlew",
        ":core:translatePipeline",
        f"-PcollectionName={collection}",
        f"-PpipelineJson={pipeline_json}",
        "-Pinline=true",
        "--quiet"
    ]

    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=60
        )

        if result.returncode == 0:
            sql = result.stdout.strip()
            if sql and not sql.startswith("Error"):
                sql_cache[cache_key] = sql
                return sql
        return f"-- Error generating SQL: {result.stderr.strip()}"
    except subprocess.TimeoutExpired:
        return "-- Timeout generating SQL"
    except Exception as e:
        return f"-- Exception: {e}"


def load_test_results() -> dict:
    """Load test results from the JSON file."""
    results = {}
    try:
        with open(TEST_RESULTS_FILE, 'r') as f:
            content = f.read().strip()
            # Handle the JSON format (may have trailing commas or be malformed)
            data = json.loads(content)
            for result in data.get('results', []):
                results[result['id']] = result
    except Exception as e:
        print(f"Warning: Could not load test results: {e}", file=sys.stderr)
    return results


def get_status_emoji(status: str) -> str:
    """Return emoji for test status."""
    if status == "PASS":
        return "PASS"
    elif status == "FAIL":
        return "FAIL"
    elif status == "SKIP":
        return "SKIP"
    return "?"


def format_category_name(category: str) -> str:
    """Format category name for display."""
    return category.replace("_", " ").title()


def main():
    # Load test cases
    with open(TEST_CASES_FILE, 'r') as f:
        data = json.load(f)

    test_cases = data['test_cases']

    # Load test results
    results = load_test_results()

    # Group tests by category
    tests_by_category = defaultdict(list)
    for test in test_cases:
        category = test.get('category', 'uncategorized')
        tests_by_category[category].append(test)

    # Count categories
    category_counts = {cat: len(tests) for cat, tests in tests_by_category.items()}
    total_tests = sum(category_counts.values())

    # Count pass/fail/skip
    pass_count = sum(1 for r in results.values() if r.get('status') == 'PASS')
    fail_count = sum(1 for r in results.values() if r.get('status') == 'FAIL')
    skip_count = sum(1 for r in results.values() if r.get('status') == 'SKIP')

    # Generate output
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    output = []
    output.append("# Test Catalog")
    output.append("")
    output.append(f"*Auto-generated on {timestamp}. SQL is regenerated fresh each time this catalog is built.*")
    output.append("")
    output.append("---")
    output.append("")
    output.append("## Test Suites Overview")
    output.append("")
    output.append("| Suite | Test Count | Description |")
    output.append("|-------|------------|-------------|")
    output.append(f"| Unit Integration Tests | {total_tests} | Operator-level validation tests |")
    output.append("| Large-Scale Pipelines | 10 | Complex cross-database validation tests |")
    output.append(f"| **Total** | **{total_tests + 10}** | |")
    output.append("")

    # Test Results Summary
    output.append("## Test Results Summary")
    output.append("")
    if results:
        output.append(f"| Status | Count |")
        output.append("|--------|-------|")
        output.append(f"| Passed | {pass_count} |")
        output.append(f"| Failed | {fail_count} |")
        output.append(f"| Skipped | {skip_count} |")
        output.append(f"| **Total** | **{pass_count + fail_count + skip_count}** |")
    else:
        output.append("*No test results available. Run tests to populate this section.*")
    output.append("")
    output.append("---")
    output.append("")

    output.append("## Unit Integration Tests by Category")
    output.append("")
    output.append("| Category | Test Count |")
    output.append("|----------|------------|")

    # Sort categories alphabetically
    for category in sorted(category_counts.keys()):
        count = category_counts[category]
        output.append(f"| {format_category_name(category)} | {count} |")

    output.append("")
    output.append("---")
    output.append("")

    # Individual tests by category
    for category in sorted(tests_by_category.keys()):
        tests = tests_by_category[category]

        output.append(f"## {format_category_name(category)} Tests")
        output.append("")

        for test in tests:
            test_id = test['id']
            name = test['name']
            description = test.get('description', '')
            collection = test.get('collection', 'unknown')
            operator = test.get('operator', 'N/A')
            pipeline = test.get('mongodb_pipeline', [])

            # Get test results
            result = results.get(test_id, {})
            status = result.get('status', 'N/A')
            mongodb_count = result.get('mongodb_count', 'N/A')
            oracle_count = result.get('oracle_count', 'N/A')

            output.append(f"### {test_id}: {name}")
            output.append("")
            output.append(f"**Description:** {description}  ")
            output.append(f"**Collection:** `{collection}`  ")
            output.append(f"**Operator:** `{operator}`  ")
            output.append("")

            # Test Results
            output.append("**Test Results:**")
            output.append(f"- Status: **{status}**")
            output.append(f"- MongoDB Count: {mongodb_count}")
            output.append(f"- Oracle Count: {oracle_count}")
            output.append("")

            output.append("**MongoDB Pipeline:**")
            output.append("```json")
            output.append(json.dumps(pipeline, indent=2))
            output.append("```")
            output.append("")

            # Generate SQL
            print(f"  Generating SQL for {test_id}...", file=sys.stderr)
            sql = generate_sql(collection, pipeline)

            output.append("**Generated SQL:**")
            output.append("```sql")
            output.append(format_sql(sql))
            output.append("```")
            output.append("")
            output.append("---")
            output.append("")

    # Write output file
    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(output))

    print(f"Generated {OUTPUT_FILE}", file=sys.stderr)
    print(f"  {total_tests} tests documented", file=sys.stderr)
    print(f"  {pass_count} passed, {fail_count} failed, {skip_count} skipped", file=sys.stderr)


if __name__ == "__main__":
    main()
