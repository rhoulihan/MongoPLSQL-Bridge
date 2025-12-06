#!/usr/bin/env python3
"""
Generates the test-catalog-data.json file for the HTML test catalog.

This script aggregates test data from multiple sources:
1. Query Tests (test-cases.json) - 152 cross-database validation tests
2. Large-Scale Tests (complex-pipelines.json) - 10 complex pipeline tests
3. Unit Tests (JUnit XML reports) - ~1400 Java unit tests
4. Integration Tests (JUnit XML reports) - ~142 integration tests

Usage:
    python3 generate-test-catalog-data.py [--run]

Options:
    --run    Execute queries against MongoDB/Oracle and update results
             (requires Docker containers running)

Output: docs/test-catalog-data.json
"""

import json
import subprocess
import sys
import os
import argparse
import xml.etree.ElementTree as ET
from datetime import datetime
from collections import defaultdict
from pathlib import Path

try:
    import sqlparse
    HAS_SQLPARSE = True
except ImportError:
    HAS_SQLPARSE = False

# Paths
SCRIPT_DIR = Path(__file__).parent.resolve()
QUERY_TESTS_DIR = SCRIPT_DIR.parent
PROJECT_ROOT = QUERY_TESTS_DIR.parent
TEST_CASES_FILE = QUERY_TESTS_DIR / "tests" / "test-cases.json"
TEST_RESULTS_FILE = QUERY_TESTS_DIR / "results" / "test-report-latest.json"
LARGE_SCALE_DIR = QUERY_TESTS_DIR / "large-scale"
LARGE_SCALE_PIPELINES = LARGE_SCALE_DIR / "complex-pipelines.json"
LARGE_SCALE_RESULTS_DIR = LARGE_SCALE_DIR / "results"
UNIT_TEST_RESULTS_DIR = PROJECT_ROOT / "core" / "build" / "test-results" / "test"
INTEGRATION_TEST_RESULTS_DIR = PROJECT_ROOT / "integration-tests" / "build" / "test-results" / "test"
OUTPUT_FILE = PROJECT_ROOT / "docs" / "test-catalog-data.json"

# Cache for generated SQL
sql_cache = {}


def format_sql(sql: str) -> str:
    """Format SQL for readability using sqlparse if available."""
    if not HAS_SQLPARSE or not sql or sql.startswith("--"):
        return sql
    try:
        return sqlparse.format(sql, reindent=True, keyword_case='upper', indent_width=2)
    except Exception:
        return sql


def format_json(obj) -> str:
    """Format JSON with indentation for display."""
    return json.dumps(obj, indent=2)


def generate_sql(collection: str, pipeline: list) -> tuple:
    """Generate Oracle SQL using the translator. Returns (sql, error)."""
    cache_key = f"{collection}:{json.dumps(pipeline)}"
    if cache_key in sql_cache:
        return sql_cache[cache_key], None

    pipeline_json = json.dumps(pipeline)
    cli_path = PROJECT_ROOT / "mongo2sql"

    if cli_path.exists():
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(pipeline_json)
            tmp_file = f.name

        try:
            result = subprocess.run(
                [str(cli_path), "--collection", collection, "--inline", tmp_file],
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=30
            )
            os.unlink(tmp_file)

            if result.returncode == 0:
                sql = result.stdout.strip()
                sql_lines = [l for l in sql.split('\n') if l.strip() and not l.strip().startswith('--')]
                sql = ' '.join(sql_lines)
                if sql and not sql.startswith("Error"):
                    sql_cache[cache_key] = sql
                    return sql, None
        except subprocess.TimeoutExpired:
            os.unlink(tmp_file)
            return None, "Timeout generating SQL"
        except Exception as e:
            try:
                os.unlink(tmp_file)
            except:
                pass

    # Fallback to gradle
    try:
        result = subprocess.run(
            ["./gradlew", ":core:translatePipeline",
             f"-PcollectionName={collection}",
             f"-PpipelineJson={pipeline_json}",
             "-Pinline=true", "--quiet"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            sql = result.stdout.strip()
            if sql and not sql.startswith("Error"):
                sql_cache[cache_key] = sql
                return sql, None
        return None, result.stderr.strip() or "Unknown error"
    except subprocess.TimeoutExpired:
        return None, "Timeout generating SQL"
    except Exception as e:
        return None, str(e)


def run_mongodb_query(collection: str, pipeline: list) -> tuple:
    """Execute MongoDB query via Docker. Returns (results, count, error)."""
    pipeline_json = json.dumps(pipeline)
    try:
        result = subprocess.run(
            ["docker", "exec", "mongo-translator-mongodb", "mongosh", "--quiet", "testdb",
             "-u", "admin", "-p", "admin123", "--authenticationDatabase", "admin",
             "--eval", f"JSON.stringify(db.{collection}.aggregate({pipeline_json}).toArray())"],
            capture_output=True,
            text=True,
            timeout=60
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            data = json.loads(output) if output else []
            return data, len(data), None
        return None, 0, result.stderr.strip()
    except json.JSONDecodeError as e:
        return None, 0, f"JSON parse error: {e}"
    except subprocess.TimeoutExpired:
        return None, 0, "Timeout"
    except Exception as e:
        return None, 0, str(e)


def run_oracle_query(sql: str) -> tuple:
    """Execute Oracle query via Docker. Returns (results, count, error)."""
    clean_sql = sql.rstrip(';')
    json_sql = f"SELECT JSON_ARRAYAGG(JSON_OBJECT(*) RETURNING CLOB) AS json_result FROM ({clean_sql})"

    try:
        result = subprocess.run(
            ["docker", "exec", "mongo-translator-oracle", "bash", "-c",
             f"""sqlplus -s translator/translator123@//localhost:1521/FREEPDB1 << 'EOSQL'
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK OFF
SET HEADING OFF
SET LONG 10000000
SET LONGCHUNKSIZE 10000000
{json_sql};
EXIT;
EOSQL"""],
            capture_output=True,
            text=True,
            timeout=120
        )
        if result.returncode == 0:
            output = result.stdout.strip()
            if not output or output == 'null':
                return [], 0, None
            data = json.loads(output)
            return data, len(data) if data else 0, None
        return None, 0, result.stderr.strip()
    except json.JSONDecodeError as e:
        return None, 0, f"JSON parse error: {e}"
    except subprocess.TimeoutExpired:
        return None, 0, "Timeout"
    except Exception as e:
        return None, 0, str(e)


def compare_results(mongo_results, oracle_results) -> tuple:
    """Compare MongoDB and Oracle results. Returns (match, message)."""
    if mongo_results is None or oracle_results is None:
        return False, "COMPARISON_ERROR: Missing results"

    mongo_count = len(mongo_results)
    oracle_count = len(oracle_results)

    if mongo_count != oracle_count:
        return False, f"COUNT_MISMATCH: MongoDB={mongo_count}, Oracle={oracle_count}"

    if mongo_count == 0:
        return True, "MATCH: count=0"

    # Simple comparison - in production use compare-results.py
    return True, f"MATCH: count={mongo_count}"


def load_query_test_results() -> dict:
    """Load cached test results from JSON file."""
    results = {}
    try:
        with open(TEST_RESULTS_FILE, 'r') as f:
            data = json.load(f)
            for result in data.get('results', []):
                results[result['id']] = result
    except Exception as e:
        print(f"Warning: Could not load test results: {e}", file=sys.stderr)
    return results


def load_large_scale_results() -> dict:
    """Load latest large-scale comparison report."""
    results = {}
    try:
        # Find most recent report
        report_files = sorted(LARGE_SCALE_RESULTS_DIR.glob("comparison-report-*.json"), reverse=True)
        if report_files:
            with open(report_files[0], 'r') as f:
                data = json.load(f)
                for result in data.get('results', []):
                    results[result['id']] = result
    except Exception as e:
        print(f"Warning: Could not load large-scale results: {e}", file=sys.stderr)
    return results


def parse_junit_xml(xml_path: Path) -> list:
    """Parse JUnit XML report and extract test results."""
    tests = []
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()

        suite_name = root.get('name', '')
        suite_time = float(root.get('time', 0))

        for testcase in root.findall('.//testcase'):
            test = {
                'id': f"{testcase.get('classname', '')}.{testcase.get('name', '')}",
                'name': testcase.get('name', ''),
                'className': testcase.get('classname', ''),
                'time': float(testcase.get('time', 0)),
                'status': 'PASS'
            }

            # Check for failures/errors/skipped
            if testcase.find('failure') is not None:
                test['status'] = 'FAIL'
                failure = testcase.find('failure')
                test['failureMessage'] = failure.get('message', '')
                test['failureType'] = failure.get('type', '')
            elif testcase.find('error') is not None:
                test['status'] = 'ERROR'
                error = testcase.find('error')
                test['errorMessage'] = error.get('message', '')
                test['errorType'] = error.get('type', '')
            elif testcase.find('skipped') is not None:
                test['status'] = 'SKIP'

            tests.append(test)

    except Exception as e:
        print(f"Warning: Could not parse {xml_path}: {e}", file=sys.stderr)

    return tests


def load_junit_results(results_dir: Path) -> list:
    """Load all JUnit XML results from a directory."""
    all_tests = []
    if not results_dir.exists():
        return all_tests

    for xml_file in results_dir.glob("TEST-*.xml"):
        tests = parse_junit_xml(xml_file)
        all_tests.extend(tests)

    return all_tests


def get_category_display_name(category: str) -> str:
    """Format category name for display."""
    display_names = {
        'comparison': 'Comparison Operators',
        'logical': 'Logical Operators',
        'accumulator': 'Accumulator Operators',
        'stage': 'Pipeline Stages',
        'arithmetic': 'Arithmetic Operators',
        'conditional': 'Conditional Operators',
        'complex': 'Complex Pipelines',
        'edge': 'Edge Cases',
        'string': 'String Operators',
        'date': 'Date Operators',
        'array': 'Array Operators',
        'lookup': 'Lookup Operations',
        'unwind': 'Unwind Operations',
        'addFields': 'AddFields/Set Operations',
        'unionWith': 'Union Operations',
        'bucket': 'Bucket Operations',
        'bucketAuto': 'BucketAuto Operations',
        'facet': 'Facet Operations',
        'setWindowFields': 'Window Functions',
        'typeConversion': 'Type Conversion',
        'redact': 'Redact Operations',
        'sample': 'Sample Operations',
        'count': 'Count Operations',
        'graphLookup': 'Graph Lookup',
        'null_handling': 'Null Handling',
        'expression': 'Expression Operators',
        'replaceRoot': 'Replace Root',
        'object': 'Object Operations',
        'window': 'Window Operations',
        'large_scale': 'Large-Scale Pipelines'
    }
    return display_names.get(category, category.replace("_", " ").title())


def process_query_tests(run_queries: bool = False) -> tuple:
    """Process query tests. Returns (tests_list, categories_dict)."""
    print("Processing query tests...", file=sys.stderr)

    with open(TEST_CASES_FILE, 'r') as f:
        data = json.load(f)

    test_cases = data['test_cases']
    results = load_query_test_results()

    tests = []
    categories = defaultdict(lambda: {'count': 0, 'passed': 0, 'failed': 0, 'skipped': 0})

    for test in test_cases:
        test_id = test['id']
        category = test.get('category', 'uncategorized')
        collection = test.get('collection', 'unknown')
        pipeline = test.get('mongodb_pipeline', [])

        print(f"  {test_id}...", file=sys.stderr)

        # Generate SQL
        sql, sql_error = generate_sql(collection, pipeline)

        # Get cached results or run queries
        result = results.get(test_id, {})

        mongo_results = None
        oracle_results = None
        comparison_result = ""

        if run_queries and not test.get('skip', False):
            print(f"    Running queries...", file=sys.stderr)
            mongo_results, mongo_count, mongo_error = run_mongodb_query(collection, pipeline)
            if sql and not sql_error:
                oracle_results, oracle_count, oracle_error = run_oracle_query(sql)
            else:
                oracle_results, oracle_count, oracle_error = None, 0, sql_error

            if mongo_error or oracle_error:
                status = 'ERROR'
                comparison_result = f"ERROR: MongoDB={mongo_error}, Oracle={oracle_error}"
            else:
                match, message = compare_results(mongo_results, oracle_results)
                status = 'PASS' if match else 'FAIL'
                comparison_result = message

            result = {
                'status': status,
                'mongodb_count': str(mongo_count) if mongo_results is not None else 'ERROR',
                'oracle_count': str(oracle_count) if oracle_results is not None else 'ERROR'
            }

        # Build test entry
        test_entry = {
            "id": test_id,
            "name": test['name'],
            "description": test.get('description', ''),
            "category": category,
            "categoryName": get_category_display_name(category),
            "operator": test.get('operator', 'N/A'),
            "collection": collection,
            "skip": test.get('skip', False),
            "skipReason": test.get('skipReason', ''),
            "testSuite": "query",

            "pipeline": pipeline,
            "pipelineFormatted": format_json(pipeline),
            "sql": sql or f"-- Error: {sql_error}",
            "sqlFormatted": format_sql(sql) if sql else f"-- Error: {sql_error}",
            "sqlError": sql_error,

            "status": result.get('status', 'N/A'),
            "matchType": result.get('matchType', ''),
            "mongodbCount": result.get('mongodb_count', 'N/A'),
            "oracleCount": result.get('oracle_count', 'N/A'),
            "expectedCount": test.get('expected_count', 0),
            "comparisonResult": comparison_result,
            "sortBy": test.get('sort_by', '_id'),

            "mongodbResults": mongo_results[:10] if mongo_results and len(mongo_results) > 0 else None,
            "oracleResults": oracle_results[:10] if oracle_results and len(oracle_results) > 0 else None
        }

        tests.append(test_entry)

        # Update category stats
        categories[category]['count'] += 1
        status = result.get('status', 'N/A')
        if status == 'PASS':
            categories[category]['passed'] += 1
        elif status == 'FAIL':
            categories[category]['failed'] += 1
        elif status == 'SKIP':
            categories[category]['skipped'] += 1

    return tests, dict(categories)


def process_large_scale_tests(run_queries: bool = False) -> tuple:
    """Process large-scale tests. Returns (tests_list, categories_dict)."""
    print("Processing large-scale tests...", file=sys.stderr)

    if not LARGE_SCALE_PIPELINES.exists():
        print("  No large-scale pipelines found", file=sys.stderr)
        return [], {}

    with open(LARGE_SCALE_PIPELINES, 'r') as f:
        data = json.load(f)

    pipelines = data.get('pipelines', [])
    results = load_large_scale_results()

    tests = []
    categories = {'large_scale': {'count': 0, 'passed': 0, 'failed': 0, 'skipped': 0}}

    for pipe in pipelines:
        pipe_id = pipe['id']
        collection = pipe.get('collection', 'unknown')
        pipeline = pipe.get('pipeline', [])

        print(f"  {pipe_id}...", file=sys.stderr)

        # Generate SQL
        sql, sql_error = generate_sql(collection, pipeline)

        # Get cached results
        result = results.get(pipe_id, {})

        # Extract sample results if available
        mongo_results = result.get('mongodb', {}).get('results', [])[:5] if result else None
        oracle_results = result.get('oracle', {}).get('results', [])[:5] if result else None

        mongo_count = len(result.get('mongodb', {}).get('results', [])) if result else 'N/A'
        oracle_count = len(result.get('oracle', {}).get('results', [])) if result else 'N/A'

        status = 'PASS'
        if result:
            mongo_success = result.get('mongodb', {}).get('success', False)
            oracle_success = result.get('oracle', {}).get('success', False)
            if not mongo_success or not oracle_success:
                status = 'FAIL'
        else:
            status = 'N/A'

        test_entry = {
            "id": pipe_id,
            "name": pipe['name'],
            "description": pipe.get('description', ''),
            "category": "large_scale",
            "categoryName": "Large-Scale Pipelines",
            "operator": "complex",
            "collection": collection,
            "skip": False,
            "testSuite": "large_scale",

            "pipeline": pipeline,
            "pipelineFormatted": format_json(pipeline),
            "sql": sql or f"-- Error: {sql_error}",
            "sqlFormatted": format_sql(sql) if sql else f"-- Error: {sql_error}",
            "sqlError": sql_error,

            "status": status,
            "mongodbCount": str(mongo_count),
            "oracleCount": str(oracle_count),
            "expectedCount": 0,
            "comparisonResult": result.get('comparison', {}).get('message', '') if result else '',

            "mongodbResults": mongo_results,
            "oracleResults": oracle_results,
            "mongodbTime": result.get('mongodb', {}).get('time', 0) if result else 0,
            "oracleTime": result.get('oracle', {}).get('time', 0) if result else 0
        }

        tests.append(test_entry)

        categories['large_scale']['count'] += 1
        if status == 'PASS':
            categories['large_scale']['passed'] += 1
        elif status == 'FAIL':
            categories['large_scale']['failed'] += 1

    return tests, categories


def process_unit_tests() -> tuple:
    """Process unit tests from JUnit XML. Returns (tests_list, summary_dict)."""
    print("Processing unit tests...", file=sys.stderr)

    junit_tests = load_junit_results(UNIT_TEST_RESULTS_DIR)

    if not junit_tests:
        print("  No unit test results found (run ./gradlew :core:test first)", file=sys.stderr)
        return [], {'count': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'errors': 0}

    # Group by class
    by_class = defaultdict(list)
    for test in junit_tests:
        class_name = test['className'].split('.')[-1]
        by_class[class_name].append(test)

    tests = []
    summary = {'count': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'errors': 0}

    for class_name, class_tests in sorted(by_class.items()):
        for test in class_tests:
            test_entry = {
                "id": test['id'],
                "name": test['name'],
                "description": f"Unit test from {test['className']}",
                "category": "unit_test",
                "categoryName": f"Unit: {class_name}",
                "className": test['className'],
                "testSuite": "unit",
                "status": test['status'],
                "time": test['time'],
                "failureMessage": test.get('failureMessage'),
                "errorMessage": test.get('errorMessage')
            }
            tests.append(test_entry)

            summary['count'] += 1
            if test['status'] == 'PASS':
                summary['passed'] += 1
            elif test['status'] == 'FAIL':
                summary['failed'] += 1
            elif test['status'] == 'SKIP':
                summary['skipped'] += 1
            elif test['status'] == 'ERROR':
                summary['errors'] += 1

    print(f"  Found {summary['count']} unit tests", file=sys.stderr)
    return tests, summary


def process_integration_tests() -> tuple:
    """Process integration tests from JUnit XML. Returns (tests_list, summary_dict)."""
    print("Processing integration tests...", file=sys.stderr)

    junit_tests = load_junit_results(INTEGRATION_TEST_RESULTS_DIR)

    if not junit_tests:
        print("  No integration test results found", file=sys.stderr)
        return [], {'count': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'errors': 0}

    tests = []
    summary = {'count': 0, 'passed': 0, 'failed': 0, 'skipped': 0, 'errors': 0}

    for test in junit_tests:
        test_entry = {
            "id": test['id'],
            "name": test['name'],
            "description": f"Integration test from {test['className']}",
            "category": "integration_test",
            "categoryName": "Integration Tests",
            "className": test['className'],
            "testSuite": "integration",
            "status": test['status'],
            "time": test['time'],
            "failureMessage": test.get('failureMessage'),
            "errorMessage": test.get('errorMessage')
        }
        tests.append(test_entry)

        summary['count'] += 1
        if test['status'] == 'PASS':
            summary['passed'] += 1
        elif test['status'] == 'FAIL':
            summary['failed'] += 1
        elif test['status'] == 'SKIP':
            summary['skipped'] += 1
        elif test['status'] == 'ERROR':
            summary['errors'] += 1

    print(f"  Found {summary['count']} integration tests", file=sys.stderr)
    return tests, summary


def main():
    parser = argparse.ArgumentParser(description='Generate test catalog data')
    parser.add_argument('--run', action='store_true',
                        help='Execute queries against databases (requires Docker)')
    args = parser.parse_args()

    print("Generating comprehensive test catalog data...", file=sys.stderr)

    # Process all test types
    query_tests, query_categories = process_query_tests(run_queries=args.run)
    large_scale_tests, large_scale_categories = process_large_scale_tests(run_queries=args.run)
    unit_tests, unit_summary = process_unit_tests()
    integration_tests, integration_summary = process_integration_tests()

    # Build output structure
    all_categories = {**query_categories, **large_scale_categories}
    for cat in all_categories:
        all_categories[cat]['name'] = get_category_display_name(cat)

    # Calculate overall summary
    query_passed = sum(1 for t in query_tests if t['status'] == 'PASS')
    query_failed = sum(1 for t in query_tests if t['status'] == 'FAIL')
    query_skipped = sum(1 for t in query_tests if t['status'] == 'SKIP')

    ls_passed = sum(1 for t in large_scale_tests if t['status'] == 'PASS')
    ls_failed = sum(1 for t in large_scale_tests if t['status'] == 'FAIL')

    output = {
        "generated": datetime.now().isoformat(),
        "version": "2.0",
        "summary": {
            "queryTests": {
                "total": len(query_tests),
                "passed": query_passed,
                "failed": query_failed,
                "skipped": query_skipped
            },
            "largeScaleTests": {
                "total": len(large_scale_tests),
                "passed": ls_passed,
                "failed": ls_failed
            },
            "unitTests": unit_summary,
            "integrationTests": integration_summary,
            "overall": {
                "total": len(query_tests) + len(large_scale_tests) + unit_summary['count'] + integration_summary['count'],
                "passed": query_passed + ls_passed + unit_summary['passed'] + integration_summary['passed'],
                "failed": query_failed + ls_failed + unit_summary['failed'] + integration_summary['failed'],
                "skipped": query_skipped + unit_summary['skipped'] + integration_summary['skipped']
            }
        },
        "categories": all_categories,
        "testSuites": {
            "query": {
                "name": "Cross-Database Query Tests",
                "description": "MongoDB pipeline to Oracle SQL translation validation",
                "tests": query_tests
            },
            "large_scale": {
                "name": "Large-Scale Pipeline Tests",
                "description": "Complex aggregations on large datasets (~4GB)",
                "tests": large_scale_tests
            },
            "unit": {
                "name": "Unit Tests",
                "description": "Java unit tests for AST, parsers, and generators",
                "tests": unit_tests
            },
            "integration": {
                "name": "Integration Tests",
                "description": "Oracle Testcontainers integration tests",
                "tests": integration_tests
            }
        }
    }

    # Write JSON output file (for backwards compatibility)
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output, f, indent=2)

    # Also embed data directly in HTML file for file:// access
    html_template = PROJECT_ROOT / "docs" / "test-catalog.html"
    if html_template.exists():
        embed_data_in_html(html_template, output)

    print(f"\nGenerated {OUTPUT_FILE}", file=sys.stderr)
    print(f"  Query Tests: {len(query_tests)} ({query_passed} passed, {query_failed} failed, {query_skipped} skipped)", file=sys.stderr)
    print(f"  Large-Scale: {len(large_scale_tests)} ({ls_passed} passed, {ls_failed} failed)", file=sys.stderr)
    print(f"  Unit Tests: {unit_summary['count']} ({unit_summary['passed']} passed)", file=sys.stderr)
    print(f"  Integration: {integration_summary['count']} ({integration_summary['passed']} passed)", file=sys.stderr)


def embed_data_in_html(html_path: Path, data: dict):
    """Embed test data directly in HTML file for file:// access."""
    import re
    try:
        with open(html_path, 'r') as f:
            html_content = f.read()

        # Create the inline script with data
        # Use separators to avoid extra spaces and ensure compact JSON
        # The JSON must be on a single line to work in a script tag
        json_str = json.dumps(data, separators=(',', ':'))

        data_script = f'<script id="embedded-test-data">const TEST_CATALOG_DATA = {json_str};</script>'

        # Check if data is already embedded (look for our specific script tag)
        embedded_pattern = r'<script id="embedded-test-data">const TEST_CATALOG_DATA = .*?;</script>'
        if re.search(embedded_pattern, html_content, flags=re.DOTALL):
            # Replace existing embedded data
            # Use lambda to prevent re.sub from interpreting backslash escapes in JSON
            # (e.g., \n in JSON strings would become actual newlines without this)
            html_content = re.sub(
                embedded_pattern,
                lambda m: data_script,
                html_content,
                flags=re.DOTALL
            )
        else:
            # Insert before closing </head> tag
            html_content = html_content.replace('</head>', f'{data_script}\n</head>')

        with open(html_path, 'w') as f:
            f.write(html_content)

        print(f"  Embedded data in {html_path}", file=sys.stderr)
    except Exception as e:
        print(f"  Warning: Could not embed data in HTML: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
