#!/bin/bash
#
# Large-Scale Cross-Database Comparison Test Suite
#
# This script generates test data, loads it into both MongoDB and Oracle,
# executes complex aggregation pipelines on both databases, and compares results.
#
# Usage:
#   ./run-comparison.sh [--size small|medium|large|xlarge] [--skip-generate] [--skip-load] [--skip-existing]
#
# Options:
#   --size           Data size (default: small for testing)
#   --skip-generate  Skip data generation (use existing data)
#   --skip-load      Skip data loading (use existing loaded data)
#   --skip-existing  Skip loading collections that already have full data (drops incomplete ones)
#   --mongodb-only   Only run MongoDB tests
#   --oracle-only    Only run Oracle tests
#

set -e

# Default options
SIZE="small"
SKIP_GENERATE=false
SKIP_LOAD=false
SKIP_EXISTING=false
MONGODB_ONLY=false
ORACLE_ONLY=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --size)
            SIZE="$2"
            shift 2
            ;;
        --skip-generate)
            SKIP_GENERATE=true
            shift
            ;;
        --skip-load)
            SKIP_LOAD=true
            shift
            ;;
        --skip-existing)
            SKIP_EXISTING=true
            shift
            ;;
        --mongodb-only)
            MONGODB_ONLY=true
            shift
            ;;
        --oracle-only)
            ORACLE_ONLY=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"
RESULTS_DIR="$SCRIPT_DIR/results"

# Create results directory
mkdir -p "$RESULTS_DIR"

echo "================================================"
echo "Large-Scale Cross-Database Comparison Test"
echo "================================================"
echo ""
echo "Configuration:"
echo "  Size: $SIZE"
echo "  Data directory: $DATA_DIR"
echo "  Results directory: $RESULTS_DIR"
echo "  Skip generate: $SKIP_GENERATE"
echo "  Skip load: $SKIP_LOAD"
echo "  Skip existing: $SKIP_EXISTING"
echo ""

# Check for Node.js
if ! command -v node &> /dev/null; then
    echo "Error: Node.js is required but not installed."
    exit 1
fi

# Check for required npm packages
check_npm_package() {
    local package=$1
    if ! node -e "require('$package')" 2>/dev/null; then
        echo "Installing $package..."
        npm install "$package" --save-dev
    fi
}

echo "Checking dependencies..."
check_npm_package "mongodb"

# Step 1: Generate Data
if [ "$SKIP_GENERATE" = false ]; then
    echo ""
    echo "================================================"
    echo "Step 1: Generating test data"
    echo "================================================"
    echo ""

    node "$SCRIPT_DIR/generate-data.js" --size "$SIZE" --output "$DATA_DIR"

    if [ $? -ne 0 ]; then
        echo "Error: Data generation failed"
        exit 1
    fi
else
    echo ""
    echo "Skipping data generation (--skip-generate)"

    if [ ! -f "$DATA_DIR/manifest.json" ]; then
        echo "Error: No data found in $DATA_DIR. Run without --skip-generate first."
        exit 1
    fi
fi

# Step 2: Load Data
if [ "$SKIP_LOAD" = false ]; then
    echo ""
    echo "================================================"
    echo "Step 2: Loading data into databases"
    echo "================================================"

    TARGET="both"
    if [ "$MONGODB_ONLY" = true ]; then
        TARGET="mongodb"
    elif [ "$ORACLE_ONLY" = true ]; then
        TARGET="oracle"
    fi

    echo ""
    echo "Loading data into: $TARGET"
    echo ""

    LOAD_ARGS="--target $TARGET --data-dir $DATA_DIR"
    if [ "$SKIP_EXISTING" = true ]; then
        LOAD_ARGS="$LOAD_ARGS --skip-existing"
    else
        LOAD_ARGS="$LOAD_ARGS --drop"
    fi

    node "$SCRIPT_DIR/load-data.js" $LOAD_ARGS

    if [ $? -ne 0 ]; then
        echo "Error: Data loading failed"
        exit 1
    fi
else
    echo ""
    echo "Skipping data loading (--skip-load)"
fi

# Step 3: Run Pipeline Comparisons
echo ""
echo "================================================"
echo "Step 3: Running pipeline comparison tests"
echo "================================================"
echo ""

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTPUT_FILE="$RESULTS_DIR/comparison-report-$TIMESTAMP.json"

node "$SCRIPT_DIR/compare-pipelines.js" --output "$OUTPUT_FILE" --verbose

EXIT_CODE=$?

# Step 4: Generate Summary
echo ""
echo "================================================"
echo "Step 4: Test Results Summary"
echo "================================================"
echo ""

if [ -f "$OUTPUT_FILE" ]; then
    # Parse results using node
    node -e "
const fs = require('fs');
const report = JSON.parse(fs.readFileSync('$OUTPUT_FILE', 'utf8'));

console.log('Test Run: ' + report.timestamp);
console.log('');
console.log('Overall Results:');
console.log('  Total:  ' + report.summary.total);
console.log('  Passed: ' + report.summary.passed);
console.log('  Failed: ' + report.summary.failed);
console.log('  Errors: ' + report.summary.errors);
console.log('');
console.log('Performance:');
console.log('  MongoDB total time: ' + report.summary.totalMongoTime + 'ms');
console.log('  Oracle total time:  ' + report.summary.totalOracleTime + 'ms');
console.log('');

if (report.summary.failed > 0 || report.summary.errors > 0) {
    console.log('Failed/Error Pipelines:');
    for (const result of report.results) {
        if (result.status !== 'passed') {
            console.log('  - ' + result.id + ': ' + result.name + ' [' + result.status.toUpperCase() + ']');
        }
    }
}
"
fi

echo ""
echo "Full report saved to: $OUTPUT_FILE"
echo ""

if [ $EXIT_CODE -eq 0 ]; then
    echo "All tests PASSED!"
else
    echo "Some tests FAILED. Check the report for details."
fi

exit $EXIT_CODE
