#!/usr/bin/env node
/**
 * Export Test Query Results
 *
 * Runs all test queries against MongoDB and exports results to individual files
 * for physical validation.
 *
 * Usage:
 *   node export-results.js [--output-dir <dir>]
 */

const { MongoClient } = require('mongodb');
const fs = require('fs');
const path = require('path');

// Configuration
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://admin:admin123@localhost:27017';
const MONGODB_DB = process.env.MONGODB_DB || 'testdb';

// Parse arguments
const args = process.argv.slice(2);
let outputDir = path.join(__dirname, '../output');

for (let i = 0; i < args.length; i++) {
    if (args[i] === '--output-dir' && args[i + 1]) {
        outputDir = args[i + 1];
        i++;
    }
}

// Ensure output directory exists
if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
}

/**
 * Load test cases from JSON file
 */
function loadTestCases() {
    const testCasesPath = path.join(__dirname, '../tests/test-cases.json');
    const curatedTestsPath = path.join(__dirname, '../import/curated-tests.json');

    const testCases = [];

    // Load main test cases
    if (fs.existsSync(testCasesPath)) {
        const data = JSON.parse(fs.readFileSync(testCasesPath, 'utf8'));
        const cases = data.test_cases || data.tests || [];
        // Normalize format
        for (const tc of cases) {
            testCases.push({
                id: tc.id,
                name: tc.name,
                description: tc.description,
                collection: tc.collection,
                pipeline: tc.mongodb_pipeline || tc.pipeline
            });
        }
    }

    // Load curated tests
    if (fs.existsSync(curatedTestsPath)) {
        const data = JSON.parse(fs.readFileSync(curatedTestsPath, 'utf8'));
        const cases = data.test_cases || data.tests || [];
        // Normalize format
        for (const tc of cases) {
            testCases.push({
                id: tc.id,
                name: tc.name,
                description: tc.description,
                collection: tc.collection,
                pipeline: tc.mongodb_pipeline || tc.pipeline
            });
        }
    }

    return testCases;
}

/**
 * Preprocess pipeline to convert $date objects to Date instances
 */
function preprocessPipeline(obj) {
    if (obj === null || obj === undefined) {
        return obj;
    }

    if (Array.isArray(obj)) {
        return obj.map(item => preprocessPipeline(item));
    }

    if (typeof obj === 'object') {
        if (obj.$date) {
            return new Date(obj.$date);
        }

        const result = {};
        for (const [key, value] of Object.entries(obj)) {
            result[key] = preprocessPipeline(value);
        }
        return result;
    }

    return obj;
}

/**
 * Format result for output
 */
function formatResult(result) {
    return JSON.stringify(result, (key, value) => {
        if (value instanceof Date) {
            return { $date: value.toISOString() };
        }
        return value;
    }, 2);
}

/**
 * Run a single test and export results
 */
async function runTest(db, test, outputDir) {
    const collection = db.collection(test.collection);
    const pipeline = preprocessPipeline(test.pipeline);

    try {
        const cursor = collection.aggregate(pipeline, { allowDiskUse: true });
        const results = await cursor.toArray();

        // Create output object with metadata
        const output = {
            testId: test.id,
            testName: test.name,
            description: test.description,
            collection: test.collection,
            pipeline: test.pipeline,
            resultCount: results.length,
            executedAt: new Date().toISOString(),
            results: results
        };

        // Write to file
        const filename = `${test.id}.json`;
        const filepath = path.join(outputDir, filename);
        fs.writeFileSync(filepath, formatResult(output));

        return { success: true, count: results.length };
    } catch (err) {
        // Write error to file
        const output = {
            testId: test.id,
            testName: test.name,
            description: test.description,
            collection: test.collection,
            pipeline: test.pipeline,
            error: err.message,
            executedAt: new Date().toISOString()
        };

        const filename = `${test.id}.error.json`;
        const filepath = path.join(outputDir, filename);
        fs.writeFileSync(filepath, formatResult(output));

        return { success: false, error: err.message };
    }
}

/**
 * Main entry point
 */
async function main() {
    console.log('==============================================');
    console.log('Export Test Query Results');
    console.log('==============================================');
    console.log(`Output directory: ${outputDir}`);
    console.log(`MongoDB: ${MONGODB_URI}`);
    console.log(`Database: ${MONGODB_DB}`);
    console.log('');

    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();
        const db = client.db(MONGODB_DB);

        // Load test cases
        const testCases = loadTestCases();
        console.log(`Loaded ${testCases.length} test cases\n`);

        // Summary tracking
        const summary = {
            total: testCases.length,
            success: 0,
            failed: 0,
            results: []
        };

        // Run each test
        for (const test of testCases) {
            process.stdout.write(`Running ${test.id}: ${test.name}... `);

            const result = await runTest(db, test, outputDir);

            if (result.success) {
                console.log(`OK (${result.count} results)`);
                summary.success++;
            } else {
                console.log(`FAILED: ${result.error}`);
                summary.failed++;
            }

            summary.results.push({
                id: test.id,
                name: test.name,
                success: result.success,
                count: result.count,
                error: result.error
            });
        }

        // Write summary file
        const summaryPath = path.join(outputDir, '_summary.json');
        fs.writeFileSync(summaryPath, JSON.stringify(summary, null, 2));

        // Print summary
        console.log('\n==============================================');
        console.log('SUMMARY');
        console.log('==============================================');
        console.log(`Total:   ${summary.total}`);
        console.log(`Success: ${summary.success}`);
        console.log(`Failed:  ${summary.failed}`);
        console.log(`\nResults exported to: ${outputDir}`);
        console.log(`Summary file: ${summaryPath}`);

    } finally {
        await client.close();
    }
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});
