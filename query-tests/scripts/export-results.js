#!/usr/bin/env node
/**
 * Export Test Query Results - MongoDB and Oracle
 *
 * Runs all test queries against both MongoDB and Oracle databases,
 * exports results to individual files for physical validation.
 *
 * Usage:
 *   node export-results.js [--output-dir <dir>] [--mongodb-only] [--oracle-only]
 */

const { MongoClient } = require('mongodb');
const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Configuration
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://admin:admin123@localhost:27017';
const MONGODB_DB = process.env.MONGODB_DB || 'testdb';

// Parse arguments
const args = process.argv.slice(2);
let outputDir = path.join(__dirname, '../output');
let mongodbOnly = false;
let oracleOnly = false;
let includeLargeScale = false;

for (let i = 0; i < args.length; i++) {
    if (args[i] === '--output-dir' && args[i + 1]) {
        outputDir = args[i + 1];
        i++;
    } else if (args[i] === '--mongodb-only') {
        mongodbOnly = true;
    } else if (args[i] === '--oracle-only') {
        oracleOnly = true;
    } else if (args[i] === '--include-large-scale' || args[i] === '--large-scale') {
        includeLargeScale = true;
    }
}

// Ensure output directory exists
if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
}

/**
 * Load test cases from JSON file
 */
function loadTestCases(includeLargeScale = false) {
    const testCasesPath = path.join(__dirname, '../tests/test-cases.json');
    const curatedTestsPath = path.join(__dirname, '../import/curated-tests.json');
    const largeScalePath = path.join(__dirname, '../large-scale/complex-pipelines.json');

    const testCases = [];

    // Load main test cases
    if (fs.existsSync(testCasesPath)) {
        const data = JSON.parse(fs.readFileSync(testCasesPath, 'utf8'));
        const cases = data.test_cases || data.tests || [];
        for (const tc of cases) {
            testCases.push({
                id: tc.id,
                name: tc.name,
                description: tc.description,
                collection: tc.collection,
                pipeline: tc.mongodb_pipeline || tc.pipeline,
                oracleSql: tc.oracle_sql,
                database: 'testdb'
            });
        }
    }

    // Load curated tests
    if (fs.existsSync(curatedTestsPath)) {
        const data = JSON.parse(fs.readFileSync(curatedTestsPath, 'utf8'));
        const cases = data.test_cases || data.tests || [];
        for (const tc of cases) {
            testCases.push({
                id: tc.id,
                name: tc.name,
                description: tc.description,
                collection: tc.collection,
                pipeline: tc.mongodb_pipeline || tc.pipeline,
                oracleSql: tc.oracle_sql,
                database: 'testdb'
            });
        }
    }

    // Load large-scale tests
    if (includeLargeScale && fs.existsSync(largeScalePath)) {
        const data = JSON.parse(fs.readFileSync(largeScalePath, 'utf8'));
        const pipelines = data.pipelines || [];
        for (const p of pipelines) {
            testCases.push({
                id: p.id,
                name: p.name,
                description: p.description,
                collection: p.collection,
                pipeline: p.pipeline,
                oracleSql: null, // Large-scale tests don't have pre-written Oracle SQL
                database: 'largescale_test'
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
 * Run Oracle query via docker exec
 */
function runOracleQuery(sql) {
    if (!sql) {
        return { success: false, error: 'No Oracle SQL provided', results: [] };
    }

    try {
        const sqlCommand = `
SET PAGESIZE 0
SET LINESIZE 32767
SET TRIMSPOOL ON
SET TRIMOUT ON
SET FEEDBACK OFF
SET HEADING OFF
SET LONG 1000000
SET LONGCHUNKSIZE 1000000
${sql};
EXIT;
`;
        const result = execSync(
            `docker exec mongo-translator-oracle bash -c "echo '${sqlCommand.replace(/'/g, "'\\''")}' | sqlplus -s translator/translator123@//localhost:1521/FREEPDB1"`,
            { encoding: 'utf8', maxBuffer: 50 * 1024 * 1024 }
        );

        // Parse results - each non-empty line is a result row
        const lines = result.split('\n').filter(line => line.trim() !== '');

        // Try to parse as JSON if it looks like JSON
        const results = lines.map(line => {
            const trimmed = line.trim();
            if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
                try {
                    return JSON.parse(trimmed);
                } catch {
                    return { raw: trimmed };
                }
            }
            return { raw: trimmed };
        });

        return {
            success: true,
            results: results,
            count: results.length,
            raw: result.trim()
        };
    } catch (err) {
        return {
            success: false,
            error: err.message,
            results: [],
            count: 0
        };
    }
}

/**
 * Run MongoDB query
 */
async function runMongoQuery(db, collection, pipeline) {
    try {
        const coll = db.collection(collection);
        const processedPipeline = preprocessPipeline(pipeline);
        const cursor = coll.aggregate(processedPipeline, { allowDiskUse: true });
        const results = await cursor.toArray();

        return {
            success: true,
            results: results,
            count: results.length
        };
    } catch (err) {
        return {
            success: false,
            error: err.message,
            results: [],
            count: 0
        };
    }
}

/**
 * Run a single test and export results
 */
async function runTest(db, test, outputDir, options) {
    const output = {
        testId: test.id,
        testName: test.name,
        description: test.description,
        collection: test.collection,
        pipeline: test.pipeline,
        oracleSql: test.oracleSql,
        executedAt: new Date().toISOString(),
        mongodb: null,
        oracle: null,
        comparison: null
    };

    // Run MongoDB query
    if (!options.oracleOnly) {
        output.mongodb = await runMongoQuery(db, test.collection, test.pipeline);
    }

    // Run Oracle query
    if (!options.mongodbOnly && test.oracleSql) {
        output.oracle = runOracleQuery(test.oracleSql);
    }

    // Compare results if both ran
    if (output.mongodb && output.oracle) {
        output.comparison = {
            mongodbCount: output.mongodb.count,
            oracleCount: output.oracle.count,
            countsMatch: output.mongodb.count === output.oracle.count,
            bothSucceeded: output.mongodb.success && output.oracle.success
        };
    }

    // Write to file
    const filename = `${test.id}.json`;
    const filepath = path.join(outputDir, filename);
    fs.writeFileSync(filepath, formatResult(output));

    return {
        success: (output.mongodb?.success !== false) && (output.oracle?.success !== false),
        mongoCount: output.mongodb?.count,
        oracleCount: output.oracle?.count,
        countsMatch: output.comparison?.countsMatch,
        error: output.mongodb?.error || output.oracle?.error
    };
}

/**
 * Main entry point
 */
async function main() {
    console.log('==============================================');
    console.log('Export Test Query Results (MongoDB + Oracle)');
    console.log('==============================================');
    console.log(`Output directory: ${outputDir}`);
    console.log(`MongoDB: ${MONGODB_URI}`);
    if (mongodbOnly) console.log('Mode: MongoDB only');
    if (oracleOnly) console.log('Mode: Oracle only');
    if (includeLargeScale) console.log('Including large-scale tests');
    console.log('');

    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();

        // Load test cases
        const testCases = loadTestCases(includeLargeScale);
        console.log(`Loaded ${testCases.length} test cases\n`);

        // Summary tracking
        const summary = {
            total: testCases.length,
            success: 0,
            failed: 0,
            countsMatch: 0,
            countsMismatch: 0,
            results: []
        };

        // Track current database to minimize switching
        let currentDbName = null;
        let db = null;

        // Run each test
        for (const test of testCases) {
            // Switch database if needed
            const testDb = test.database || MONGODB_DB;
            if (testDb !== currentDbName) {
                db = client.db(testDb);
                currentDbName = testDb;
                console.log(`\n--- Database: ${testDb} ---\n`);
            }

            process.stdout.write(`Running ${test.id}: ${test.name}... `);

            const result = await runTest(db, test, outputDir, { mongodbOnly, oracleOnly });

            if (result.success) {
                summary.success++;
                if (result.countsMatch === true) {
                    summary.countsMatch++;
                    console.log(`OK (MongoDB: ${result.mongoCount}, Oracle: ${result.oracleCount}) ✓`);
                } else if (result.countsMatch === false) {
                    summary.countsMismatch++;
                    console.log(`MISMATCH (MongoDB: ${result.mongoCount}, Oracle: ${result.oracleCount}) ✗`);
                } else {
                    console.log(`OK (${result.mongoCount ?? result.oracleCount} results)`);
                }
            } else {
                console.log(`FAILED: ${result.error}`);
                summary.failed++;
            }

            summary.results.push({
                id: test.id,
                name: test.name,
                database: test.database,
                success: result.success,
                mongoCount: result.mongoCount,
                oracleCount: result.oracleCount,
                countsMatch: result.countsMatch,
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
        console.log(`Total:          ${summary.total}`);
        console.log(`Success:        ${summary.success}`);
        console.log(`Failed:         ${summary.failed}`);
        if (!mongodbOnly && !oracleOnly) {
            console.log(`Counts Match:   ${summary.countsMatch}`);
            console.log(`Counts Differ:  ${summary.countsMismatch}`);
        }
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
