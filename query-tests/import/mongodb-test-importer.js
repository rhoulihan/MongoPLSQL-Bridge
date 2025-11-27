#!/usr/bin/env node
/**
 * MongoDB jstest Importer for MongoPLSQL-Bridge
 *
 * This script parses MongoDB aggregation jstests and converts them to our
 * cross-database validation test format.
 *
 * Usage:
 *   node mongodb-test-importer.js <jstest-file-or-directory> [--output <file>]
 *   node mongodb-test-importer.js --fetch <operator> [--output <file>]
 *
 * Examples:
 *   node mongodb-test-importer.js ./jstests/concat.js
 *   node mongodb-test-importer.js --fetch concat,size,cond
 *   node mongodb-test-importer.js ./jstests/ --output imported-tests.json
 */

const fs = require('fs');
const path = require('path');
const https = require('https');

// MongoDB jstest base URL
const MONGODB_JSTESTS_BASE = 'https://raw.githubusercontent.com/mongodb/mongo/master/jstests/aggregation';

// Operator to file path mapping
const OPERATOR_PATHS = {
    // Expression operators
    'concat': '/expressions/concat.js',
    'toLower': '/expressions/upperlower.js',
    'toUpper': '/expressions/upperlower.js',
    'substr': '/expressions/substr.js',
    'substrBytes': '/expressions/substr.js',
    'substrCP': '/expressions/substr.js',
    'strLenCP': '/expressions/strlen.js',
    'strLenBytes': '/expressions/strlen.js',
    'trim': '/expressions/trim.js',
    'ltrim': '/expressions/trim.js',
    'rtrim': '/expressions/trim.js',
    'split': '/expressions/split.js',
    'indexOfBytes': '/expressions/indexof_bytes.js',
    'indexOfCP': '/expressions/indexof_codepoints.js',

    // Array operators
    'arrayElemAt': '/expressions/array_elem_at.js',
    'size': '/expressions/size.js',
    'first': '/expressions/first_last.js',
    'last': '/expressions/first_last.js',
    'slice': '/expressions/slice.js',
    'concatArrays': '/expressions/concat_arrays.js',
    'filter': '/expressions/filter.js',
    'map': '/expressions/map.js',
    'reduce': '/expressions/reduce.js',
    'in': '/expressions/in.js',
    'reverseArray': '/expressions/reverseArray.js',

    // Date operators
    'year': '/expressions/day_of_expressions.js',
    'month': '/expressions/day_of_expressions.js',
    'dayOfMonth': '/expressions/day_of_expressions.js',
    'hour': '/expressions/day_of_expressions.js',
    'minute': '/expressions/day_of_expressions.js',
    'second': '/expressions/day_of_expressions.js',
    'dayOfWeek': '/expressions/day_of_expressions.js',
    'dayOfYear': '/expressions/day_of_expressions.js',
    'dateFromString': '/expressions/date_from_string.js',
    'dateToString': '/expressions/date_to_string.js',
    'dateTrunc': '/expressions/date_trunc.js',

    // Conditional operators
    'cond': '/expressions/cond.js',
    'ifNull': '/expressions/ifnull.js',
    'switch': '/expressions/switch.js',

    // Arithmetic operators
    'add': '/expressions/add.js',
    'subtract': '/expressions/subtract.js',
    'multiply': '/expressions/multiply.js',
    'divide': '/expressions/divide.js',
    'mod': '/expressions/mod.js',
    'abs': '/expressions/abs.js',
    'ceil': '/expressions/ceil.js',
    'floor': '/expressions/floor.js',
    'round': '/expressions/round.js',

    // Comparison operators
    'cmp': '/expressions/cmp_literal.js',
    'eq': '/expressions/cmp_literal.js',
    'ne': '/expressions/ne_constant.js',
    'gt': '/expressions/cmp_literal.js',
    'gte': '/expressions/cmp_literal.js',
    'lt': '/expressions/cmp_literal.js',
    'lte': '/expressions/cmp_literal.js',

    // Accumulators
    'firstAcc': '/accumulators/first_last.js',
    'lastAcc': '/accumulators/first_last.js',
    'firstN': '/accumulators/first_n_last_n.js',
    'lastN': '/accumulators/first_n_last_n.js',
    'minN': '/accumulators/min_n_max_n.js',
    'maxN': '/accumulators/min_n_max_n.js',
    'topN': '/accumulators/top_bottom_top_n_bottom_n.js',
    'bottomN': '/accumulators/top_bottom_top_n_bottom_n.js',

    // Stage operators
    'lookup': '/sources/lookup/lookup_equijoin_semantics_lib.js',
    'unwind': '/sources/unwind/unwind.js',
    'group': '/sources/group/group.js',
    'match': '/sources/match/match.js',
    'project': '/sources/project/project.js',
    'sort': '/sources/sort/sort.js',
    'addFields': '/sources/addFields/addFields.js',
};

// Supported operators that we can translate to Oracle
const SUPPORTED_OPERATORS = new Set([
    // String
    '$concat', '$toLower', '$toUpper', '$substr', '$substrCP', '$trim', '$ltrim', '$rtrim', '$strLenCP',
    // Array
    '$arrayElemAt', '$size', '$first', '$last',
    // Date
    '$year', '$month', '$dayOfMonth', '$hour', '$minute', '$second', '$dayOfWeek', '$dayOfYear',
    // Conditional
    '$cond', '$ifNull',
    // Arithmetic
    '$add', '$subtract', '$multiply', '$divide', '$mod',
    // Comparison
    '$eq', '$ne', '$gt', '$gte', '$lt', '$lte', '$in', '$nin',
    // Logical
    '$and', '$or', '$not', '$nor',
    // Accumulators
    '$sum', '$avg', '$min', '$max', '$count', '$first', '$last', '$push', '$addToSet',
    // Stages
    '$match', '$group', '$project', '$sort', '$limit', '$skip', '$lookup', '$unwind', '$addFields', '$set'
]);

/**
 * Fetches a file from the MongoDB repository
 */
function fetchFile(urlPath) {
    return new Promise((resolve, reject) => {
        const url = MONGODB_JSTESTS_BASE + urlPath;
        console.error(`Fetching: ${url}`);

        https.get(url, (res) => {
            if (res.statusCode === 404) {
                reject(new Error(`File not found: ${url}`));
                return;
            }
            if (res.statusCode !== 200) {
                reject(new Error(`HTTP ${res.statusCode}: ${url}`));
                return;
            }

            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => resolve(data));
        }).on('error', reject);
    });
}

/**
 * Extracts test patterns from MongoDB jstest JavaScript code
 */
class JsTestParser {
    constructor(content, sourceFile) {
        this.content = content;
        this.sourceFile = sourceFile;
        this.testCases = [];
        this.collectionName = 'test_collection';
        this.testData = [];
    }

    parse() {
        // Extract collection name
        this.extractCollectionName();

        // Extract test data (insertMany/insertOne calls)
        this.extractTestData();

        // Extract aggregation pipelines and assertions
        this.extractAggregations();

        // Extract error test cases
        this.extractErrorCases();

        return {
            sourceFile: this.sourceFile,
            collectionName: this.collectionName,
            testData: this.testData,
            testCases: this.testCases
        };
    }

    extractCollectionName() {
        // Match patterns like: coll = db.concat, db.getCollection("test"), etc.
        const patterns = [
            /(?:const|let|var)?\s*(?:coll|collection|c)\s*=\s*db\.(\w+)/,
            /db\.getCollection\s*\(\s*["'](\w+)["']\s*\)/,
            /db\.(\w+)\.drop\s*\(\)/,
            /db\.(\w+)\.insertMany/,
            /db\.(\w+)\.insert/,
        ];

        for (const pattern of patterns) {
            const match = this.content.match(pattern);
            if (match) {
                this.collectionName = match[1];
                break;
            }
        }
    }

    extractTestData() {
        // Match insertMany calls
        const insertManyPattern = /\.insertMany\s*\(\s*(\[[\s\S]*?\])\s*\)/g;
        let match;
        while ((match = insertManyPattern.exec(this.content)) !== null) {
            try {
                const docs = this.parseJsArray(match[1]);
                this.testData.push(...docs);
            } catch (e) {
                console.error(`Failed to parse insertMany: ${e.message}`);
            }
        }

        // Match insertOne/insert calls
        const insertOnePattern = /\.insert(?:One)?\s*\(\s*(\{[\s\S]*?\})\s*\)/g;
        while ((match = insertOnePattern.exec(this.content)) !== null) {
            try {
                const doc = this.parseJsObject(match[1]);
                this.testData.push(doc);
            } catch (e) {
                console.error(`Failed to parse insert: ${e.message}`);
            }
        }
    }

    extractAggregations() {
        // Match aggregate calls with their expected results
        const patterns = [
            // assert.eq(coll.aggregate([...]).toArray(), [...])
            /assert\.eq\s*\(\s*(?:\w+\.)?aggregate\s*\(\s*(\[[\s\S]*?\])\s*\)\.toArray\s*\(\s*\)\s*,\s*(\[[\s\S]*?\])\s*\)/g,
            // assertResult(expected, pipeline) - various forms
            /assertResult\s*\(\s*(\{[\s\S]*?\}|\[[\s\S]*?\])\s*,\s*(\[[\s\S]*?\])\s*\)/g,
            /assertResult\s*\(\s*["']([^"']+)["']\s*,\s*(\[[\s\S]*?\])\s*\)/g,
            // testOp patterns for expression tests
            /testOp\s*\(\s*(\{[^}]+\})\s*,\s*([^,]+)\s*\)/g,
            // assertExpression patterns
            /assertExpression\s*\(\s*(\{[\s\S]*?\})\s*,\s*([^)]+)\s*\)/g,
            // let result = coll.aggregate([...]).toArray()
            /(?:let|const|var)\s+\w+\s*=\s*(?:\w+\.)?aggregate\s*\(\s*(\[[\s\S]*?\])\s*\)/g,
            // Direct aggregate calls
            /(?:coll|collection|c|db\.\w+)\.aggregate\s*\(\s*(\[[\s\S]*?\])\s*\)/g,
        ];

        let testIndex = 1;

        for (const pattern of patterns) {
            let match;
            while ((match = pattern.exec(this.content)) !== null) {
                try {
                    let pipeline, expected;

                    if (match.length === 3) {
                        // Pattern with expected result
                        if (pattern.source.includes('assertResult')) {
                            expected = match[1];
                            pipeline = match[2];
                        } else {
                            pipeline = match[1];
                            expected = match[2];
                        }
                    } else {
                        pipeline = match[1];
                    }

                    const parsedPipeline = this.parseJsArray(pipeline);
                    if (this.isSupported(parsedPipeline)) {
                        const testCase = {
                            id: `IMPORT_${path.basename(this.sourceFile, '.js').toUpperCase()}_${String(testIndex).padStart(3, '0')}`,
                            name: this.generateTestName(parsedPipeline),
                            category: this.detectCategory(parsedPipeline),
                            operator: this.detectOperator(parsedPipeline),
                            description: `Imported from MongoDB jstest: ${this.sourceFile}`,
                            collection: this.collectionName,
                            mongodb_pipeline: parsedPipeline,
                            source_line: this.getLineNumber(match.index),
                        };

                        if (expected) {
                            try {
                                testCase.expected_result = this.parseJsArray(expected);
                            } catch (e) {
                                // Expected might be a single object
                                try {
                                    testCase.expected_result = [this.parseJsObject(expected)];
                                } catch (e2) {
                                    // Skip if we can't parse expected
                                }
                            }
                        }

                        this.testCases.push(testCase);
                        testIndex++;
                    }
                } catch (e) {
                    console.error(`Failed to parse aggregation at index ${match.index}: ${e.message}`);
                }
            }
        }
    }

    extractErrorCases() {
        // Match assertError/assertErrorCode patterns
        const errorPatterns = [
            /assertError(?:Code)?\s*\(\s*(\d+)\s*,\s*(\[[\s\S]*?\])\s*\)/g,
            /assert\.commandFailedWithCode\s*\([^,]+,\s*(\[[\s\S]*?\])\s*,\s*(\d+)\s*\)/g,
        ];

        for (const pattern of errorPatterns) {
            let match;
            while ((match = pattern.exec(this.content)) !== null) {
                try {
                    const errorCode = match[1];
                    const pipeline = match[2];
                    const parsedPipeline = this.parseJsArray(pipeline);

                    // Store error cases separately for reference
                    this.testCases.push({
                        id: `IMPORT_ERROR_${path.basename(this.sourceFile, '.js').toUpperCase()}_${errorCode}`,
                        name: `Error case - code ${errorCode}`,
                        category: 'error',
                        operator: this.detectOperator(parsedPipeline),
                        description: `Error test from MongoDB jstest: ${this.sourceFile}`,
                        collection: this.collectionName,
                        mongodb_pipeline: parsedPipeline,
                        expected_error_code: parseInt(errorCode),
                        source_line: this.getLineNumber(match.index),
                    });
                } catch (e) {
                    // Skip unparseable error cases
                }
            }
        }
    }

    /**
     * Parse JavaScript array/object syntax to JSON
     */
    parseJsArray(jsCode) {
        return this.parseJsValue(jsCode.trim());
    }

    parseJsObject(jsCode) {
        return this.parseJsValue(jsCode.trim());
    }

    parseJsValue(code) {
        // Clean up the code for JSON parsing
        let cleaned = code
            // Remove comments
            .replace(/\/\/.*$/gm, '')
            .replace(/\/\*[\s\S]*?\*\//g, '')
            // Handle MongoDB extended JSON types
            .replace(/NumberInt\s*\(\s*(\d+)\s*\)/g, '$1')
            .replace(/NumberLong\s*\(\s*["']?(\d+)["']?\s*\)/g, '$1')
            .replace(/NumberDecimal\s*\(\s*["']([^"']+)["']\s*\)/g, '$1')
            .replace(/ObjectId\s*\(\s*["']([^"']+)["']\s*\)/g, '{"$oid": "$1"}')
            .replace(/ISODate\s*\(\s*["']([^"']+)["']\s*\)/g, '{"$date": "$1"}')
            .replace(/new Date\s*\(\s*["']([^"']+)["']\s*\)/g, '{"$date": "$1"}')
            .replace(/new Date\s*\(\s*\)/g, '{"$date": "2024-01-01T00:00:00.000Z"}')
            .replace(/Timestamp\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)/g, '{"$timestamp": {"t": $1, "i": $2}}')
            .replace(/BinData\s*\(\s*(\d+)\s*,\s*["']([^"']+)["']\s*\)/g, '{"$binary": {"base64": "$2", "subType": "$1"}}')
            .replace(/UUID\s*\(\s*["']([^"']+)["']\s*\)/g, '{"$uuid": "$1"}')
            .replace(/MinKey\s*\(\s*\)/g, '{"$minKey": 1}')
            .replace(/MaxKey\s*\(\s*\)/g, '{"$maxKey": 1}')
            .replace(/\/([^\/]+)\/([gimsuy]*)/g, '{"$regex": "$1", "$options": "$2"}')
            // Handle unquoted keys
            .replace(/([{,]\s*)([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:/g, '$1"$2":')
            // Handle single quotes
            .replace(/'/g, '"')
            // Handle trailing commas
            .replace(/,(\s*[}\]])/g, '$1')
            // Handle undefined/null
            .replace(/:\s*undefined/g, ': null');

        try {
            return JSON.parse(cleaned);
        } catch (e) {
            // Try a more aggressive cleanup
            cleaned = cleaned
                .replace(/\n/g, ' ')
                .replace(/\s+/g, ' ')
                .trim();
            return JSON.parse(cleaned);
        }
    }

    /**
     * Check if pipeline uses only supported operators
     */
    isSupported(pipeline) {
        const operators = this.findOperators(pipeline);
        return operators.every(op => SUPPORTED_OPERATORS.has(op));
    }

    /**
     * Find all operators used in a pipeline
     */
    findOperators(obj) {
        const operators = new Set();

        const traverse = (value) => {
            if (Array.isArray(value)) {
                value.forEach(traverse);
            } else if (value && typeof value === 'object') {
                for (const key of Object.keys(value)) {
                    if (key.startsWith('$')) {
                        operators.add(key);
                    }
                    traverse(value[key]);
                }
            }
        };

        traverse(obj);
        return Array.from(operators);
    }

    /**
     * Generate a descriptive test name from pipeline
     */
    generateTestName(pipeline) {
        const operators = this.findOperators(pipeline);
        const stageOps = operators.filter(op =>
            ['$match', '$group', '$project', '$sort', '$limit', '$skip', '$lookup', '$unwind', '$addFields', '$set'].includes(op)
        );
        const exprOps = operators.filter(op => !stageOps.includes(op));

        let name = '';
        if (stageOps.length > 0) {
            name = stageOps.join(' + ');
        }
        if (exprOps.length > 0) {
            name += (name ? ' with ' : '') + exprOps.slice(0, 3).join(', ');
            if (exprOps.length > 3) name += '...';
        }

        return name || 'Aggregation test';
    }

    /**
     * Detect test category from pipeline
     */
    detectCategory(pipeline) {
        const operators = this.findOperators(pipeline);

        if (operators.some(op => ['$concat', '$toLower', '$toUpper', '$substr', '$trim', '$strLenCP'].includes(op))) {
            return 'string';
        }
        if (operators.some(op => ['$year', '$month', '$dayOfMonth', '$hour', '$minute', '$second'].includes(op))) {
            return 'date';
        }
        if (operators.some(op => ['$arrayElemAt', '$size', '$first', '$last'].includes(op))) {
            return 'array';
        }
        if (operators.some(op => ['$add', '$subtract', '$multiply', '$divide', '$mod'].includes(op))) {
            return 'arithmetic';
        }
        if (operators.some(op => ['$cond', '$ifNull', '$switch'].includes(op))) {
            return 'conditional';
        }
        if (operators.some(op => ['$lookup'].includes(op))) {
            return 'lookup';
        }
        if (operators.some(op => ['$unwind'].includes(op))) {
            return 'unwind';
        }
        if (operators.some(op => ['$group'].includes(op))) {
            return 'accumulator';
        }

        return 'imported';
    }

    /**
     * Detect primary operator from pipeline
     */
    detectOperator(pipeline) {
        const operators = this.findOperators(pipeline);
        // Return non-stage operators first, then stage operators
        const exprOps = operators.filter(op =>
            !['$match', '$group', '$project', '$sort', '$limit', '$skip', '$lookup', '$unwind', '$addFields', '$set'].includes(op)
        );

        return exprOps.length > 0 ? exprOps[0] : operators[0] || 'unknown';
    }

    /**
     * Get line number for a character index
     */
    getLineNumber(index) {
        return this.content.substring(0, index).split('\n').length;
    }
}

/**
 * Convert parsed test cases to our test format
 */
function convertToTestFormat(parsed, idPrefix = 'IMP') {
    const testCases = [];
    let index = 1;

    for (const tc of parsed.testCases) {
        // Skip error cases for now (we can't validate those against Oracle)
        if (tc.category === 'error') continue;

        // Skip if no valid pipeline
        if (!tc.mongodb_pipeline || tc.mongodb_pipeline.length === 0) continue;

        const testCase = {
            id: `${idPrefix}${String(index).padStart(3, '0')}`,
            name: tc.name,
            category: tc.category,
            operator: tc.operator,
            description: tc.description,
            collection: parsed.collectionName,
            mongodb_pipeline: tc.mongodb_pipeline,
            oracle_sql: null,  // To be generated by the translator
            expected_count: tc.expected_result ? tc.expected_result.length : null,
            sort_by: detectSortField(tc.mongodb_pipeline),
            source: {
                file: parsed.sourceFile,
                line: tc.source_line
            }
        };

        if (tc.expected_result) {
            testCase.expected_result = tc.expected_result;
        }

        testCases.push(testCase);
        index++;
    }

    return {
        collection: parsed.collectionName,
        test_data: parsed.testData,
        test_cases: testCases
    };
}

/**
 * Detect sort field from pipeline
 */
function detectSortField(pipeline) {
    for (const stage of pipeline) {
        if (stage.$sort) {
            const fields = Object.keys(stage.$sort);
            if (fields.length > 0) return fields[0];
        }
    }
    return null;
}

/**
 * Main entry point
 */
async function main() {
    const args = process.argv.slice(2);

    if (args.length === 0 || args.includes('--help') || args.includes('-h')) {
        console.log(`
MongoDB jstest Importer for MongoPLSQL-Bridge

Usage:
  node mongodb-test-importer.js <jstest-file> [--output <file>]
  node mongodb-test-importer.js --fetch <operators> [--output <file>]
  node mongodb-test-importer.js --list-operators

Options:
  --fetch <operators>   Fetch tests from MongoDB repo (comma-separated)
  --output <file>       Output file (default: stdout)
  --list-operators      List available operators to fetch
  --help, -h            Show this help

Examples:
  node mongodb-test-importer.js ./concat.js
  node mongodb-test-importer.js --fetch concat,size,cond
  node mongodb-test-importer.js --fetch arrayElemAt --output array-tests.json
`);
        process.exit(0);
    }

    if (args.includes('--list-operators')) {
        console.log('Available operators to fetch:');
        for (const [op, path] of Object.entries(OPERATOR_PATHS)) {
            console.log(`  ${op.padEnd(20)} -> ${path}`);
        }
        process.exit(0);
    }

    let outputFile = null;
    const outputIndex = args.indexOf('--output');
    if (outputIndex !== -1 && args[outputIndex + 1]) {
        outputFile = args[outputIndex + 1];
    }

    const allResults = {
        version: '1.0',
        description: 'Imported MongoDB aggregation tests',
        imported_at: new Date().toISOString(),
        sources: [],
        test_data: {},
        test_cases: []
    };

    if (args.includes('--fetch')) {
        const fetchIndex = args.indexOf('--fetch');
        const operators = args[fetchIndex + 1].split(',');

        // Get unique file paths
        const filePaths = new Set();
        for (const op of operators) {
            const opPath = OPERATOR_PATHS[op];
            if (opPath) {
                filePaths.add(opPath);
            } else {
                console.error(`Unknown operator: ${op}`);
            }
        }

        for (const filePath of filePaths) {
            try {
                const content = await fetchFile(filePath);
                const parser = new JsTestParser(content, filePath);
                const parsed = parser.parse();
                const converted = convertToTestFormat(parsed,
                    path.basename(filePath, '.js').toUpperCase().replace(/[^A-Z0-9]/g, '').substring(0, 6));

                allResults.sources.push(filePath);
                if (converted.test_data.length > 0) {
                    allResults.test_data[converted.collection] = converted.test_data;
                }
                allResults.test_cases.push(...converted.test_cases);

                console.error(`Parsed ${converted.test_cases.length} test cases from ${filePath}`);
            } catch (e) {
                console.error(`Error processing ${filePath}: ${e.message}`);
            }
        }
    } else {
        // Process local files
        const filePath = args[0];

        if (!fs.existsSync(filePath)) {
            console.error(`File not found: ${filePath}`);
            process.exit(1);
        }

        const stats = fs.statSync(filePath);
        const files = stats.isDirectory()
            ? fs.readdirSync(filePath).filter(f => f.endsWith('.js')).map(f => path.join(filePath, f))
            : [filePath];

        for (const file of files) {
            try {
                const content = fs.readFileSync(file, 'utf8');
                const parser = new JsTestParser(content, file);
                const parsed = parser.parse();
                const converted = convertToTestFormat(parsed,
                    path.basename(file, '.js').toUpperCase().replace(/[^A-Z0-9]/g, '').substring(0, 6));

                allResults.sources.push(file);
                if (converted.test_data.length > 0) {
                    allResults.test_data[converted.collection] = converted.test_data;
                }
                allResults.test_cases.push(...converted.test_cases);

                console.error(`Parsed ${converted.test_cases.length} test cases from ${file}`);
            } catch (e) {
                console.error(`Error processing ${file}: ${e.message}`);
            }
        }
    }

    // Re-number test cases
    allResults.test_cases = allResults.test_cases.map((tc, i) => ({
        ...tc,
        id: `IMP${String(i + 1).padStart(3, '0')}`
    }));

    const output = JSON.stringify(allResults, null, 2);

    if (outputFile) {
        fs.writeFileSync(outputFile, output);
        console.error(`\nWrote ${allResults.test_cases.length} test cases to ${outputFile}`);
    } else {
        console.log(output);
    }

    console.error(`\nSummary:`);
    console.error(`  Sources: ${allResults.sources.length}`);
    console.error(`  Test cases: ${allResults.test_cases.length}`);
    console.error(`  Collections with data: ${Object.keys(allResults.test_data).length}`);
}

main().catch(e => {
    console.error(`Fatal error: ${e.message}`);
    process.exit(1);
});
