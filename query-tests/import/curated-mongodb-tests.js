#!/usr/bin/env node
/**
 * Curated MongoDB Test Generator
 *
 * This script generates well-structured test cases based on patterns
 * found in MongoDB's official jstests. These are hand-curated to ensure
 * they work with our Oracle translation.
 *
 * Usage:
 *   node curated-mongodb-tests.js [--output <file>]
 */

const fs = require('fs');

/**
 * Curated test cases based on MongoDB jstests patterns
 * Each test includes:
 * - Test data to load
 * - MongoDB aggregation pipeline
 * - Expected behavior description
 */
const CURATED_TESTS = {
    // ========================================
    // STRING OPERATORS
    // ========================================
    string: {
        collection: 'string_tests',
        test_data: [
            { _id: 1, str: 'hello', str2: 'world', empty: '', mixed: 'HeLLo WoRLD' },
            { _id: 2, str: 'UPPERCASE', str2: 'lowercase', empty: '', mixed: '123abc' },
            { _id: 3, str: '  trimme  ', str2: '  spaces  ', empty: null, mixed: 'MiXeD' },
            { _id: 4, str: 'abcdefghij', str2: 'klmnop', num: 12345 },
            { _id: 5, str: '', str2: '', empty: '' },  // Empty strings
        ],
        tests: [
            {
                name: '$concat - basic string concatenation',
                operator: '$concat',
                pipeline: [
                    { $project: { _id: 1, result: { $concat: ['$str', ' ', '$str2'] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, result: 'hello world' },
                    { _id: 2, result: 'UPPERCASE lowercase' },
                    { _id: 3, result: '  trimme   spaces  ' },
                    { _id: 4, result: 'abcdefghij klmnop' },
                    { _id: 5, result: ' ' },
                ]
            },
            {
                name: '$concat - with null field returns null',
                operator: '$concat',
                pipeline: [
                    { $match: { _id: 3 } },
                    { $project: { _id: 1, result: { $concat: ['$str', '$empty'] } } }
                ],
                expected: [{ _id: 3, result: null }]
            },
            {
                name: '$toLower - convert to lowercase',
                operator: '$toLower',
                pipeline: [
                    { $project: { _id: 1, result: { $toLower: '$mixed' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, result: 'hello world' },
                    { _id: 2, result: '123abc' },
                    { _id: 3, result: 'mixed' },
                ]
            },
            {
                name: '$toUpper - convert to uppercase',
                operator: '$toUpper',
                pipeline: [
                    { $project: { _id: 1, result: { $toUpper: '$str' } } },
                    { $sort: { _id: 1 } },
                    { $limit: 3 }
                ],
                expected: [
                    { _id: 1, result: 'HELLO' },
                    { _id: 2, result: 'UPPERCASE' },
                    { _id: 3, result: '  TRIMME  ' },
                ]
            },
            {
                name: '$substr - extract substring',
                operator: '$substr',
                pipeline: [
                    { $match: { _id: 4 } },
                    { $project: { _id: 1, result: { $substr: ['$str', 0, 5] } } }
                ],
                expected: [{ _id: 4, result: 'abcde' }]
            },
            {
                name: '$substr - with offset',
                operator: '$substr',
                pipeline: [
                    { $match: { _id: 4 } },
                    { $project: { _id: 1, result: { $substr: ['$str', 3, 4] } } }
                ],
                expected: [{ _id: 4, result: 'defg' }]
            },
            {
                name: '$strLenCP - string length',
                operator: '$strLenCP',
                pipeline: [
                    { $project: { _id: 1, len: { $strLenCP: '$str' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, len: 5 },
                    { _id: 2, len: 9 },
                    { _id: 3, len: 10 },
                    { _id: 4, len: 10 },
                    { _id: 5, len: 0 },
                ]
            },
            {
                name: '$trim - remove whitespace',
                operator: '$trim',
                pipeline: [
                    { $match: { _id: 3 } },
                    { $project: { _id: 1, result: { $trim: { input: '$str' } } } }
                ],
                expected: [{ _id: 3, result: 'trimme' }]
            },
        ]
    },

    // ========================================
    // ARRAY OPERATORS
    // ========================================
    array: {
        collection: 'array_tests',
        test_data: [
            { _id: 1, arr: [1, 2, 3, 4, 5], tags: ['a', 'b', 'c'] },
            { _id: 2, arr: ['x', 'y', 'z'], tags: ['single'] },
            { _id: 3, arr: [], tags: [] },  // Empty arrays
            { _id: 4, arr: [10], tags: ['one', 'two', 'three', 'four'] },
            { _id: 5, arr: [[1, 2], [3, 4]], tags: ['nested'] },  // Nested arrays
            { _id: 6, arr: [null, 1, null, 2], tags: ['with', 'nulls'] },
        ],
        tests: [
            {
                name: '$size - array length',
                operator: '$size',
                pipeline: [
                    { $project: { _id: 1, arrSize: { $size: '$arr' }, tagSize: { $size: '$tags' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, arrSize: 5, tagSize: 3 },
                    { _id: 2, arrSize: 3, tagSize: 1 },
                    { _id: 3, arrSize: 0, tagSize: 0 },
                    { _id: 4, arrSize: 1, tagSize: 4 },
                    { _id: 5, arrSize: 2, tagSize: 1 },
                    { _id: 6, arrSize: 4, tagSize: 2 },
                ]
            },
            {
                name: '$arrayElemAt - first element (index 0)',
                operator: '$arrayElemAt',
                pipeline: [
                    { $match: { _id: { $in: [1, 2, 4] } } },
                    { $project: { _id: 1, first: { $arrayElemAt: ['$arr', 0] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, first: 1 },
                    { _id: 2, first: 'x' },
                    { _id: 4, first: 10 },
                ]
            },
            {
                name: '$arrayElemAt - last element (index -1)',
                operator: '$arrayElemAt',
                pipeline: [
                    { $match: { _id: { $in: [1, 2, 4] } } },
                    { $project: { _id: 1, last: { $arrayElemAt: ['$arr', -1] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, last: 5 },
                    { _id: 2, last: 'z' },
                    { _id: 4, last: 10 },
                ]
            },
            {
                name: '$arrayElemAt - out of bounds returns null',
                operator: '$arrayElemAt',
                pipeline: [
                    { $match: { _id: 4 } },
                    { $project: { _id: 1, outOfBounds: { $arrayElemAt: ['$arr', 10] } } }
                ],
                expected: [{ _id: 4, outOfBounds: null }]
            },
            {
                name: '$arrayElemAt - empty array returns null',
                operator: '$arrayElemAt',
                pipeline: [
                    { $match: { _id: 3 } },
                    { $project: { _id: 1, fromEmpty: { $arrayElemAt: ['$arr', 0] } } }
                ],
                expected: [{ _id: 3, fromEmpty: null }]
            },
            {
                name: '$first - array first element',
                operator: '$first',
                pipeline: [
                    { $match: { _id: { $lte: 2 } } },
                    { $project: { _id: 1, first: { $first: '$tags' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, first: 'a' },
                    { _id: 2, first: 'single' },
                ]
            },
            {
                name: '$last - array last element',
                operator: '$last',
                pipeline: [
                    { $match: { _id: { $lte: 2 } } },
                    { $project: { _id: 1, last: { $last: '$tags' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, last: 'c' },
                    { _id: 2, last: 'single' },
                ]
            },
        ]
    },

    // ========================================
    // CONDITIONAL OPERATORS
    // ========================================
    conditional: {
        collection: 'cond_tests',
        test_data: [
            { _id: 1, score: 95, status: 'active', value: 100 },
            { _id: 2, score: 75, status: 'active', value: 50 },
            { _id: 3, score: 55, status: 'inactive', value: null },
            { _id: 4, score: 45, status: 'active', value: 0 },
            { _id: 5, score: 85, status: 'pending', value: -10 },
        ],
        tests: [
            {
                name: '$cond - basic if-then-else',
                operator: '$cond',
                pipeline: [
                    { $project: { _id: 1, grade: { $cond: { if: { $gte: ['$score', 70] }, then: 'pass', else: 'fail' } } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, grade: 'pass' },
                    { _id: 2, grade: 'pass' },
                    { _id: 3, grade: 'fail' },
                    { _id: 4, grade: 'fail' },
                    { _id: 5, grade: 'pass' },
                ]
            },
            {
                name: '$cond - nested conditions',
                operator: '$cond',
                pipeline: [
                    {
                        $project: {
                            _id: 1,
                            grade: {
                                $cond: {
                                    if: { $gte: ['$score', 90] },
                                    then: 'A',
                                    else: {
                                        $cond: {
                                            if: { $gte: ['$score', 70] },
                                            then: 'B',
                                            else: 'C'
                                        }
                                    }
                                }
                            }
                        }
                    },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, grade: 'A' },
                    { _id: 2, grade: 'B' },
                    { _id: 3, grade: 'C' },
                    { _id: 4, grade: 'C' },
                    { _id: 5, grade: 'B' },
                ]
            },
            {
                name: '$cond - array syntax [condition, true, false]',
                operator: '$cond',
                pipeline: [
                    { $project: { _id: 1, isActive: { $cond: [{ $eq: ['$status', 'active'] }, 'yes', 'no'] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, isActive: 'yes' },
                    { _id: 2, isActive: 'yes' },
                    { _id: 3, isActive: 'no' },
                    { _id: 4, isActive: 'yes' },
                    { _id: 5, isActive: 'no' },
                ]
            },
            {
                name: '$ifNull - replace null with default',
                operator: '$ifNull',
                pipeline: [
                    { $project: { _id: 1, val: { $ifNull: ['$value', -1] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, val: 100 },
                    { _id: 2, val: 50 },
                    { _id: 3, val: -1 },
                    { _id: 4, val: 0 },
                    { _id: 5, val: -10 },
                ]
            },
            {
                name: '$ifNull - missing field',
                operator: '$ifNull',
                pipeline: [
                    { $project: { _id: 1, missing: { $ifNull: ['$nonexistent', 'default'] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, missing: 'default' },
                    { _id: 2, missing: 'default' },
                    { _id: 3, missing: 'default' },
                    { _id: 4, missing: 'default' },
                    { _id: 5, missing: 'default' },
                ]
            },
        ]
    },

    // ========================================
    // DATE OPERATORS
    // ========================================
    date: {
        collection: 'date_tests',
        test_data: [
            { _id: 1, date: new Date('2024-01-15T10:30:45.123Z'), name: 'January morning' },
            { _id: 2, date: new Date('2024-06-21T14:00:00.000Z'), name: 'June afternoon' },
            { _id: 3, date: new Date('2024-12-25T00:00:00.000Z'), name: 'Christmas midnight' },
            { _id: 4, date: new Date('2024-02-29T23:59:59.999Z'), name: 'Leap day end' },
            { _id: 5, date: new Date('2024-07-04T12:00:00.000Z'), name: 'July 4th noon' },
        ],
        tests: [
            {
                name: '$year - extract year',
                operator: '$year',
                pipeline: [
                    { $project: { _id: 1, year: { $year: '$date' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, year: 2024 },
                    { _id: 2, year: 2024 },
                    { _id: 3, year: 2024 },
                    { _id: 4, year: 2024 },
                    { _id: 5, year: 2024 },
                ]
            },
            {
                name: '$month - extract month (1-12)',
                operator: '$month',
                pipeline: [
                    { $project: { _id: 1, month: { $month: '$date' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, month: 1 },
                    { _id: 2, month: 6 },
                    { _id: 3, month: 12 },
                    { _id: 4, month: 2 },
                    { _id: 5, month: 7 },
                ]
            },
            {
                name: '$dayOfMonth - extract day of month',
                operator: '$dayOfMonth',
                pipeline: [
                    { $project: { _id: 1, day: { $dayOfMonth: '$date' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, day: 15 },
                    { _id: 2, day: 21 },
                    { _id: 3, day: 25 },
                    { _id: 4, day: 29 },
                    { _id: 5, day: 4 },
                ]
            },
            {
                name: '$hour - extract hour (0-23)',
                operator: '$hour',
                pipeline: [
                    { $project: { _id: 1, hour: { $hour: '$date' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, hour: 10 },
                    { _id: 2, hour: 14 },
                    { _id: 3, hour: 0 },
                    { _id: 4, hour: 23 },
                    { _id: 5, hour: 12 },
                ]
            },
            {
                name: '$minute - extract minute',
                operator: '$minute',
                pipeline: [
                    { $project: { _id: 1, minute: { $minute: '$date' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, minute: 30 },
                    { _id: 2, minute: 0 },
                    { _id: 3, minute: 0 },
                    { _id: 4, minute: 59 },
                    { _id: 5, minute: 0 },
                ]
            },
            {
                name: '$second - extract second',
                operator: '$second',
                pipeline: [
                    { $project: { _id: 1, second: { $second: '$date' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, second: 45 },
                    { _id: 2, second: 0 },
                    { _id: 3, second: 0 },
                    { _id: 4, second: 59 },
                    { _id: 5, second: 0 },
                ]
            },
            {
                name: '$dayOfWeek - extract day of week (1=Sunday)',
                operator: '$dayOfWeek',
                pipeline: [
                    { $project: { _id: 1, dow: { $dayOfWeek: '$date' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, dow: 2 },  // Monday
                    { _id: 2, dow: 6 },  // Friday
                    { _id: 3, dow: 4 },  // Wednesday
                    { _id: 4, dow: 5 },  // Thursday
                    { _id: 5, dow: 5 },  // Thursday
                ]
            },
        ]
    },

    // ========================================
    // ARITHMETIC OPERATORS (edge cases)
    // ========================================
    arithmetic: {
        collection: 'arith_tests',
        test_data: [
            { _id: 1, a: 10, b: 3 },
            { _id: 2, a: 0, b: 5 },
            { _id: 3, a: -10, b: 3 },
            { _id: 4, a: 100, b: 0 },  // Division by zero case
            { _id: 5, a: 7.5, b: 2.5 },
            { _id: 6, a: null, b: 5 },
        ],
        tests: [
            {
                name: '$add - basic addition',
                operator: '$add',
                pipeline: [
                    { $match: { _id: { $lte: 3 } } },
                    { $project: { _id: 1, sum: { $add: ['$a', '$b'] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, sum: 13 },
                    { _id: 2, sum: 5 },
                    { _id: 3, sum: -7 },
                ]
            },
            {
                name: '$subtract - basic subtraction',
                operator: '$subtract',
                pipeline: [
                    { $match: { _id: { $lte: 3 } } },
                    { $project: { _id: 1, diff: { $subtract: ['$a', '$b'] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, diff: 7 },
                    { _id: 2, diff: -5 },
                    { _id: 3, diff: -13 },
                ]
            },
            {
                name: '$multiply - basic multiplication',
                operator: '$multiply',
                pipeline: [
                    { $match: { _id: { $lte: 3 } } },
                    { $project: { _id: 1, product: { $multiply: ['$a', '$b'] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, product: 30 },
                    { _id: 2, product: 0 },
                    { _id: 3, product: -30 },
                ]
            },
            {
                name: '$divide - basic division',
                operator: '$divide',
                pipeline: [
                    { $match: { _id: { $in: [1, 3, 5] } } },
                    { $project: { _id: 1, quotient: { $divide: ['$a', '$b'] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, quotient: 3.3333333333333335 },
                    { _id: 3, quotient: -3.3333333333333335 },
                    { _id: 5, quotient: 3 },
                ]
            },
            {
                name: '$mod - modulo operation',
                operator: '$mod',
                pipeline: [
                    { $match: { _id: { $lte: 3 } } },
                    { $project: { _id: 1, remainder: { $mod: ['$a', '$b'] } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 1, remainder: 1 },
                    { _id: 2, remainder: 0 },
                    { _id: 3, remainder: -1 },
                ]
            },
            {
                name: '$add - with null returns null',
                operator: '$add',
                pipeline: [
                    { $match: { _id: 6 } },
                    { $project: { _id: 1, result: { $add: ['$a', '$b'] } } }
                ],
                expected: [{ _id: 6, result: null }]
            },
        ]
    },

    // ========================================
    // ACCUMULATOR EDGE CASES
    // ========================================
    accumulator: {
        collection: 'acc_tests',
        test_data: [
            { _id: 1, group: 'A', value: 10, status: 'active' },
            { _id: 2, group: 'A', value: 20, status: 'active' },
            { _id: 3, group: 'A', value: 30, status: 'inactive' },
            { _id: 4, group: 'B', value: 5, status: 'active' },
            { _id: 5, group: 'B', value: null, status: 'active' },
            { _id: 6, group: 'C', value: 100, status: 'pending' },
        ],
        tests: [
            {
                name: '$sum - with null values (null is ignored)',
                operator: '$sum',
                pipeline: [
                    { $group: { _id: '$group', total: { $sum: '$value' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 'A', total: 60 },
                    { _id: 'B', total: 5 },
                    { _id: 'C', total: 100 },
                ]
            },
            {
                name: '$avg - with null values',
                operator: '$avg',
                pipeline: [
                    { $group: { _id: '$group', avg: { $avg: '$value' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 'A', avg: 20 },
                    { _id: 'B', avg: 5 },
                    { _id: 'C', avg: 100 },
                ]
            },
            {
                name: '$min/$max - basic',
                operator: '$min/$max',
                pipeline: [
                    { $group: { _id: '$group', minVal: { $min: '$value' }, maxVal: { $max: '$value' } } },
                    { $sort: { _id: 1 } }
                ],
                expected: [
                    { _id: 'A', minVal: 10, maxVal: 30 },
                    { _id: 'B', minVal: 5, maxVal: 5 },
                    { _id: 'C', minVal: 100, maxVal: 100 },
                ]
            },
            {
                name: '$push - collect values into array',
                operator: '$push',
                pipeline: [
                    { $match: { group: 'A' } },
                    { $sort: { _id: 1 } },
                    { $group: { _id: '$group', values: { $push: '$value' } } }
                ],
                expected: [{ _id: 'A', values: [10, 20, 30] }]
            },
            {
                name: '$addToSet - unique values',
                operator: '$addToSet',
                pipeline: [
                    { $group: { _id: null, statuses: { $addToSet: '$status' } } }
                ],
                // Note: Order is not guaranteed, but should have 3 unique values
                expected_count: 1
            },
            {
                name: '$count - count documents',
                operator: '$count',
                pipeline: [
                    { $match: { status: 'active' } },
                    { $count: 'activeCount' }
                ],
                expected: [{ activeCount: 4 }]
            },
        ]
    },
};

/**
 * Convert curated tests to our test format
 */
function generateTestCases() {
    const result = {
        version: '1.0',
        description: 'Curated MongoDB aggregation tests based on official jstests patterns',
        generated_at: new Date().toISOString(),
        test_data: {},
        test_cases: []
    };

    let testIndex = 1;

    for (const [category, data] of Object.entries(CURATED_TESTS)) {
        // Add test data
        result.test_data[data.collection] = data.test_data.map(doc => {
            // Convert Date objects to ISO strings for JSON
            const converted = { ...doc };
            for (const [key, value] of Object.entries(converted)) {
                if (value instanceof Date) {
                    converted[key] = { $date: value.toISOString() };
                }
            }
            return converted;
        });

        // Add test cases
        for (const test of data.tests) {
            const testCase = {
                id: `CUR${String(testIndex).padStart(3, '0')}`,
                name: test.name,
                category: category,
                operator: test.operator,
                description: `Curated test from MongoDB patterns: ${test.name}`,
                collection: data.collection,
                mongodb_pipeline: test.pipeline,
                oracle_sql: null,  // To be filled by translator or manually
                sort_by: detectSortField(test.pipeline),
            };

            if (test.expected) {
                testCase.expected_result = test.expected;
                testCase.expected_count = test.expected.length;
            } else if (test.expected_count) {
                testCase.expected_count = test.expected_count;
            }

            result.test_cases.push(testCase);
            testIndex++;
        }
    }

    return result;
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
 * Generate MongoDB data loader script
 */
function generateMongoLoader(testData) {
    let script = `// MongoDB Test Data Loader - Generated ${new Date().toISOString()}
// Usage: mongosh < this_file.js

use test_db;

`;
    for (const [collection, documents] of Object.entries(testData)) {
        script += `// ${collection}\n`;
        script += `db.${collection}.drop();\n`;
        script += `db.${collection}.insertMany(${JSON.stringify(documents, null, 2).replace(/"\$date":\s*"([^"]+)"/g, 'ISODate("$1")')});\n\n`;
    }

    return script;
}

/**
 * Generate Oracle SQL loader
 */
function generateOracleLoader(testData) {
    let script = `-- Oracle Test Data Loader - Generated ${new Date().toISOString()}
-- Usage: sqlplus user/pass@db @this_file.sql

SET ECHO OFF
SET FEEDBACK OFF

`;

    for (const [collection, documents] of Object.entries(testData)) {
        script += `-- Drop and create ${collection}\n`;
        script += `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${collection} CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;\n/\n`;
        script += `CREATE TABLE ${collection} (id VARCHAR2(50) PRIMARY KEY, data JSON);\n\n`;

        for (const doc of documents) {
            const id = doc._id;
            // Convert $date back to string for Oracle JSON
            const jsonDoc = JSON.stringify(doc).replace(/\{"\$date":\s*"([^"]+)"\}/g, '"$1"');
            script += `INSERT INTO ${collection} (id, data) VALUES ('${id}', '${jsonDoc.replace(/'/g, "''")}');\n`;
        }
        script += '\n';
    }

    script += 'COMMIT;\n';
    return script;
}

// Main
const args = process.argv.slice(2);
let outputFile = null;
const outputIndex = args.indexOf('--output');
if (outputIndex !== -1 && args[outputIndex + 1]) {
    outputFile = args[outputIndex + 1];
}

const result = generateTestCases();

if (args.includes('--mongo-loader')) {
    const loader = generateMongoLoader(result.test_data);
    console.log(loader);
} else if (args.includes('--oracle-loader')) {
    const loader = generateOracleLoader(result.test_data);
    console.log(loader);
} else {
    const output = JSON.stringify(result, null, 2);
    if (outputFile) {
        fs.writeFileSync(outputFile, output);
        console.error(`Wrote ${result.test_cases.length} test cases to ${outputFile}`);
    } else {
        console.log(output);
    }
}

console.error(`\nSummary:`);
console.error(`  Categories: ${Object.keys(CURATED_TESTS).length}`);
console.error(`  Test cases: ${result.test_cases.length}`);
console.error(`  Collections: ${Object.keys(result.test_data).length}`);
