#!/usr/bin/env node
/**
 * Generate Purchase Orders Data
 *
 * Generates a configurable number of purchase order documents for testing
 * the $facet pagination pattern. This script can be used to create larger
 * datasets for performance testing.
 *
 * Usage:
 *   node generate-purchase-orders.js [--count <number>] [--output <file>] [--format mongodb|oracle]
 *
 * Examples:
 *   node generate-purchase-orders.js --count 100 --output purchase_orders.json
 *   node generate-purchase-orders.js --count 1000 --format oracle --output load_data.sql
 */

const fs = require('fs');

// Configuration
const DEFAULT_COUNT = 100;
const LOCATIONS = ['LOC001', 'LOC002', 'LOC003', 'LOC004', 'LOC005'];
const STATUSES = ['pending', 'completed', 'shipped', 'cancelled', 'processing'];

// Parse command line arguments
const args = process.argv.slice(2);
let count = DEFAULT_COUNT;
let outputFile = null;
let format = 'mongodb';

for (let i = 0; i < args.length; i++) {
    if (args[i] === '--count' && args[i + 1]) {
        count = parseInt(args[i + 1], 10);
        i++;
    } else if (args[i] === '--output' && args[i + 1]) {
        outputFile = args[i + 1];
        i++;
    } else if (args[i] === '--format' && args[i + 1]) {
        format = args[i + 1].toLowerCase();
        i++;
    } else if (args[i] === '--help' || args[i] === '-h') {
        console.log(`
Generate Purchase Orders Data

Usage:
  node generate-purchase-orders.js [options]

Options:
  --count <number>     Number of documents to generate (default: ${DEFAULT_COUNT})
  --output <file>      Output file path (default: stdout for JSON, required for Oracle)
  --format <type>      Output format: mongodb (JSON) or oracle (SQL)
  --help, -h           Show this help message

Examples:
  node generate-purchase-orders.js --count 100 --output data.json
  node generate-purchase-orders.js --count 500 --format oracle --output load.sql
`);
        process.exit(0);
    }
}

/**
 * Generate a random date between two dates
 */
function randomDate(start, end) {
    const startTime = start.getTime();
    const endTime = end.getTime();
    return new Date(startTime + Math.random() * (endTime - startTime));
}

/**
 * Generate a random float between min and max
 */
function randomFloat(min, max) {
    return Math.round((Math.random() * (max - min) + min) * 100) / 100;
}

/**
 * Pick random element from array
 */
function randomChoice(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
}

/**
 * Generate unique document reference numbers
 */
function generateDocRef(year, index) {
    return `DOC-${year}-${String(index).padStart(5, '0')}`;
}

/**
 * Generate customer PO number
 */
function generateCustomerPO(locationIndex, orderIndex) {
    const letters = ['A', 'B', 'C', 'D', 'E'];
    return `PO-${letters[locationIndex]}${String(orderIndex).padStart(4, '0')}`;
}

/**
 * Generate purchase order documents
 */
function generateDocuments(count) {
    const documents = [];
    const startDate = new Date('2024-01-01T00:00:00.000Z');
    const endDate = new Date('2025-12-31T23:59:59.999Z');

    // Track doc ref numbers per location for realistic grouping
    const locationCounters = {};
    LOCATIONS.forEach((loc, idx) => {
        locationCounters[loc] = { docIndex: 1, poIndex: 1, locIdx: idx };
    });

    for (let i = 0; i < count; i++) {
        const location = randomChoice(LOCATIONS);
        const counter = locationCounters[location];

        // Sometimes reuse same docRef/PO combination to create groups (20% chance)
        const reusePrevious = i > 0 && Math.random() < 0.2;

        let docRefNumber, customerPONumber, orderedDate;

        let orderedDateObj;  // The $date object to use

        if (reusePrevious && documents.length > 0) {
            // Find a previous doc with same location
            const prevDocs = documents.filter(d => d.locationNumber === location);
            if (prevDocs.length > 0) {
                const prev = prevDocs[Math.floor(Math.random() * prevDocs.length)];
                docRefNumber = prev.docRefNumber;
                customerPONumber = prev.customerPONumber;
                orderedDateObj = prev.orderedDate;  // Already in {$date: ...} format
            } else {
                docRefNumber = generateDocRef(2024, counter.docIndex++);
                customerPONumber = generateCustomerPO(counter.locIdx, counter.poIndex++);
                orderedDateObj = { $date: randomDate(startDate, endDate).toISOString() };
            }
        } else {
            docRefNumber = generateDocRef(2024, counter.docIndex++);
            customerPONumber = generateCustomerPO(counter.locIdx, counter.poIndex++);
            orderedDateObj = { $date: randomDate(startDate, endDate).toISOString() };
        }

        const doc = {
            _id: `PO${String(i + 1).padStart(6, '0')}`,
            locationNumber: location,
            docRefNumber: docRefNumber,
            customerPONumber: customerPONumber,
            orderedDate: orderedDateObj,
            amount: randomFloat(100, 10000),
            status: randomChoice(STATUSES),
            lineItems: Math.floor(Math.random() * 10) + 1,
            priority: randomChoice(['high', 'medium', 'low']),
            createdAt: { $date: new Date().toISOString() }
        };

        documents.push(doc);
    }

    return documents;
}

/**
 * Generate Oracle SQL for loading documents
 */
function generateOracleSql(documents) {
    const chunkSize = 1500;

    let sql = `-- Generated Purchase Orders Data
-- Documents: ${documents.length}
-- Generated: ${new Date().toISOString()}

SET DEFINE OFF
SET SERVEROUTPUT ON SIZE UNLIMITED
SET FEEDBACK OFF

-- Drop and recreate table
BEGIN EXECUTE IMMEDIATE 'DROP TABLE purchase_orders CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/

CREATE TABLE purchase_orders (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    data CLOB CONSTRAINT purchase_orders_json CHECK (data IS JSON)
);

DECLARE
    v_clob CLOB;
    v_count NUMBER := 0;
BEGIN
`;

    documents.forEach((doc, i) => {
        // Convert $date to ISO string for Oracle
        const processedDoc = JSON.parse(JSON.stringify(doc));
        if (processedDoc.orderedDate && processedDoc.orderedDate.$date) {
            processedDoc.orderedDate = processedDoc.orderedDate.$date;
        }
        if (processedDoc.createdAt && processedDoc.createdAt.$date) {
            processedDoc.createdAt = processedDoc.createdAt.$date;
        }

        const jsonStr = JSON.stringify(processedDoc).replace(/'/g, "''");

        // Split into chunks
        const chunks = [];
        for (let j = 0; j < jsonStr.length; j += chunkSize) {
            chunks.push(jsonStr.substring(j, j + chunkSize));
        }

        sql += `    -- Document ${i + 1}\n`;
        sql += `    v_clob := '';\n`;
        chunks.forEach(chunk => {
            sql += `    v_clob := v_clob || '${chunk}';\n`;
        });
        sql += `    INSERT INTO purchase_orders (data) VALUES (v_clob);\n`;
        sql += `    v_count := v_count + 1;\n`;

        // Commit every 50 documents
        if ((i + 1) % 50 === 0) {
            sql += `    COMMIT;\n`;
        }
        sql += '\n';
    });

    sql += `    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Inserted ' || v_count || ' documents into purchase_orders');
END;
/

SELECT COUNT(*) AS total_records FROM purchase_orders;

EXIT;
`;

    return sql;
}

/**
 * Main entry point
 */
function main() {
    console.error(`Generating ${count} purchase order documents...`);

    const documents = generateDocuments(count);

    let output;
    if (format === 'oracle') {
        output = generateOracleSql(documents);
    } else {
        // MongoDB JSON format
        output = JSON.stringify({
            collection: 'purchase_orders',
            description: `Generated purchase orders test data (${count} documents)`,
            generated: new Date().toISOString(),
            documents: documents
        }, null, 2);
    }

    if (outputFile) {
        fs.writeFileSync(outputFile, output);
        console.error(`Output written to: ${outputFile}`);
    } else {
        console.log(output);
    }

    // Print summary
    const uniqueLocations = new Set(documents.map(d => d.locationNumber)).size;
    const uniqueDocRefs = new Set(documents.map(d => d.docRefNumber)).size;
    const uniqueCombos = new Set(documents.map(d =>
        `${d.docRefNumber}|${d.customerPONumber}|${d.orderedDate.$date}`
    )).size;

    console.error(`
Summary:
  Total documents:        ${documents.length}
  Unique locations:       ${uniqueLocations}
  Unique doc references:  ${uniqueDocRefs}
  Unique group combos:    ${uniqueCombos}
`);
}

main();
