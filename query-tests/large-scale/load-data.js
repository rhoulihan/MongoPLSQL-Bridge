#!/usr/bin/env node
/**
 * Data Loader for Large-Scale Cross-Database Testing
 *
 * Loads generated data into MongoDB and Oracle databases for comparison testing.
 * Oracle data is loaded using SQL with PL/SQL for CLOB handling.
 *
 * Usage:
 *   node load-data.js --target mongodb|oracle|both [--data-dir <dir>] [--drop]
 *
 * Options:
 *   --target    Target database(s): mongodb, oracle, or both (default: both)
 *   --data-dir  Directory containing generated data (default: ./data)
 *   --drop      Drop existing collections/tables before loading
 *   --batch     Batch size for inserts (default: 100)
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

// Parse command line arguments
const args = process.argv.slice(2);
let target = 'both';
let dataDir = path.join(__dirname, 'data');
let dropExisting = false;
let batchSize = 100;

for (let i = 0; i < args.length; i++) {
    if (args[i] === '--target' && args[i + 1]) {
        target = args[i + 1];
        i++;
    } else if (args[i] === '--data-dir' && args[i + 1]) {
        dataDir = args[i + 1];
        i++;
    } else if (args[i] === '--drop') {
        dropExisting = true;
    } else if (args[i] === '--batch' && args[i + 1]) {
        batchSize = parseInt(args[i + 1]);
        i++;
    }
}

// Database configurations
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://admin:admin123@localhost:27017';
const MONGODB_DB = process.env.MONGODB_DB || 'largescale_test';

// Oracle connection via Docker
const ORACLE_CONTAINER = 'mongo-translator-oracle';
const ORACLE_USER = 'translator';
const ORACLE_PASSWORD = 'translator123';
const ORACLE_CONNECT_STRING = '//localhost:1521/FREEPDB1';

/**
 * Load data into MongoDB
 */
async function loadMongoDb(dataDir, dropExisting, batchSize) {
    const { MongoClient } = require('mongodb');

    console.log('\n=== Loading data into MongoDB ===');
    console.log(`URI: ${MONGODB_URI}`);
    console.log(`Database: ${MONGODB_DB}`);

    const client = new MongoClient(MONGODB_URI);

    try {
        await client.connect();
        const db = client.db(MONGODB_DB);

        // Read manifest
        const manifestPath = path.join(dataDir, 'manifest.json');
        if (!fs.existsSync(manifestPath)) {
            throw new Error(`Manifest not found at ${manifestPath}. Run generate-data.js first.`);
        }
        const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));

        console.log(`\nDataset size: ${manifest.size}`);
        console.log(`Generated: ${manifest.generated}`);
        console.log(`Collections: ${manifest.collections.length}`);

        for (const collectionInfo of manifest.collections) {
            const collectionName = collectionInfo.name;
            const expectedCount = collectionInfo.count;

            console.log(`\n--- ${collectionName} (${expectedCount.toLocaleString()} documents) ---`);

            const collection = db.collection(collectionName);

            if (dropExisting) {
                console.log('  Dropping existing collection...');
                await collection.drop().catch(() => {}); // Ignore if doesn't exist
            }

            // Get batch files
            const collectionDir = path.join(dataDir, collectionName);
            if (!fs.existsSync(collectionDir)) {
                console.log(`  WARNING: Data directory not found: ${collectionDir}`);
                continue;
            }

            const batchFiles = fs.readdirSync(collectionDir)
                .filter(f => f.endsWith('.json'))
                .sort();

            let totalInserted = 0;
            const startTime = Date.now();

            for (const batchFile of batchFiles) {
                const batchPath = path.join(collectionDir, batchFile);
                const documents = JSON.parse(fs.readFileSync(batchPath, 'utf8'));

                // Convert date strings to Date objects
                const processedDocs = documents.map(doc => processDocument(doc));

                // Insert in smaller batches
                for (let i = 0; i < processedDocs.length; i += batchSize) {
                    const batch = processedDocs.slice(i, i + batchSize);
                    await collection.insertMany(batch, { ordered: false });
                    totalInserted += batch.length;

                    const progress = ((totalInserted / expectedCount) * 100).toFixed(1);
                    process.stdout.write(`\r  Progress: ${progress}% (${totalInserted.toLocaleString()}/${expectedCount.toLocaleString()})`);
                }
            }

            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            const rate = (totalInserted / (elapsed || 1)).toFixed(0);
            console.log(`\n  Completed: ${totalInserted.toLocaleString()} docs in ${elapsed}s (${rate} docs/sec)`);

            // Create indexes
            console.log('  Creating indexes...');
            await createIndexes(collection, collectionName);
        }

        console.log('\n=== MongoDB loading complete ===\n');

    } finally {
        await client.close();
    }
}

/**
 * Generate SQL for loading a collection into Oracle
 * Uses PL/SQL with CLOB concatenation for large JSON documents
 */
function generateOracleSql(collectionName, documents, dropExisting) {
    const chunkSize = 2000; // Characters per chunk to avoid SQL*Plus line limits

    let sql = '';
    sql += 'SET DEFINE OFF\n';
    sql += 'SET SERVEROUTPUT ON SIZE UNLIMITED\n';
    sql += 'SET FEEDBACK OFF\n';
    sql += '\n';

    if (dropExisting) {
        sql += `-- Drop existing table\n`;
        sql += `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ${collectionName} CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;\n`;
        sql += '/\n';
    }

    sql += `-- Create table if not exists\n`;
    sql += `BEGIN\n`;
    sql += `  EXECUTE IMMEDIATE 'CREATE TABLE ${collectionName} (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, data CLOB CONSTRAINT ${collectionName}_json CHECK (data IS JSON))';\n`;
    sql += `EXCEPTION\n`;
    sql += `  WHEN OTHERS THEN\n`;
    sql += `    IF SQLCODE != -955 THEN RAISE; END IF; -- Ignore "table already exists"\n`;
    sql += `END;\n`;
    sql += '/\n\n';

    sql += `DECLARE\n`;
    sql += `  v_clob CLOB;\n`;
    sql += `  v_count NUMBER := 0;\n`;
    sql += `BEGIN\n`;

    documents.forEach((doc, i) => {
        // Escape single quotes in JSON
        const jsonStr = JSON.stringify(doc).replace(/'/g, "''");

        // Split into chunks
        const chunks = [];
        for (let j = 0; j < jsonStr.length; j += chunkSize) {
            chunks.push(jsonStr.substring(j, j + chunkSize));
        }

        sql += `  -- Document ${i + 1}\n`;
        sql += `  v_clob := '';\n`;
        chunks.forEach(chunk => {
            sql += `  v_clob := v_clob || '${chunk}';\n`;
        });
        sql += `  INSERT INTO ${collectionName} (data) VALUES (v_clob);\n`;
        sql += `  v_count := v_count + 1;\n`;

        // Commit every 50 documents to avoid undo segment issues
        if ((i + 1) % 50 === 0) {
            sql += `  COMMIT;\n`;
        }
        sql += '\n';
    });

    sql += `  COMMIT;\n`;
    sql += `  DBMS_OUTPUT.PUT_LINE('Inserted ' || v_count || ' documents into ${collectionName}');\n`;
    sql += `END;\n`;
    sql += `/\n\n`;

    return sql;
}

/**
 * Execute SQL in Oracle via Docker
 */
function executeOracleSql(sqlFile) {
    try {
        // Copy SQL file to container
        execSync(`docker cp "${sqlFile}" ${ORACLE_CONTAINER}:/tmp/load_data.sql`, { encoding: 'utf8' });

        // Execute SQL
        const result = execSync(
            `docker exec ${ORACLE_CONTAINER} sqlplus -s ${ORACLE_USER}/${ORACLE_PASSWORD}@${ORACLE_CONNECT_STRING} @/tmp/load_data.sql`,
            { encoding: 'utf8', maxBuffer: 50 * 1024 * 1024 }
        );

        return result;
    } catch (err) {
        throw new Error(`Oracle execution failed: ${err.message}`);
    }
}

/**
 * Load data into Oracle using SQL
 */
async function loadOracle(dataDir, dropExisting, batchSize) {
    console.log('\n=== Loading data into Oracle ===');
    console.log(`Container: ${ORACLE_CONTAINER}`);
    console.log(`User: ${ORACLE_USER}`);

    // Read manifest
    const manifestPath = path.join(dataDir, 'manifest.json');
    if (!fs.existsSync(manifestPath)) {
        throw new Error(`Manifest not found at ${manifestPath}. Run generate-data.js first.`);
    }
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'));

    console.log(`\nDataset size: ${manifest.size}`);
    console.log(`Generated: ${manifest.generated}`);
    console.log(`Collections: ${manifest.collections.length}`);

    for (const collectionInfo of manifest.collections) {
        const collectionName = collectionInfo.name;
        const expectedCount = collectionInfo.count;

        console.log(`\n--- ${collectionName} (${expectedCount.toLocaleString()} documents) ---`);

        // Get batch files
        const collectionDir = path.join(dataDir, collectionName);
        if (!fs.existsSync(collectionDir)) {
            console.log(`  WARNING: Data directory not found: ${collectionDir}`);
            continue;
        }

        const batchFiles = fs.readdirSync(collectionDir)
            .filter(f => f.endsWith('.json'))
            .sort();

        let totalInserted = 0;
        const startTime = Date.now();
        let isFirstBatch = true;

        for (const batchFile of batchFiles) {
            const batchPath = path.join(collectionDir, batchFile);
            const documents = JSON.parse(fs.readFileSync(batchPath, 'utf8'));

            // Process in smaller batches for Oracle
            const oracleBatchSize = Math.min(batchSize, 50); // Oracle is slower, use smaller batches

            for (let i = 0; i < documents.length; i += oracleBatchSize) {
                const batch = documents.slice(i, i + oracleBatchSize);

                // Generate SQL for this batch
                const sql = generateOracleSql(collectionName, batch, isFirstBatch && dropExisting);
                isFirstBatch = false;

                // Write to temp file
                const tempFile = '/tmp/oracle_batch.sql';
                fs.writeFileSync(tempFile, sql);

                // Execute
                try {
                    executeOracleSql(tempFile);
                    totalInserted += batch.length;

                    const progress = ((totalInserted / expectedCount) * 100).toFixed(1);
                    process.stdout.write(`\r  Progress: ${progress}% (${totalInserted.toLocaleString()}/${expectedCount.toLocaleString()})`);
                } catch (err) {
                    console.error(`\n  Error inserting batch: ${err.message}`);
                }
            }
        }

        const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
        const rate = (totalInserted / (elapsed || 1)).toFixed(0);
        console.log(`\n  Completed: ${totalInserted.toLocaleString()} docs in ${elapsed}s (${rate} docs/sec)`);

        // Create indexes
        console.log('  Creating indexes...');
        createOracleIndexes(collectionName);
    }

    console.log('\n=== Oracle loading complete ===\n');
}

/**
 * Process document to convert date strings to Date objects (MongoDB)
 */
function processDocument(doc) {
    if (doc === null || typeof doc !== 'object') {
        return doc;
    }

    if (Array.isArray(doc)) {
        return doc.map(item => processDocument(item));
    }

    const result = {};
    for (const [key, value] of Object.entries(doc)) {
        if (value && typeof value === 'object' && value.$date) {
            result[key] = new Date(value.$date);
        } else if (value && typeof value === 'string' && isISODateString(value)) {
            result[key] = new Date(value);
        } else if (typeof value === 'object') {
            result[key] = processDocument(value);
        } else {
            result[key] = value;
        }
    }
    return result;
}

/**
 * Check if string is ISO date format
 */
function isISODateString(str) {
    if (typeof str !== 'string') return false;
    // Match ISO 8601 date format
    return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?$/.test(str);
}

/**
 * Create indexes for MongoDB collection
 */
async function createIndexes(collection, collectionName) {
    const indexes = {
        'ecommerce_orders': [
            { customerId: 1 },
            { status: 1 },
            { 'timestamps.createdAt': 1 },
            { 'items.productId': 1 }
        ],
        'ecommerce_products': [
            { 'category.primary': 1 },
            { 'brand.name': 1 },
            { 'metadata.status': 1 },
            { 'ratings.average': -1 }
        ],
        'ecommerce_customers': [
            { 'profile.email': 1 },
            { 'loyalty.tier': 1 },
            { 'metadata.status': 1 }
        ],
        'ecommerce_reviews': [
            { productId: 1 },
            { customerId: 1 },
            { 'rating.overall': 1 },
            { 'metadata.status': 1 }
        ],
        'analytics_sessions': [
            { userId: 1 },
            { 'device.type': 1 },
            { 'traffic.source': 1 },
            { startTime: 1 }
        ],
        'analytics_events': [
            { sessionId: 1 },
            { eventType: 1 },
            { timestamp: 1 }
        ],
        'social_users': [
            { username: 1 },
            { 'stats.followers': -1 },
            { 'metadata.status': 1 }
        ],
        'social_posts': [
            { authorId: 1 },
            { type: 1 },
            { 'metadata.status': 1 },
            { 'metadata.createdAt': -1 }
        ],
        'iot_devices': [
            { deviceId: 1 },
            { type: 1 },
            { 'location.building': 1 },
            { 'status.health': 1 }
        ],
        'iot_readings': [
            { deviceId: 1 },
            { timestamp: 1 },
            { deviceId: 1, timestamp: 1 }
        ]
    };

    const collectionIndexes = indexes[collectionName] || [];
    for (const index of collectionIndexes) {
        try {
            await collection.createIndex(index);
        } catch (err) {
            console.log(`    Warning: Could not create index ${JSON.stringify(index)}: ${err.message}`);
        }
    }
}

/**
 * Create indexes for Oracle table
 */
function createOracleIndexes(collectionName) {
    const indexSql = `
SET FEEDBACK OFF
BEGIN
  -- Create JSON search index for better query performance
  BEGIN
    EXECUTE IMMEDIATE 'CREATE SEARCH INDEX idx_${collectionName}_json ON ${collectionName} (data) FOR JSON';
  EXCEPTION
    WHEN OTHERS THEN NULL; -- Ignore if exists
  END;
END;
/
EXIT;
`;

    const tempFile = '/tmp/oracle_index.sql';
    fs.writeFileSync(tempFile, indexSql);

    try {
        executeOracleSql(tempFile);
    } catch (err) {
        // Ignore index creation errors
    }
}

/**
 * Main entry point
 */
async function main() {
    console.log('==============================================');
    console.log('Large-Scale Data Loader for Cross-DB Testing');
    console.log('==============================================');
    console.log(`Target: ${target}`);
    console.log(`Data directory: ${dataDir}`);
    console.log(`Drop existing: ${dropExisting}`);
    console.log(`Batch size: ${batchSize}`);

    if (!fs.existsSync(dataDir)) {
        console.error(`\nError: Data directory not found: ${dataDir}`);
        console.error('Run generate-data.js first to generate test data.');
        process.exit(1);
    }

    const startTime = Date.now();

    try {
        if (target === 'mongodb' || target === 'both') {
            await loadMongoDb(dataDir, dropExisting, batchSize);
        }

        if (target === 'oracle' || target === 'both') {
            await loadOracle(dataDir, dropExisting, batchSize);
        }

        const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
        console.log(`\n=== All data loading complete in ${totalElapsed} minutes ===\n`);

    } catch (err) {
        console.error('\nError during data loading:', err.message);
        process.exit(1);
    }
}

main();
