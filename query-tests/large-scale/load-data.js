#!/usr/bin/env node
/**
 * Data Loader for Large-Scale Cross-Database Testing
 *
 * Loads generated data into MongoDB and Oracle databases for comparison testing.
 *
 * Usage:
 *   node load-data.js --target mongodb|oracle|both [--data-dir <dir>] [--drop]
 *
 * Options:
 *   --target    Target database(s): mongodb, oracle, or both (default: both)
 *   --data-dir  Directory containing generated data (default: ./data)
 *   --drop      Drop existing collections before loading
 *   --batch     Batch size for inserts (default: 1000)
 */

const fs = require('fs');
const path = require('path');

// Parse command line arguments
const args = process.argv.slice(2);
let target = 'both';
let dataDir = path.join(__dirname, 'data');
let dropExisting = false;
let batchSize = 1000;

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

const ORACLE_USER = process.env.ORACLE_USER || 'mongouser';
const ORACLE_PASSWORD = process.env.ORACLE_PASSWORD || 'mongopass';
const ORACLE_CONNECT_STRING = process.env.ORACLE_CONNECT_STRING || 'localhost:1521/FREEPDB1';

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
 * Load data into Oracle
 */
async function loadOracle(dataDir, dropExisting, batchSize) {
    const oracledb = require('oracledb');

    console.log('\n=== Loading data into Oracle ===');
    console.log(`Connect string: ${ORACLE_CONNECT_STRING}`);
    console.log(`User: ${ORACLE_USER}`);

    let connection;

    try {
        // Initialize Oracle client (thick mode for SODA)
        try {
            oracledb.initOracleClient();
        } catch (err) {
            // Already initialized or thin mode
        }

        connection = await oracledb.getConnection({
            user: ORACLE_USER,
            password: ORACLE_PASSWORD,
            connectString: ORACLE_CONNECT_STRING
        });

        // Get SODA database
        const soda = connection.getSodaDatabase();

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

            if (dropExisting) {
                console.log('  Dropping existing collection...');
                try {
                    const existingColl = await soda.openCollection(collectionName);
                    if (existingColl) {
                        await existingColl.drop();
                    }
                } catch (err) {
                    // Ignore if doesn't exist
                }
            }

            // Create collection
            const collection = await soda.createCollection(collectionName);

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

                // Insert in batches
                for (let i = 0; i < documents.length; i += batchSize) {
                    const batch = documents.slice(i, i + batchSize);

                    // SODA bulk insert
                    await collection.insertManyAndGet(batch);
                    await connection.commit();

                    totalInserted += batch.length;

                    const progress = ((totalInserted / expectedCount) * 100).toFixed(1);
                    process.stdout.write(`\r  Progress: ${progress}% (${totalInserted.toLocaleString()}/${expectedCount.toLocaleString()})`);
                }
            }

            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            const rate = (totalInserted / (elapsed || 1)).toFixed(0);
            console.log(`\n  Completed: ${totalInserted.toLocaleString()} docs in ${elapsed}s (${rate} docs/sec)`);

            // Create index on _id
            console.log('  Creating indexes...');
            await createOracleIndexes(connection, collectionName);
        }

        console.log('\n=== Oracle loading complete ===\n');

    } finally {
        if (connection) {
            await connection.close();
        }
    }
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
 * Create indexes for Oracle collection
 */
async function createOracleIndexes(connection, collectionName) {
    // Oracle SODA automatically creates indexes on key fields
    // Additional indexes can be created using SQL
    const indexStatements = {
        'ecommerce_orders': [
            `CREATE INDEX idx_${collectionName}_customer ON "${collectionName}" (JSON_VALUE(data, '$._id'))`,
            `CREATE SEARCH INDEX idx_${collectionName}_search ON "${collectionName}" (data) FOR JSON`
        ],
        'ecommerce_products': [
            `CREATE INDEX idx_${collectionName}_category ON "${collectionName}" (JSON_VALUE(data, '$.category.primary'))`,
            `CREATE SEARCH INDEX idx_${collectionName}_search ON "${collectionName}" (data) FOR JSON`
        ]
        // Add more as needed
    };

    const statements = indexStatements[collectionName] || [];
    for (const stmt of statements) {
        try {
            await connection.execute(stmt);
        } catch (err) {
            // Ignore if index already exists
            if (!err.message.includes('ORA-00955') && !err.message.includes('ORA-29879')) {
                console.log(`    Warning: ${err.message}`);
            }
        }
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
