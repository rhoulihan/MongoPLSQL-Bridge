// MongoDB initialization script for MongoPLSQL-Bridge testing
// This script mirrors the Oracle test data for cross-database validation

// Switch to the test database
db = db.getSiblingDB('testdb');

// Create user for the test database
db.createUser({
    user: 'translator',
    pwd: 'translator123',
    roles: [
        { role: 'readWrite', db: 'testdb' }
    ]
});

print('Created translator user');

// ============================================================
// Test Customers Collection
// ============================================================
db.test_customers.drop();
db.createCollection('test_customers');

db.test_customers.insertMany([
    {
        name: "John Doe",
        email: "john@example.com",
        status: "active",
        age: 30,
        tier: "gold"
    },
    {
        name: "Jane Smith",
        email: "jane@example.com",
        status: "active",
        age: 25,
        tier: "silver"
    },
    {
        name: "Bob Wilson",
        email: "bob@example.com",
        status: "inactive",
        age: 45,
        tier: "bronze"
    },
    {
        name: "Alice Brown",
        email: "alice@example.com",
        status: "active",
        age: 35,
        tier: "gold"
    }
]);

print('Inserted ' + db.test_customers.countDocuments() + ' customers');

// Create indexes matching Oracle
db.test_customers.createIndex({ status: 1 });
db.test_customers.createIndex({ email: 1 });

// ============================================================
// Test Orders Collection
// ============================================================
db.test_orders.drop();
db.createCollection('test_orders');

db.test_orders.insertMany([
    {
        orderId: 1001,
        customerId: "john@example.com",
        status: "completed",
        amount: 150.00,
        items: [
            { product: "Widget", qty: 2 },
            { product: "Gadget", qty: 1 }
        ]
    },
    {
        orderId: 1002,
        customerId: "jane@example.com",
        status: "completed",
        amount: 250.00,
        items: [
            { product: "Widget", qty: 5 }
        ]
    },
    {
        orderId: 1003,
        customerId: "john@example.com",
        status: "pending",
        amount: 75.00,
        items: [
            { product: "Gadget", qty: 3 }
        ]
    },
    {
        orderId: 1004,
        customerId: "alice@example.com",
        status: "completed",
        amount: 500.00,
        items: [
            { product: "Premium Widget", qty: 1 }
        ]
    },
    {
        orderId: 1005,
        customerId: "bob@example.com",
        status: "cancelled",
        amount: 100.00,
        items: [
            { product: "Widget", qty: 2 }
        ]
    }
]);

print('Inserted ' + db.test_orders.countDocuments() + ' orders');

// Create indexes matching Oracle
db.test_orders.createIndex({ status: 1 });
db.test_orders.createIndex({ customerId: 1 });

// ============================================================
// Test Products Collection
// ============================================================
db.test_products.drop();
db.createCollection('test_products');

db.test_products.insertMany([
    {
        name: "Widget",
        price: 25.00,
        category: "tools",
        inStock: true
    },
    {
        name: "Gadget",
        price: 50.00,
        category: "electronics",
        inStock: true
    },
    {
        name: "Premium Widget",
        price: 500.00,
        category: "tools",
        inStock: false
    }
]);

print('Inserted ' + db.test_products.countDocuments() + ' products');

// ============================================================
// Validation Queries - Same pipelines we'll test against Oracle
// ============================================================

print('\n=== Validation Queries ===\n');

// Simple match
print('Active customers: ' + db.test_customers.countDocuments({ status: "active" }));

// Match with comparison
print('Customers age > 30: ' + db.test_customers.countDocuments({ age: { $gt: 30 } }));

// Aggregation: Group by status with count
var statusCounts = db.test_orders.aggregate([
    { $group: { _id: "$status", count: { $sum: 1 } } },
    { $sort: { count: -1 } }
]).toArray();
print('Orders by status: ' + JSON.stringify(statusCounts));

// Aggregation: Sum amounts by customer
var customerTotals = db.test_orders.aggregate([
    { $match: { status: "completed" } },
    { $group: { _id: "$customerId", totalAmount: { $sum: "$amount" } } },
    { $sort: { totalAmount: -1 } }
]).toArray();
print('Customer totals (completed): ' + JSON.stringify(customerTotals));

// Aggregation with $project
var projectedOrders = db.test_orders.aggregate([
    { $match: { status: "completed" } },
    { $project: { orderId: 1, amount: 1, _id: 0 } },
    { $limit: 3 }
]).toArray();
print('Projected orders: ' + JSON.stringify(projectedOrders));

print('\n=== MongoDB Setup Complete ===\n');
