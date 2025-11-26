-- Setup script for MongoDB to Oracle Translator testing
-- This script runs automatically when the Oracle container starts

-- Create JSON collection table for testing
CREATE TABLE test_customers (
    id RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_orders (
    id RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE test_products (
    id RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
    data JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample customer data
INSERT INTO test_customers (data) VALUES
    ('{"name": "John Doe", "email": "john@example.com", "status": "active", "age": 30, "tier": "gold"}');
INSERT INTO test_customers (data) VALUES
    ('{"name": "Jane Smith", "email": "jane@example.com", "status": "active", "age": 25, "tier": "silver"}');
INSERT INTO test_customers (data) VALUES
    ('{"name": "Bob Wilson", "email": "bob@example.com", "status": "inactive", "age": 45, "tier": "bronze"}');
INSERT INTO test_customers (data) VALUES
    ('{"name": "Alice Brown", "email": "alice@example.com", "status": "active", "age": 35, "tier": "gold"}');

-- Insert sample order data
INSERT INTO test_orders (data) VALUES
    ('{"orderId": 1001, "customerId": "john@example.com", "status": "completed", "amount": 150.00, "items": [{"product": "Widget", "qty": 2}, {"product": "Gadget", "qty": 1}]}');
INSERT INTO test_orders (data) VALUES
    ('{"orderId": 1002, "customerId": "jane@example.com", "status": "completed", "amount": 250.00, "items": [{"product": "Widget", "qty": 5}]}');
INSERT INTO test_orders (data) VALUES
    ('{"orderId": 1003, "customerId": "john@example.com", "status": "pending", "amount": 75.00, "items": [{"product": "Gadget", "qty": 3}]}');
INSERT INTO test_orders (data) VALUES
    ('{"orderId": 1004, "customerId": "alice@example.com", "status": "completed", "amount": 500.00, "items": [{"product": "Premium Widget", "qty": 1}]}');
INSERT INTO test_orders (data) VALUES
    ('{"orderId": 1005, "customerId": "bob@example.com", "status": "cancelled", "amount": 100.00, "items": [{"product": "Widget", "qty": 2}]}');

-- Insert sample product data
INSERT INTO test_products (data) VALUES
    ('{"name": "Widget", "price": 25.00, "category": "tools", "inStock": true}');
INSERT INTO test_products (data) VALUES
    ('{"name": "Gadget", "price": 50.00, "category": "electronics", "inStock": true}');
INSERT INTO test_products (data) VALUES
    ('{"name": "Premium Widget", "price": 500.00, "category": "tools", "inStock": false}');

-- Create indexes for JSON paths commonly queried
CREATE INDEX idx_customers_status ON test_customers(JSON_VALUE(data, '$.status'));
CREATE INDEX idx_customers_email ON test_customers(JSON_VALUE(data, '$.email'));
CREATE INDEX idx_orders_status ON test_orders(JSON_VALUE(data, '$.status'));
CREATE INDEX idx_orders_customer ON test_orders(JSON_VALUE(data, '$.customerId'));

COMMIT;

-- Verify setup
SELECT 'Customers: ' || COUNT(*) AS setup_status FROM test_customers;
SELECT 'Orders: ' || COUNT(*) AS setup_status FROM test_orders;
SELECT 'Products: ' || COUNT(*) AS setup_status FROM test_products;
