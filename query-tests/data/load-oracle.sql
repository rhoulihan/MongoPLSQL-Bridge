-- Oracle Test Data Loader for MongoPLSQL-Bridge Query Validation
-- Usage: sqlplus translator/translator123@//localhost:1521/FREEPDB1 @load-oracle.sql

SET ECHO OFF
SET FEEDBACK OFF
SET PAGESIZE 0
SET LINESIZE 200

PROMPT ============================================================
PROMPT   Loading MongoPLSQL-Bridge Test Data into Oracle
PROMPT ============================================================
PROMPT

-- ============================================================
-- Drop existing tables
-- ============================================================
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE employees CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE products CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE customers CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE events CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE inventory CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

-- ============================================================
-- Create Sales Table
-- ============================================================
PROMPT Loading sales table...

CREATE TABLE sales (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO sales (id, data) VALUES ('S001', '{
    "_id": "S001",
    "orderId": 1001,
    "customerId": "C001",
    "customerName": "John Doe",
    "status": "completed",
    "category": "electronics",
    "region": "north",
    "amount": 150.00,
    "quantity": 2,
    "discount": 10,
    "tax": 15.00,
    "orderDate": "2024-01-15",
    "items": [
        {"product": "Widget", "qty": 1, "price": 100},
        {"product": "Gadget", "qty": 1, "price": 50}
    ],
    "tags": ["premium", "express"],
    "metadata": {"source": "web", "campaign": "winter-sale"}
}');

INSERT INTO sales (id, data) VALUES ('S002', '{
    "_id": "S002",
    "orderId": 1002,
    "customerId": "C002",
    "customerName": "Jane Smith",
    "status": "completed",
    "category": "electronics",
    "region": "south",
    "amount": 250.00,
    "quantity": 5,
    "discount": 25,
    "tax": 25.00,
    "orderDate": "2024-01-16",
    "items": [
        {"product": "Widget", "qty": 5, "price": 50}
    ],
    "tags": ["bulk"],
    "metadata": {"source": "mobile", "campaign": "winter-sale"}
}');

INSERT INTO sales (id, data) VALUES ('S003', '{
    "_id": "S003",
    "orderId": 1003,
    "customerId": "C001",
    "customerName": "John Doe",
    "status": "pending",
    "category": "clothing",
    "region": "north",
    "amount": 75.50,
    "quantity": 3,
    "discount": 0,
    "tax": 7.55,
    "orderDate": "2024-01-17",
    "items": [
        {"product": "Shirt", "qty": 2, "price": 25},
        {"product": "Pants", "qty": 1, "price": 25.50}
    ],
    "tags": [],
    "metadata": {"source": "web", "campaign": null}
}');

INSERT INTO sales (id, data) VALUES ('S004', '{
    "_id": "S004",
    "orderId": 1004,
    "customerId": "C003",
    "customerName": "Alice Brown",
    "status": "completed",
    "category": "electronics",
    "region": "east",
    "amount": 500.00,
    "quantity": 1,
    "discount": 50,
    "tax": 50.00,
    "orderDate": "2024-01-18",
    "items": [
        {"product": "Premium Widget", "qty": 1, "price": 500}
    ],
    "tags": ["premium", "vip"],
    "metadata": {"source": "store", "campaign": "vip-exclusive"}
}');

INSERT INTO sales (id, data) VALUES ('S005', '{
    "_id": "S005",
    "orderId": 1005,
    "customerId": "C004",
    "customerName": "Bob Wilson",
    "status": "cancelled",
    "category": "furniture",
    "region": "west",
    "amount": 1200.00,
    "quantity": 2,
    "discount": 100,
    "tax": 120.00,
    "orderDate": "2024-01-19",
    "items": [
        {"product": "Chair", "qty": 2, "price": 600}
    ],
    "tags": ["bulky"],
    "metadata": {"source": "web", "campaign": "furniture-fest"}
}');

INSERT INTO sales (id, data) VALUES ('S006', '{
    "_id": "S006",
    "orderId": 1006,
    "customerId": "C002",
    "customerName": "Jane Smith",
    "status": "completed",
    "category": "clothing",
    "region": "south",
    "amount": 89.99,
    "quantity": 4,
    "discount": 5,
    "tax": 9.00,
    "orderDate": "2024-01-20",
    "items": [
        {"product": "Socks", "qty": 4, "price": 22.50}
    ],
    "tags": ["clearance"],
    "metadata": {"source": "mobile", "campaign": "clearance"}
}');

INSERT INTO sales (id, data) VALUES ('S007', '{
    "_id": "S007",
    "orderId": 1007,
    "customerId": "C005",
    "customerName": "Charlie Green",
    "status": "pending",
    "category": "electronics",
    "region": "north",
    "amount": 0,
    "quantity": 0,
    "discount": 0,
    "tax": 0,
    "orderDate": "2024-01-21",
    "items": [],
    "tags": [],
    "metadata": {"source": "api", "campaign": null}
}');

INSERT INTO sales (id, data) VALUES ('S008', '{
    "_id": "S008",
    "orderId": 1008,
    "customerId": "C001",
    "customerName": "John Doe",
    "status": "refunded",
    "category": "electronics",
    "region": "north",
    "amount": -150.00,
    "quantity": -2,
    "discount": 0,
    "tax": -15.00,
    "orderDate": "2024-01-22",
    "items": [
        {"product": "Widget", "qty": -1, "price": 100},
        {"product": "Gadget", "qty": -1, "price": 50}
    ],
    "tags": ["refund"],
    "metadata": {"source": "support", "campaign": null}
}');

INSERT INTO sales (id, data) VALUES ('S009', '{
    "_id": "S009",
    "orderId": 1009,
    "customerId": "C006",
    "customerName": "Diana Prince",
    "status": "completed",
    "category": "jewelry",
    "region": "east",
    "amount": 9999.99,
    "quantity": 1,
    "discount": 500,
    "tax": 999.99,
    "orderDate": "2024-01-23",
    "items": [
        {"product": "Diamond Ring", "qty": 1, "price": 9999.99}
    ],
    "tags": ["luxury", "premium", "vip"],
    "metadata": {"source": "store", "campaign": "valentine"}
}');

INSERT INTO sales (id, data) VALUES ('S010', '{
    "_id": "S010",
    "orderId": 1010,
    "customerId": "C007",
    "customerName": "Eve Johnson",
    "status": "processing",
    "category": "electronics",
    "region": "west",
    "amount": 299.99,
    "quantity": 3,
    "discount": null,
    "tax": 30.00,
    "orderDate": "2024-01-24",
    "items": [
        {"product": "Headphones", "qty": 1, "price": 199.99},
        {"product": "Case", "qty": 2, "price": 50}
    ],
    "tags": ["new-customer"],
    "metadata": null
}');

PROMPT   Inserted 10 sales records

-- Create indexes
CREATE INDEX idx_sales_status ON sales(JSON_VALUE(data, '$.status'));
CREATE INDEX idx_sales_category ON sales(JSON_VALUE(data, '$.category'));
CREATE INDEX idx_sales_region ON sales(JSON_VALUE(data, '$.region'));
CREATE INDEX idx_sales_customer ON sales(JSON_VALUE(data, '$.customerId'));
CREATE INDEX idx_sales_amount ON sales(JSON_VALUE(data, '$.amount' RETURNING NUMBER));

-- ============================================================
-- Create Employees Table
-- ============================================================
PROMPT Loading employees table...

CREATE TABLE employees (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO employees (id, data) VALUES ('E001', '{"_id": "E001", "name": "Alice", "department": "Engineering", "team": "Backend", "salary": 95000, "bonus": 10000, "yearsOfService": 5, "active": true, "rating": 4.5}');
INSERT INTO employees (id, data) VALUES ('E002', '{"_id": "E002", "name": "Bob", "department": "Engineering", "team": "Frontend", "salary": 85000, "bonus": 8000, "yearsOfService": 3, "active": true, "rating": 4.0}');
INSERT INTO employees (id, data) VALUES ('E003', '{"_id": "E003", "name": "Carol", "department": "Engineering", "team": "Backend", "salary": 105000, "bonus": 15000, "yearsOfService": 7, "active": true, "rating": 4.8}');
INSERT INTO employees (id, data) VALUES ('E004', '{"_id": "E004", "name": "David", "department": "Sales", "team": "Enterprise", "salary": 75000, "bonus": 25000, "yearsOfService": 4, "active": true, "rating": 4.2}');
INSERT INTO employees (id, data) VALUES ('E005', '{"_id": "E005", "name": "Eve", "department": "Sales", "team": "SMB", "salary": 65000, "bonus": 15000, "yearsOfService": 2, "active": true, "rating": 3.8}');
INSERT INTO employees (id, data) VALUES ('E006', '{"_id": "E006", "name": "Frank", "department": "Sales", "team": "Enterprise", "salary": 80000, "bonus": 30000, "yearsOfService": 6, "active": false, "rating": 4.5}');
INSERT INTO employees (id, data) VALUES ('E007', '{"_id": "E007", "name": "Grace", "department": "Marketing", "team": "Digital", "salary": 70000, "bonus": 5000, "yearsOfService": 1, "active": true, "rating": 3.5}');
INSERT INTO employees (id, data) VALUES ('E008', '{"_id": "E008", "name": "Henry", "department": "Marketing", "team": "Content", "salary": 72000, "bonus": 6000, "yearsOfService": 2, "active": true, "rating": 4.0}');
INSERT INTO employees (id, data) VALUES ('E009', '{"_id": "E009", "name": "Ivy", "department": "Engineering", "team": "DevOps", "salary": 98000, "bonus": 12000, "yearsOfService": 4, "active": true, "rating": 4.3}');
INSERT INTO employees (id, data) VALUES ('E010', '{"_id": "E010", "name": "Jack", "department": "HR", "team": "Recruiting", "salary": 60000, "bonus": 3000, "yearsOfService": 1, "active": true, "rating": null}');

PROMPT   Inserted 10 employee records

CREATE INDEX idx_emp_dept ON employees(JSON_VALUE(data, '$.department'));
CREATE INDEX idx_emp_salary ON employees(JSON_VALUE(data, '$.salary' RETURNING NUMBER));
CREATE INDEX idx_emp_active ON employees(JSON_VALUE(data, '$.active'));

-- ============================================================
-- Create Products Table
-- ============================================================
PROMPT Loading products table...

CREATE TABLE products (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO products (id, data) VALUES ('P001', '{"_id": "P001", "name": "Widget", "category": "tools", "subcategory": "hand-tools", "price": 25.00, "cost": 10.00, "stock": 100, "active": true, "rating": 4.5, "tags": ["bestseller", "featured"]}');
INSERT INTO products (id, data) VALUES ('P002', '{"_id": "P002", "name": "Gadget", "category": "electronics", "subcategory": "accessories", "price": 50.00, "cost": 20.00, "stock": 75, "active": true, "rating": 4.2, "tags": ["new"]}');
INSERT INTO products (id, data) VALUES ('P003', '{"_id": "P003", "name": "Premium Widget", "category": "tools", "subcategory": "hand-tools", "price": 500.00, "cost": 200.00, "stock": 10, "active": true, "rating": 4.9, "tags": ["premium", "featured"]}');
INSERT INTO products (id, data) VALUES ('P004', '{"_id": "P004", "name": "Basic Widget", "category": "tools", "subcategory": "hand-tools", "price": 10.00, "cost": 5.00, "stock": 500, "active": true, "rating": 3.5, "tags": ["budget"]}');
INSERT INTO products (id, data) VALUES ('P005', '{"_id": "P005", "name": "Discontinued Item", "category": "misc", "subcategory": null, "price": 0, "cost": 15.00, "stock": 0, "active": false, "rating": null, "tags": []}');
INSERT INTO products (id, data) VALUES ('P006', '{"_id": "P006", "name": "Headphones", "category": "electronics", "subcategory": "audio", "price": 199.99, "cost": 80.00, "stock": 50, "active": true, "rating": 4.7, "tags": ["premium", "bestseller"]}');
INSERT INTO products (id, data) VALUES ('P007', '{"_id": "P007", "name": "USB Cable", "category": "electronics", "subcategory": "accessories", "price": 9.99, "cost": 2.00, "stock": 1000, "active": true, "rating": 4.0, "tags": ["essential"]}');
INSERT INTO products (id, data) VALUES ('P008', '{"_id": "P008", "name": "Chair", "category": "furniture", "subcategory": "office", "price": 299.99, "cost": 150.00, "stock": 25, "active": true, "rating": 4.4, "tags": ["ergonomic"]}');

PROMPT   Inserted 8 product records

CREATE INDEX idx_prod_category ON products(JSON_VALUE(data, '$.category'));
CREATE INDEX idx_prod_price ON products(JSON_VALUE(data, '$.price' RETURNING NUMBER));
CREATE INDEX idx_prod_active ON products(JSON_VALUE(data, '$.active'));

-- ============================================================
-- Create Customers Table (for $lookup tests)
-- ============================================================
PROMPT Loading customers table...

CREATE TABLE customers (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO customers (id, data) VALUES ('C001', '{"_id": "C001", "name": "John Doe", "email": "john.doe@example.com", "tier": "gold", "joinDate": "2023-06-15T10:30:00.000Z", "address": {"city": "New York", "state": "NY", "zip": "10001"}}');
INSERT INTO customers (id, data) VALUES ('C002', '{"_id": "C002", "name": "Jane Smith", "email": "jane.smith@example.com", "tier": "silver", "joinDate": "2023-08-20T14:45:00.000Z", "address": {"city": "Los Angeles", "state": "CA", "zip": "90001"}}');
INSERT INTO customers (id, data) VALUES ('C003', '{"_id": "C003", "name": "Alice Brown", "email": "alice.brown@example.com", "tier": "gold", "joinDate": "2023-03-10T09:00:00.000Z", "address": {"city": "Chicago", "state": "IL", "zip": "60601"}}');
INSERT INTO customers (id, data) VALUES ('C004', '{"_id": "C004", "name": "Bob Wilson", "email": "bob.wilson@example.com", "tier": "bronze", "joinDate": "2024-01-05T16:20:00.000Z", "address": {"city": "Houston", "state": "TX", "zip": "77001"}}');
INSERT INTO customers (id, data) VALUES ('C005', '{"_id": "C005", "name": "Charlie Green", "email": "charlie.green@example.com", "tier": "silver", "joinDate": "2023-11-30T11:15:00.000Z", "address": {"city": "Phoenix", "state": "AZ", "zip": "85001"}}');
INSERT INTO customers (id, data) VALUES ('C006', '{"_id": "C006", "name": "Diana Prince", "email": "diana.prince@example.com", "tier": "platinum", "joinDate": "2022-12-01T08:00:00.000Z", "address": {"city": "Miami", "state": "FL", "zip": "33101"}}');
INSERT INTO customers (id, data) VALUES ('C007', '{"_id": "C007", "name": "Eve Johnson", "email": "eve.johnson@example.com", "tier": "bronze", "joinDate": "2024-01-20T13:30:00.000Z", "address": {"city": "Seattle", "state": "WA", "zip": "98101"}}');

PROMPT   Inserted 7 customer records

CREATE INDEX idx_cust_tier ON customers(JSON_VALUE(data, '$.tier'));
CREATE INDEX idx_cust_state ON customers(JSON_VALUE(data, '$.address.state'));

-- ============================================================
-- Create Events Table (for date operator tests)
-- ============================================================
PROMPT Loading events table...

CREATE TABLE events (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO events (id, data) VALUES ('EV001', '{"_id": "EV001", "title": "Product Launch", "eventDate": "2024-03-15T14:30:00.000Z", "category": "marketing", "attendees": 150, "tags": ["launch", "product"]}');
INSERT INTO events (id, data) VALUES ('EV002', '{"_id": "EV002", "title": "Team Meeting", "eventDate": "2024-01-22T09:00:00.000Z", "category": "internal", "attendees": 25, "tags": ["recurring", "team"]}');
INSERT INTO events (id, data) VALUES ('EV003', '{"_id": "EV003", "title": "Customer Webinar", "eventDate": "2024-06-10T16:00:00.000Z", "category": "sales", "attendees": 500, "tags": ["webinar", "customers"]}');
INSERT INTO events (id, data) VALUES ('EV004', '{"_id": "EV004", "title": "Q1 Review", "eventDate": "2024-04-01T10:00:00.000Z", "category": "internal", "attendees": 50, "tags": ["quarterly", "review"]}');
INSERT INTO events (id, data) VALUES ('EV005', '{"_id": "EV005", "title": "Trade Show", "eventDate": "2024-09-20T08:00:00.000Z", "category": "marketing", "attendees": 1000, "tags": ["trade-show", "networking"]}');
INSERT INTO events (id, data) VALUES ('EV006', '{"_id": "EV006", "title": "Holiday Party", "eventDate": "2024-12-20T18:00:00.000Z", "category": "social", "attendees": 200, "tags": ["party", "annual"]}');
INSERT INTO events (id, data) VALUES ('EV007', '{"_id": "EV007", "title": "Training Session", "eventDate": "2024-02-28T13:00:00.000Z", "category": "internal", "attendees": 30, "tags": ["training", "onboarding"]}');
INSERT INTO events (id, data) VALUES ('EV008', '{"_id": "EV008", "title": "Board Meeting", "eventDate": "2024-07-15T11:00:00.000Z", "category": "executive", "attendees": 10, "tags": ["board", "quarterly"]}');

PROMPT   Inserted 8 event records

CREATE INDEX idx_event_category ON events(JSON_VALUE(data, '$.category'));
CREATE INDEX idx_event_date ON events(JSON_VALUE(data, '$.eventDate'));

-- ============================================================
-- Create Inventory Table (for $lookup with products)
-- ============================================================
PROMPT Loading inventory table...

CREATE TABLE inventory (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO inventory (id, data) VALUES ('INV001', '{"_id": "INV001", "productId": "P001", "warehouse": "WH-EAST", "quantity": 50, "lastRestocked": "2024-01-10"}');
INSERT INTO inventory (id, data) VALUES ('INV002', '{"_id": "INV002", "productId": "P001", "warehouse": "WH-WEST", "quantity": 50, "lastRestocked": "2024-01-12"}');
INSERT INTO inventory (id, data) VALUES ('INV003', '{"_id": "INV003", "productId": "P002", "warehouse": "WH-EAST", "quantity": 40, "lastRestocked": "2024-01-08"}');
INSERT INTO inventory (id, data) VALUES ('INV004', '{"_id": "INV004", "productId": "P002", "warehouse": "WH-CENTRAL", "quantity": 35, "lastRestocked": "2024-01-15"}');
INSERT INTO inventory (id, data) VALUES ('INV005', '{"_id": "INV005", "productId": "P003", "warehouse": "WH-EAST", "quantity": 5, "lastRestocked": "2024-01-05"}');
INSERT INTO inventory (id, data) VALUES ('INV006', '{"_id": "INV006", "productId": "P003", "warehouse": "WH-WEST", "quantity": 5, "lastRestocked": "2024-01-06"}');
INSERT INTO inventory (id, data) VALUES ('INV007', '{"_id": "INV007", "productId": "P004", "warehouse": "WH-CENTRAL", "quantity": 500, "lastRestocked": "2024-01-01"}');
INSERT INTO inventory (id, data) VALUES ('INV008', '{"_id": "INV008", "productId": "P006", "warehouse": "WH-EAST", "quantity": 25, "lastRestocked": "2024-01-18"}');
INSERT INTO inventory (id, data) VALUES ('INV009', '{"_id": "INV009", "productId": "P006", "warehouse": "WH-WEST", "quantity": 25, "lastRestocked": "2024-01-19"}');
INSERT INTO inventory (id, data) VALUES ('INV010', '{"_id": "INV010", "productId": "P007", "warehouse": "WH-CENTRAL", "quantity": 1000, "lastRestocked": "2024-01-20"}');
INSERT INTO inventory (id, data) VALUES ('INV011', '{"_id": "INV011", "productId": "P008", "warehouse": "WH-EAST", "quantity": 15, "lastRestocked": "2024-01-14"}');
INSERT INTO inventory (id, data) VALUES ('INV012', '{"_id": "INV012", "productId": "P008", "warehouse": "WH-WEST", "quantity": 10, "lastRestocked": "2024-01-16"}');

PROMPT   Inserted 12 inventory records

CREATE INDEX idx_inv_product ON inventory(JSON_VALUE(data, '$.productId'));
CREATE INDEX idx_inv_warehouse ON inventory(JSON_VALUE(data, '$.warehouse'));

COMMIT;

-- ============================================================
-- Summary
-- ============================================================
PROMPT
PROMPT ============================================================
PROMPT   Oracle Test Data Load Complete
PROMPT ============================================================

SET FEEDBACK ON
SELECT 'sales' AS table_name, COUNT(*) AS row_count FROM sales
UNION ALL
SELECT 'employees', COUNT(*) FROM employees
UNION ALL
SELECT 'products', COUNT(*) FROM products
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
UNION ALL
SELECT 'events', COUNT(*) FROM events
UNION ALL
SELECT 'inventory', COUNT(*) FROM inventory;

PROMPT ============================================================

EXIT;
