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

INSERT INTO employees (id, data) VALUES ('E001', '{"_id": "E001", "name": "Alice", "department": "Engineering", "team": "Backend", "salary": 95000, "bonus": 10000, "yearsOfService": 5, "active": true, "rating": 4.5, "reportsTo": null}');
INSERT INTO employees (id, data) VALUES ('E002', '{"_id": "E002", "name": "Bob", "department": "Engineering", "team": "Frontend", "salary": 85000, "bonus": 8000, "yearsOfService": 3, "active": true, "rating": 4.0, "reportsTo": "E001"}');
INSERT INTO employees (id, data) VALUES ('E003', '{"_id": "E003", "name": "Carol", "department": "Engineering", "team": "Backend", "salary": 105000, "bonus": 15000, "yearsOfService": 7, "active": true, "rating": 4.8, "reportsTo": "E001"}');
INSERT INTO employees (id, data) VALUES ('E004', '{"_id": "E004", "name": "David", "department": "Sales", "team": "Enterprise", "salary": 75000, "bonus": 25000, "yearsOfService": 4, "active": true, "rating": 4.2, "reportsTo": "E002"}');
INSERT INTO employees (id, data) VALUES ('E005', '{"_id": "E005", "name": "Eve", "department": "Sales", "team": "SMB", "salary": 65000, "bonus": 15000, "yearsOfService": 2, "active": true, "rating": 3.8, "reportsTo": "E002"}');
INSERT INTO employees (id, data) VALUES ('E006', '{"_id": "E006", "name": "Frank", "department": "Sales", "team": "Enterprise", "salary": 80000, "bonus": 30000, "yearsOfService": 6, "active": false, "rating": 4.5, "reportsTo": "E003"}');
INSERT INTO employees (id, data) VALUES ('E007', '{"_id": "E007", "name": "Grace", "department": "Marketing", "team": "Digital", "salary": 70000, "bonus": 5000, "yearsOfService": 1, "active": true, "rating": 3.5, "reportsTo": "E003"}');
INSERT INTO employees (id, data) VALUES ('E008', '{"_id": "E008", "name": "Henry", "department": "Marketing", "team": "Content", "salary": 72000, "bonus": 6000, "yearsOfService": 2, "active": true, "rating": 4.0, "reportsTo": "E004"}');
INSERT INTO employees (id, data) VALUES ('E009', '{"_id": "E009", "name": "Ivy", "department": "Engineering", "team": "DevOps", "salary": 98000, "bonus": 12000, "yearsOfService": 4, "active": true, "rating": 4.3, "reportsTo": "E001"}');
INSERT INTO employees (id, data) VALUES ('E010', '{"_id": "E010", "name": "Jack", "department": "HR", "team": "Recruiting", "salary": 60000, "bonus": 3000, "yearsOfService": 1, "active": true, "rating": null, "reportsTo": "E009"}');

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
-- Create Org Chart Table (for $graphLookup - hierarchical data)
-- ============================================================
PROMPT Loading org_chart table...

CREATE TABLE org_chart (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO org_chart (id, data) VALUES ('CEO', '{"_id": "CEO", "name": "John Smith", "title": "CEO", "reportsTo": null, "department": "Executive"}');
INSERT INTO org_chart (id, data) VALUES ('CTO', '{"_id": "CTO", "name": "Jane Doe", "title": "CTO", "reportsTo": "CEO", "department": "Technology"}');
INSERT INTO org_chart (id, data) VALUES ('CFO', '{"_id": "CFO", "name": "Bob Wilson", "title": "CFO", "reportsTo": "CEO", "department": "Finance"}');
INSERT INTO org_chart (id, data) VALUES ('VP_ENG', '{"_id": "VP_ENG", "name": "Alice Brown", "title": "VP Engineering", "reportsTo": "CTO", "department": "Engineering"}');
INSERT INTO org_chart (id, data) VALUES ('VP_PROD', '{"_id": "VP_PROD", "name": "Charlie Davis", "title": "VP Product", "reportsTo": "CTO", "department": "Product"}');
INSERT INTO org_chart (id, data) VALUES ('DIR_BE', '{"_id": "DIR_BE", "name": "Diana Evans", "title": "Director Backend", "reportsTo": "VP_ENG", "department": "Engineering"}');
INSERT INTO org_chart (id, data) VALUES ('DIR_FE', '{"_id": "DIR_FE", "name": "Edward Foster", "title": "Director Frontend", "reportsTo": "VP_ENG", "department": "Engineering"}');
INSERT INTO org_chart (id, data) VALUES ('MGR_API', '{"_id": "MGR_API", "name": "Fiona Garcia", "title": "API Manager", "reportsTo": "DIR_BE", "department": "Engineering"}');
INSERT INTO org_chart (id, data) VALUES ('MGR_DB', '{"_id": "MGR_DB", "name": "George Harris", "title": "Database Manager", "reportsTo": "DIR_BE", "department": "Engineering"}');
INSERT INTO org_chart (id, data) VALUES ('DEV_1', '{"_id": "DEV_1", "name": "Helen Irving", "title": "Senior Developer", "reportsTo": "MGR_API", "department": "Engineering"}');
INSERT INTO org_chart (id, data) VALUES ('DEV_2', '{"_id": "DEV_2", "name": "Ivan Jones", "title": "Developer", "reportsTo": "MGR_API", "department": "Engineering"}');
INSERT INTO org_chart (id, data) VALUES ('DEV_3', '{"_id": "DEV_3", "name": "Julia King", "title": "Developer", "reportsTo": "MGR_DB", "department": "Engineering"}');

PROMPT   Inserted 12 org_chart records

CREATE INDEX idx_org_reportsTo ON org_chart(JSON_VALUE(data, '$.reportsTo'));
CREATE INDEX idx_org_dept ON org_chart(JSON_VALUE(data, '$.department'));

-- ============================================================
-- Create Orders Table (for $lookup tests with products)
-- ============================================================
PROMPT Loading orders table...

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE orders CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE orders (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO orders (id, data) VALUES ('O001', '{"_id": "O001", "customerId": "C001", "orderDate": {"$date": "2024-01-15T10:00:00.000Z"}, "status": "completed", "items": [{"productId": "P001", "quantity": 3}, {"productId": "P002", "quantity": 1}]}');
INSERT INTO orders (id, data) VALUES ('O002', '{"_id": "O002", "customerId": "C002", "orderDate": {"$date": "2024-01-16T14:30:00.000Z"}, "status": "completed", "items": [{"productId": "P003", "quantity": 1}, {"productId": "P007", "quantity": 5}]}');
INSERT INTO orders (id, data) VALUES ('O003', '{"_id": "O003", "customerId": "C001", "orderDate": {"$date": "2024-01-18T09:15:00.000Z"}, "status": "pending", "items": [{"productId": "P006", "quantity": 2}]}');
INSERT INTO orders (id, data) VALUES ('O004', '{"_id": "O004", "customerId": "C003", "orderDate": {"$date": "2024-01-20T16:00:00.000Z"}, "status": "completed", "items": [{"productId": "P004", "quantity": 10}, {"productId": "P001", "quantity": 2}, {"productId": "P002", "quantity": 3}]}');
INSERT INTO orders (id, data) VALUES ('O005', '{"_id": "O005", "customerId": "C004", "orderDate": {"$date": "2024-01-22T11:45:00.000Z"}, "status": "shipped", "items": [{"productId": "P008", "quantity": 1}]}');
INSERT INTO orders (id, data) VALUES ('O006', '{"_id": "O006", "customerId": "C002", "orderDate": {"$date": "2024-01-25T08:30:00.000Z"}, "status": "completed", "items": [{"productId": "P006", "quantity": 1}, {"productId": "P007", "quantity": 2}]}');
INSERT INTO orders (id, data) VALUES ('O007', '{"_id": "O007", "customerId": "C005", "orderDate": {"$date": "2024-02-01T13:00:00.000Z"}, "status": "completed", "items": [{"productId": "P001", "quantity": 5}, {"productId": "P004", "quantity": 20}]}');
INSERT INTO orders (id, data) VALUES ('O008', '{"_id": "O008", "customerId": "C006", "orderDate": {"$date": "2024-02-05T15:20:00.000Z"}, "status": "pending", "items": [{"productId": "P003", "quantity": 2}, {"productId": "P006", "quantity": 1}]}');

PROMPT   Inserted 8 orders records

CREATE INDEX idx_orders_customerId ON orders(JSON_VALUE(data, '$.customerId'));
CREATE INDEX idx_orders_status ON orders(JSON_VALUE(data, '$.status'));

COMMIT;

-- ============================================================
-- Create Commission Sales Table (for sales commission pipeline tests)
-- ============================================================
PROMPT Loading commission_sales table...

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE commission_sales CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE commission_sales (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO commission_sales (id, data) VALUES ('CS001', '{"_id": "CS001", "salesRepId": "SR001", "accountId": "ACC001", "amount": 75000, "saleDate": {"$date": "2024-10-15T10:00:00.000Z"}, "productCategory": "software", "dealType": "new", "contractTermMonths": 12}');
INSERT INTO commission_sales (id, data) VALUES ('CS002', '{"_id": "CS002", "salesRepId": "SR001", "accountId": "ACC002", "amount": 125000, "saleDate": {"$date": "2024-11-05T14:30:00.000Z"}, "productCategory": "services", "dealType": "expansion", "contractTermMonths": 24}');
INSERT INTO commission_sales (id, data) VALUES ('CS003', '{"_id": "CS003", "salesRepId": "SR002", "accountId": "ACC001", "amount": 50000, "saleDate": {"$date": "2024-10-20T09:00:00.000Z"}, "productCategory": "hardware", "dealType": "new", "contractTermMonths": 12}');
INSERT INTO commission_sales (id, data) VALUES ('CS004', '{"_id": "CS004", "salesRepId": "SR002", "accountId": "ACC003", "amount": 200000, "saleDate": {"$date": "2024-11-10T11:00:00.000Z"}, "productCategory": "software", "dealType": "renewal", "contractTermMonths": 36}');
INSERT INTO commission_sales (id, data) VALUES ('CS005', '{"_id": "CS005", "salesRepId": "SR003", "accountId": "ACC004", "amount": 80000, "saleDate": {"$date": "2024-10-25T16:00:00.000Z"}, "productCategory": "support", "dealType": "upsell", "contractTermMonths": 12}');
INSERT INTO commission_sales (id, data) VALUES ('CS006', '{"_id": "CS006", "salesRepId": "SR001", "accountId": "ACC005", "amount": 300000, "saleDate": {"$date": "2024-12-01T10:00:00.000Z"}, "productCategory": "software", "dealType": "new", "contractTermMonths": 24}');

PROMPT   Inserted 6 commission_sales records

CREATE INDEX idx_csales_rep ON commission_sales(JSON_VALUE(data, '$.salesRepId'));
CREATE INDEX idx_csales_account ON commission_sales(JSON_VALUE(data, '$.accountId'));

-- ============================================================
-- Create Sales Reps Table (for sales commission pipeline tests)
-- ============================================================
PROMPT Loading sales_reps table...

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE sales_reps CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE sales_reps (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO sales_reps (id, data) VALUES ('SR001', '{"_id": "SR001", "name": {"first": "Sarah", "last": "Johnson"}, "email": "sarah.johnson@company.com", "region": "AMER-WEST", "territory": "California", "tier": "principal", "hireDate": {"$date": "2020-03-15T00:00:00.000Z"}, "quota": {"quarterly": 400000, "annual": 1600000}, "managerId": null}');
INSERT INTO sales_reps (id, data) VALUES ('SR002', '{"_id": "SR002", "name": {"first": "Mike", "last": "Chen"}, "email": "mike.chen@company.com", "region": "AMER-EAST", "territory": "New York", "tier": "senior", "hireDate": {"$date": "2021-06-01T00:00:00.000Z"}, "quota": {"quarterly": 300000, "annual": 1200000}, "managerId": "SR001"}');
INSERT INTO sales_reps (id, data) VALUES ('SR003', '{"_id": "SR003", "name": {"first": "Emily", "last": "Brown"}, "email": "emily.brown@company.com", "region": "EMEA", "territory": "UK", "tier": "associate", "hireDate": {"$date": "2023-01-10T00:00:00.000Z"}, "quota": {"quarterly": 200000, "annual": 800000}, "managerId": "SR001"}');

PROMPT   Inserted 3 sales_reps records

CREATE INDEX idx_reps_region ON sales_reps(JSON_VALUE(data, '$.region'));

-- ============================================================
-- Create Commission Accounts Table (for sales commission pipeline tests)
-- ============================================================
PROMPT Loading commission_accounts table...

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE commission_accounts CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE commission_accounts (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO commission_accounts (id, data) VALUES ('ACC001', '{"_id": "ACC001", "name": "TechCorp Inc", "accountType": "enterprise", "industrySector": "technology", "industryVertical": "cloud", "arr": 500000, "employeeCount": 5000, "headquarters": {"country": "USA", "state": "CA", "city": "San Francisco"}}');
INSERT INTO commission_accounts (id, data) VALUES ('ACC002', '{"_id": "ACC002", "name": "FinanceFirst Bank", "accountType": "enterprise", "industrySector": "financial_services", "industryVertical": "banking", "arr": 750000, "employeeCount": 10000, "headquarters": {"country": "USA", "state": "NY", "city": "New York"}}');
INSERT INTO commission_accounts (id, data) VALUES ('ACC003', '{"_id": "ACC003", "name": "HealthPlus Medical", "accountType": "commercial", "industrySector": "healthcare", "industryVertical": "hospitals", "arr": 250000, "employeeCount": 2000, "headquarters": {"country": "USA", "state": "TX", "city": "Houston"}}');
INSERT INTO commission_accounts (id, data) VALUES ('ACC004', '{"_id": "ACC004", "name": "RetailMax Stores", "accountType": "commercial", "industrySector": "retail", "industryVertical": "e-commerce", "arr": 150000, "employeeCount": 500, "headquarters": {"country": "UK", "state": null, "city": "London"}}');
INSERT INTO commission_accounts (id, data) VALUES ('ACC005', '{"_id": "ACC005", "name": "MegaSoft Solutions", "accountType": "enterprise", "industrySector": "technology", "industryVertical": "software", "arr": 1000000, "employeeCount": 8000, "headquarters": {"country": "USA", "state": "WA", "city": "Seattle"}}');

PROMPT   Inserted 5 commission_accounts records

CREATE INDEX idx_acct_type ON commission_accounts(JSON_VALUE(data, '$.accountType'));
CREATE INDEX idx_acct_sector ON commission_accounts(JSON_VALUE(data, '$.industrySector'));

-- ============================================================
-- Create Commission Schedule Table (for sales commission pipeline tests)
-- ============================================================
PROMPT Loading commission_schedule table...

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE commission_schedule CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE commission_schedule (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO commission_schedule (id, data) VALUES ('SCHED001', '{"_id": "SCHED001", "accountType": "enterprise", "effectiveDate": {"$date": "2024-01-01T00:00:00.000Z"}, "expirationDate": null, "tiers": [{"tierName": "base", "minVolume": 0, "maxVolume": 250000, "baseRate": 0.06, "accelerator": 1.0}, {"tierName": "achiever", "minVolume": 250000, "maxVolume": 500000, "baseRate": 0.08, "accelerator": 1.25}, {"tierName": "performer", "minVolume": 500000, "maxVolume": 1000000, "baseRate": 0.10, "accelerator": 1.5}, {"tierName": "elite", "minVolume": 1000000, "maxVolume": 999999999, "baseRate": 0.12, "accelerator": 2.0}], "productMultipliers": {"software": 1.0, "hardware": 0.6, "services": 1.2, "support": 0.5}, "dealTypeMultipliers": {"new": 1.0, "renewal": 0.5, "expansion": 0.8, "upsell": 0.9}}');
INSERT INTO commission_schedule (id, data) VALUES ('SCHED002', '{"_id": "SCHED002", "accountType": "commercial", "effectiveDate": {"$date": "2024-01-01T00:00:00.000Z"}, "expirationDate": null, "tiers": [{"tierName": "base", "minVolume": 0, "maxVolume": 100000, "baseRate": 0.07, "accelerator": 1.0}, {"tierName": "achiever", "minVolume": 100000, "maxVolume": 250000, "baseRate": 0.09, "accelerator": 1.25}, {"tierName": "performer", "minVolume": 250000, "maxVolume": 500000, "baseRate": 0.11, "accelerator": 1.5}, {"tierName": "elite", "minVolume": 500000, "maxVolume": 999999999, "baseRate": 0.13, "accelerator": 2.0}], "productMultipliers": {"software": 1.0, "hardware": 0.6, "services": 1.2, "support": 0.5}, "dealTypeMultipliers": {"new": 1.0, "renewal": 0.5, "expansion": 0.8, "upsell": 0.9}}');

PROMPT   Inserted 2 commission_schedule records

CREATE INDEX idx_sched_type ON commission_schedule(JSON_VALUE(data, '$.accountType'));

-- ============================================================
-- Create MongoDB Founders Table (for $graphLookup tests - from MongoDB official docs)
-- ============================================================
PROMPT Loading mongodb_founders table...

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE mongodb_founders CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE mongodb_founders (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO mongodb_founders (id, data) VALUES ('1', '{"_id": 1, "name": "Dev"}');
INSERT INTO mongodb_founders (id, data) VALUES ('2', '{"_id": 2, "name": "Eliot", "reportsTo": "Dev"}');
INSERT INTO mongodb_founders (id, data) VALUES ('3', '{"_id": 3, "name": "Ron", "reportsTo": "Eliot"}');
INSERT INTO mongodb_founders (id, data) VALUES ('4', '{"_id": 4, "name": "Andrew", "reportsTo": "Eliot"}');
INSERT INTO mongodb_founders (id, data) VALUES ('5', '{"_id": 5, "name": "Asya", "reportsTo": "Ron"}');
INSERT INTO mongodb_founders (id, data) VALUES ('6', '{"_id": 6, "name": "Dan", "reportsTo": "Andrew"}');

PROMPT   Inserted 6 mongodb_founders records

CREATE INDEX idx_founders_name ON mongodb_founders(JSON_VALUE(data, '$.name'));

-- ============================================================
-- Create Graph Start Table (starting point documents for $graphLookup tests)
-- ============================================================
PROMPT Loading graph_start table...

BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE graph_start CASCADE CONSTRAINTS';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE graph_start (
    id VARCHAR2(50) PRIMARY KEY,
    data JSON
);

INSERT INTO graph_start (id, data) VALUES ('1', '{"_id": 1, "x": "Andrew"}');
INSERT INTO graph_start (id, data) VALUES ('2', '{"_id": 2, "x": "Dan"}');
INSERT INTO graph_start (id, data) VALUES ('3', '{"_id": 3, "x": ["Dev", "Eliot"]}');

PROMPT   Inserted 3 graph_start records

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
SELECT 'inventory', COUNT(*) FROM inventory
UNION ALL
SELECT 'org_chart', COUNT(*) FROM org_chart
UNION ALL
SELECT 'orders', COUNT(*) FROM orders
UNION ALL
SELECT 'commission_sales', COUNT(*) FROM commission_sales
UNION ALL
SELECT 'sales_reps', COUNT(*) FROM sales_reps
UNION ALL
SELECT 'commission_accounts', COUNT(*) FROM commission_accounts
UNION ALL
SELECT 'commission_schedule', COUNT(*) FROM commission_schedule
UNION ALL
SELECT 'mongodb_founders', COUNT(*) FROM mongodb_founders
UNION ALL
SELECT 'graph_start', COUNT(*) FROM graph_start;

PROMPT ============================================================

EXIT;

-- =====================================================
-- Additional collections for comprehensive test coverage
-- =====================================================

-- orders_detailed collection (10 documents)
BEGIN EXECUTE IMMEDIATE 'DROP TABLE ORDERS_DETAILED CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE ORDERS_DETAILED (
  id VARCHAR2(100) PRIMARY KEY,
  data JSON
);

INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD001', '{"_id":"ORD001","orderNumber":"ON-2024-00001","customerId":"C001","orderDate":{"$date":"2024-01-15T10:30:00.000Z"},"status":"delivered","shippingAddress":{"street":"123 Main St","city":"New York","state":"NY","zip":"10001","country":"USA"},"lineItems":[{"sku":"SKU-001","productName":"Wireless Headphones","category":"electronics","quantity":2,"unitPrice":79.99,"discount":10,"variants":{"color":"black"}},{"sku":"SKU-002","productName":"USB-C Cable","category":"accessories","quantity":3,"unitPrice":12.99,"discount":0,"variants":{"color":"white"}}],"payment":{"method":"credit_card","last4":"4242","amount":182.95,"currency":"USD"},"fulfillment":{"warehouse":"WH-EAST","shippedDate":{"$date":"2024-01-16T14:00:00.000Z"},"carrier":"UPS","trackingNumber":"1Z999AA10123456784"}}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD002', '{"_id":"ORD002","orderNumber":"ON-2024-00002","customerId":"C002","orderDate":{"$date":"2024-01-18T14:45:00.000Z"},"status":"delivered","shippingAddress":{"street":"456 Oak Ave","city":"Los Angeles","state":"CA","zip":"90001","country":"USA"},"lineItems":[{"sku":"SKU-004","productName":"Mechanical Keyboard","category":"electronics","quantity":1,"unitPrice":129.99,"discount":15,"variants":{"color":"silver"}}],"payment":{"method":"paypal","last4":null,"amount":110.49,"currency":"USD"},"fulfillment":{"warehouse":"WH-WEST","shippedDate":{"$date":"2024-01-19T09:30:00.000Z"},"carrier":"FedEx","trackingNumber":"794644790149"}}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD003', '{"_id":"ORD003","orderNumber":"ON-2024-00003","customerId":"C001","orderDate":{"$date":"2024-02-05T09:15:00.000Z"},"status":"shipped","shippingAddress":{"street":"123 Main St","city":"New York","state":"NY","zip":"10001","country":"USA"},"lineItems":[{"sku":"SKU-003","productName":"Laptop Stand","category":"accessories","quantity":1,"unitPrice":45.0,"discount":0,"variants":{"color":"black"}},{"sku":"SKU-005","productName":"Mouse Pad","category":"accessories","quantity":2,"unitPrice":15.99,"discount":5,"variants":{"color":"blue"}}],"payment":{"method":"credit_card","last4":"1234","amount":75.38,"currency":"USD"},"fulfillment":{"warehouse":"WH-EAST","shippedDate":{"$date":"2024-02-07T11:00:00.000Z"},"carrier":"UPS","trackingNumber":"1Z999AA10234567891"}}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD004', '{"_id":"ORD004","orderNumber":"ON-2024-00004","customerId":"C003","orderDate":{"$date":"2024-02-10T16:20:00.000Z"},"status":"pending","shippingAddress":{"street":"789 Pine Rd","city":"Chicago","state":"IL","zip":"60601","country":"USA"},"lineItems":[{"sku":"SKU-006","productName":"Monitor Light","category":"electronics","quantity":1,"unitPrice":59.99,"discount":0,"variants":{"color":"black"}},{"sku":"SKU-007","productName":"Desk Organizer","category":"office","quantity":1,"unitPrice":24.99,"discount":0,"variants":{"color":"white"}}],"payment":{"method":"debit_card","last4":"5678","amount":84.98,"currency":"USD"},"fulfillment":null}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD005', '{"_id":"ORD005","orderNumber":"ON-2024-00005","customerId":"C004","orderDate":{"$date":"2024-02-15T11:30:00.000Z"},"status":"cancelled","shippingAddress":{"street":"321 Elm St","city":"Houston","state":"TX","zip":"77001","country":"USA"},"lineItems":[{"sku":"SKU-008","productName":"Webcam","category":"electronics","quantity":1,"unitPrice":89.99,"discount":0,"variants":{"color":"black"}}],"payment":{"method":"credit_card","last4":"9012","amount":0,"currency":"USD"},"fulfillment":null}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD006', '{"_id":"ORD006","orderNumber":"ON-2024-00006","customerId":"C002","orderDate":{"$date":"2024-03-01T08:00:00.000Z"},"status":"delivered","shippingAddress":{"street":"456 Oak Ave","city":"Los Angeles","state":"CA","zip":"90001","country":"USA"},"lineItems":[{"sku":"SKU-001","productName":"Wireless Headphones","category":"electronics","quantity":1,"unitPrice":79.99,"discount":20,"variants":{"color":"white"}},{"sku":"SKU-003","productName":"Laptop Stand","category":"accessories","quantity":2,"unitPrice":45.0,"discount":10,"variants":{"color":"silver"}}],"payment":{"method":"credit_card","last4":"3456","amount":144.99,"currency":"USD"},"fulfillment":{"warehouse":"WH-WEST","shippedDate":{"$date":"2024-03-02T10:15:00.000Z"},"carrier":"USPS","trackingNumber":"9400111899223100001234"}}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD007', '{"_id":"ORD007","orderNumber":"ON-2024-00007","customerId":"C005","orderDate":{"$date":"2024-03-10T13:45:00.000Z"},"status":"delivered","shippingAddress":{"street":"555 Maple Dr","city":"Phoenix","state":"AZ","zip":"85001","country":"USA"},"lineItems":[{"sku":"SKU-002","productName":"USB-C Cable","category":"accessories","quantity":5,"unitPrice":12.99,"discount":0,"variants":{"color":"black"}}],"payment":{"method":"paypal","last4":null,"amount":64.95,"currency":"USD"},"fulfillment":{"warehouse":"WH-CENTRAL","shippedDate":{"$date":"2024-03-11T08:30:00.000Z"},"carrier":"DHL","trackingNumber":"1234567890"}}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD008', '{"_id":"ORD008","orderNumber":"ON-2024-00008","customerId":"C006","orderDate":{"$date":"2024-03-20T15:00:00.000Z"},"status":"processing","shippingAddress":{"street":"777 Beach Blvd","city":"Miami","state":"FL","zip":"33101","country":"USA"},"lineItems":[{"sku":"SKU-004","productName":"Mechanical Keyboard","category":"electronics","quantity":2,"unitPrice":129.99,"discount":5,"variants":{"color":"black"}},{"sku":"SKU-005","productName":"Mouse Pad","category":"accessories","quantity":2,"unitPrice":15.99,"discount":0,"variants":{"color":"red"}}],"payment":{"method":"credit_card","last4":"7890","amount":278.96,"currency":"USD"},"fulfillment":null}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD009', '{"_id":"ORD009","orderNumber":"ON-2024-00009","customerId":"C007","orderDate":{"$date":"2024-04-01T10:00:00.000Z"},"status":"delivered","shippingAddress":{"street":"888 Rain St","city":"Seattle","state":"WA","zip":"98101","country":"USA"},"lineItems":[{"sku":"SKU-006","productName":"Monitor Light","category":"electronics","quantity":1,"unitPrice":59.99,"discount":10,"variants":{"color":"white"}}],"payment":{"method":"debit_card","last4":"2345","amount":53.99,"currency":"USD"},"fulfillment":{"warehouse":"WH-WEST","shippedDate":{"$date":"2024-04-02T14:00:00.000Z"},"carrier":"UPS","trackingNumber":"1Z999AA10345678901"}}');
INSERT INTO ORDERS_DETAILED (id, data) VALUES ('ORD010', '{"_id":"ORD010","orderNumber":"ON-2024-00010","customerId":"C001","orderDate":{"$date":"2024-04-15T12:30:00.000Z"},"status":"delivered","shippingAddress":{"street":"123 Main St","city":"New York","state":"NY","zip":"10001","country":"USA"},"lineItems":[{"sku":"SKU-008","productName":"Webcam","category":"electronics","quantity":1,"unitPrice":89.99,"discount":0,"variants":{"color":"black"}},{"sku":"SKU-007","productName":"Desk Organizer","category":"office","quantity":1,"unitPrice":24.99,"discount":0,"variants":{"color":"black"}}],"payment":{"method":"credit_card","last4":"4242","amount":114.98,"currency":"USD"},"fulfillment":{"warehouse":"WH-EAST","shippedDate":{"$date":"2024-04-16T09:00:00.000Z"},"carrier":"FedEx","trackingNumber":"794644790250"}}');

-- purchase_orders collection (15 documents)
BEGIN EXECUTE IMMEDIATE 'DROP TABLE PURCHASE_ORDERS CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE PURCHASE_ORDERS (
  id VARCHAR2(100) PRIMARY KEY,
  data JSON
);

INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO001', '{"_id":"PO001","locationNumber":"LOC001","docRefNumber":"DOC-2024-001","customerPONumber":"PO-A001","orderedDate":{"$date":"2024-03-15T10:00:00.000Z"},"amount":1500.0,"status":"completed"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO002', '{"_id":"PO002","locationNumber":"LOC001","docRefNumber":"DOC-2024-002","customerPONumber":"PO-A002","orderedDate":{"$date":"2024-04-10T14:30:00.000Z"},"amount":2500.0,"status":"completed"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO003', '{"_id":"PO003","locationNumber":"LOC001","docRefNumber":"DOC-2024-003","customerPONumber":"PO-A003","orderedDate":{"$date":"2024-05-20T09:15:00.000Z"},"amount":750.0,"status":"pending"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO004', '{"_id":"PO004","locationNumber":"LOC001","docRefNumber":"DOC-2024-004","customerPONumber":"PO-A004","orderedDate":{"$date":"2024-06-05T11:45:00.000Z"},"amount":3200.0,"status":"completed"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO005', '{"_id":"PO005","locationNumber":"LOC001","docRefNumber":"DOC-2024-005","customerPONumber":"PO-A005","orderedDate":{"$date":"2024-07-12T16:00:00.000Z"},"amount":890.0,"status":"pending"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO006', '{"_id":"PO006","locationNumber":"LOC001","docRefNumber":"DOC-2024-001","customerPONumber":"PO-A001","orderedDate":{"$date":"2024-03-15T10:00:00.000Z"},"amount":500.0,"status":"completed"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO007', '{"_id":"PO007","locationNumber":"LOC002","docRefNumber":"DOC-2024-006","customerPONumber":"PO-B001","orderedDate":{"$date":"2024-04-22T13:00:00.000Z"},"amount":4100.0,"status":"completed"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO008', '{"_id":"PO008","locationNumber":"LOC001","docRefNumber":"DOC-2024-007","customerPONumber":"PO-A006","orderedDate":{"$date":"2024-08-01T08:30:00.000Z"},"amount":1750.0,"status":"shipped"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO009', '{"_id":"PO009","locationNumber":"LOC001","docRefNumber":"DOC-2024-008","customerPONumber":"PO-A007","orderedDate":{"$date":"2024-09-18T15:20:00.000Z"},"amount":620.0,"status":"pending"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO010', '{"_id":"PO010","locationNumber":"LOC001","docRefNumber":"DOC-2024-009","customerPONumber":"PO-A008","orderedDate":{"$date":"2024-10-25T10:45:00.000Z"},"amount":2100.0,"status":"completed"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO011', '{"_id":"PO011","locationNumber":"LOC003","docRefNumber":"DOC-2024-010","customerPONumber":"PO-C001","orderedDate":{"$date":"2024-11-05T12:00:00.000Z"},"amount":5500.0,"status":"completed"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO012', '{"_id":"PO012","locationNumber":"LOC001","docRefNumber":"DOC-2024-010","customerPONumber":"PO-A009","orderedDate":{"$date":"2024-11-20T09:00:00.000Z"},"amount":980.0,"status":"pending"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO013', '{"_id":"PO013","locationNumber":"LOC001","docRefNumber":"DOC-2024-011","customerPONumber":"PO-A010","orderedDate":{"$date":"2025-01-08T14:15:00.000Z"},"amount":3400.0,"status":"completed"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO014', '{"_id":"PO014","locationNumber":"LOC001","docRefNumber":"DOC-2024-012","customerPONumber":"PO-A011","orderedDate":{"$date":"2025-02-14T11:30:00.000Z"},"amount":1200.0,"status":"shipped"}');
INSERT INTO PURCHASE_ORDERS (id, data) VALUES ('PO015', '{"_id":"PO015","locationNumber":"LOC002","docRefNumber":"DOC-2024-013","customerPONumber":"PO-B002","orderedDate":{"$date":"2025-03-01T16:45:00.000Z"},"amount":7800.0,"status":"completed"}');

-- page_views collection (15 documents)
BEGIN EXECUTE IMMEDIATE 'DROP TABLE PAGE_VIEWS CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE PAGE_VIEWS (
  id VARCHAR2(100) PRIMARY KEY,
  data JSON
);

INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV001', '{"_id":"PV001","sessionId":"sess_001","userId":"user_001","timestamp":{"$date":"2024-01-15T10:30:00.000Z"},"page":"/","referrer":null,"device":{"type":"desktop","os":"Windows","browser":"Chrome"},"duration":45,"events":[{"type":"scroll","depth":75}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV002', '{"_id":"PV002","sessionId":"sess_001","userId":"user_001","timestamp":{"$date":"2024-01-15T10:31:00.000Z"},"page":"/products","referrer":"/","device":{"type":"desktop","os":"Windows","browser":"Chrome"},"duration":120,"events":[{"type":"scroll","depth":90},{"type":"click","element":"product-card"}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV003', '{"_id":"PV003","sessionId":"sess_001","userId":"user_001","timestamp":{"$date":"2024-01-15T10:35:00.000Z"},"page":"/products/headphones","referrer":"/products","device":{"type":"desktop","os":"Windows","browser":"Chrome"},"duration":180,"events":[{"type":"scroll","depth":100},{"type":"click","element":"add-to-cart"}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV004', '{"_id":"PV004","sessionId":"sess_001","userId":"user_001","timestamp":{"$date":"2024-01-15T10:40:00.000Z"},"page":"/cart","referrer":"/products/headphones","device":{"type":"desktop","os":"Windows","browser":"Chrome"},"duration":60,"events":[{"type":"click","element":"checkout-btn"}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV005', '{"_id":"PV005","sessionId":"sess_001","userId":"user_001","timestamp":{"$date":"2024-01-15T10:42:00.000Z"},"page":"/checkout","referrer":"/cart","device":{"type":"desktop","os":"Windows","browser":"Chrome"},"duration":300,"events":[{"type":"click","element":"place-order"}],"isConversion":true}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV006', '{"_id":"PV006","sessionId":"sess_002","userId":"user_002","timestamp":{"$date":"2024-01-15T14:00:00.000Z"},"page":"/","referrer":null,"device":{"type":"mobile","os":"iOS","browser":"Safari"},"duration":30,"events":[],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV007', '{"_id":"PV007","sessionId":"sess_002","userId":"user_002","timestamp":{"$date":"2024-01-15T14:01:00.000Z"},"page":"/products","referrer":"/","device":{"type":"mobile","os":"iOS","browser":"Safari"},"duration":90,"events":[{"type":"scroll","depth":50}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV008', '{"_id":"PV008","sessionId":"sess_003","userId":null,"timestamp":{"$date":"2024-01-16T09:00:00.000Z"},"page":"/search","referrer":null,"device":{"type":"desktop","os":"macOS","browser":"Safari"},"duration":45,"events":[{"type":"click","element":"search-box"}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV009', '{"_id":"PV009","sessionId":"sess_003","userId":null,"timestamp":{"$date":"2024-01-16T09:02:00.000Z"},"page":"/products/keyboards","referrer":"/search","device":{"type":"desktop","os":"macOS","browser":"Safari"},"duration":150,"events":[{"type":"scroll","depth":80}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV010', '{"_id":"PV010","sessionId":"sess_004","userId":"user_003","timestamp":{"$date":"2024-01-17T11:30:00.000Z"},"page":"/account","referrer":null,"device":{"type":"tablet","os":"iPadOS","browser":"Safari"},"duration":60,"events":[],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV011', '{"_id":"PV011","sessionId":"sess_005","userId":"user_001","timestamp":{"$date":"2024-01-18T16:00:00.000Z"},"page":"/","referrer":null,"device":{"type":"mobile","os":"Android","browser":"Chrome"},"duration":25,"events":[{"type":"scroll","depth":30}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV012', '{"_id":"PV012","sessionId":"sess_005","userId":"user_001","timestamp":{"$date":"2024-01-18T16:02:00.000Z"},"page":"/products/accessories","referrer":"/","device":{"type":"mobile","os":"Android","browser":"Chrome"},"duration":100,"events":[{"type":"scroll","depth":60},{"type":"click","element":"filter-btn"}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV013', '{"_id":"PV013","sessionId":"sess_006","userId":"user_004","timestamp":{"$date":"2024-01-20T10:15:00.000Z"},"page":"/checkout","referrer":"/cart","device":{"type":"desktop","os":"Windows","browser":"Firefox"},"duration":240,"events":[{"type":"click","element":"place-order"}],"isConversion":true}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV014', '{"_id":"PV014","sessionId":"sess_007","userId":null,"timestamp":{"$date":"2024-01-22T08:30:00.000Z"},"page":"/about","referrer":null,"device":{"type":"desktop","os":"Linux","browser":"Firefox"},"duration":90,"events":[{"type":"scroll","depth":100}],"isConversion":false}');
INSERT INTO PAGE_VIEWS (id, data) VALUES ('PV015', '{"_id":"PV015","sessionId":"sess_008","userId":"user_005","timestamp":{"$date":"2024-01-25T19:00:00.000Z"},"page":"/products","referrer":"/","device":{"type":"mobile","os":"iOS","browser":"Safari"},"duration":200,"events":[{"type":"scroll","depth":95},{"type":"click","element":"product-card"}],"isConversion":false}');

-- support_tickets collection (10 documents)
BEGIN EXECUTE IMMEDIATE 'DROP TABLE SUPPORT_TICKETS CASCADE CONSTRAINTS'; EXCEPTION WHEN OTHERS THEN NULL; END;
/
CREATE TABLE SUPPORT_TICKETS (
  id VARCHAR2(100) PRIMARY KEY,
  data JSON
);

INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT001', '{"_id":"TKT001","ticketNumber":"CS-2024-00001","customerId":"C001","createdAt":{"$date":"2024-01-15T10:30:00.000Z"},"resolvedAt":{"$date":"2024-01-15T14:45:00.000Z"},"status":"resolved","priority":"high","category":"billing","subcategory":"refund_request","assignedTo":"agent_001","escalations":[{"level":1,"timestamp":{"$date":"2024-01-15T11:00:00.000Z"},"reason":"no_response"}],"messages":[{"from":"customer","timestamp":{"$date":"2024-01-15T10:30:00.000Z"},"text":"Need refund for order"},{"from":"agent","timestamp":{"$date":"2024-01-15T10:45:00.000Z"},"text":"Processing your request"}],"satisfaction":{"score":4,"feedback":"Quick resolution"}}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT002', '{"_id":"TKT002","ticketNumber":"CS-2024-00002","customerId":"C002","createdAt":{"$date":"2024-01-16T09:00:00.000Z"},"resolvedAt":{"$date":"2024-01-16T11:30:00.000Z"},"status":"resolved","priority":"medium","category":"technical","subcategory":"bug_report","assignedTo":"agent_002","escalations":[],"messages":[{"from":"customer","timestamp":{"$date":"2024-01-16T09:00:00.000Z"},"text":"Website not loading"},{"from":"agent","timestamp":{"$date":"2024-01-16T09:15:00.000Z"},"text":"We are investigating"}],"satisfaction":{"score":5,"feedback":"Very helpful"}}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT003', '{"_id":"TKT003","ticketNumber":"CS-2024-00003","customerId":"C003","createdAt":{"$date":"2024-01-17T14:00:00.000Z"},"resolvedAt":null,"status":"open","priority":"low","category":"shipping","subcategory":"delayed","assignedTo":"agent_003","escalations":[],"messages":[{"from":"customer","timestamp":{"$date":"2024-01-17T14:00:00.000Z"},"text":"Where is my order?"}],"satisfaction":null}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT004', '{"_id":"TKT004","ticketNumber":"CS-2024-00004","customerId":"C001","createdAt":{"$date":"2024-01-18T08:30:00.000Z"},"resolvedAt":{"$date":"2024-01-19T16:00:00.000Z"},"status":"resolved","priority":"urgent","category":"billing","subcategory":"payment_failed","assignedTo":"agent_001","escalations":[{"level":1,"timestamp":{"$date":"2024-01-18T09:30:00.000Z"},"reason":"sla_breach"},{"level":2,"timestamp":{"$date":"2024-01-18T14:00:00.000Z"},"reason":"customer_request"}],"messages":[{"from":"customer","timestamp":{"$date":"2024-01-18T08:30:00.000Z"},"text":"Payment keeps failing"},{"from":"agent","timestamp":{"$date":"2024-01-18T08:45:00.000Z"},"text":"Let me check your account"},{"from":"agent","timestamp":{"$date":"2024-01-19T16:00:00.000Z"},"text":"Issue resolved"}],"satisfaction":{"score":3,"feedback":"Took too long"}}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT005', '{"_id":"TKT005","ticketNumber":"CS-2024-00005","customerId":"C004","createdAt":{"$date":"2024-01-20T11:00:00.000Z"},"resolvedAt":{"$date":"2024-01-20T12:00:00.000Z"},"status":"resolved","priority":"medium","category":"account","subcategory":"password_reset","assignedTo":"agent_004","escalations":[],"messages":[{"from":"customer","timestamp":{"$date":"2024-01-20T11:00:00.000Z"},"text":"Cannot reset password"},{"from":"agent","timestamp":{"$date":"2024-01-20T11:10:00.000Z"},"text":"Sending reset link"}],"satisfaction":{"score":5,"feedback":"Great support"}}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT006', '{"_id":"TKT006","ticketNumber":"CS-2024-00006","customerId":"C005","createdAt":{"$date":"2024-01-22T15:30:00.000Z"},"resolvedAt":null,"status":"in_progress","priority":"high","category":"shipping","subcategory":"damaged","assignedTo":"agent_002","escalations":[{"level":1,"timestamp":{"$date":"2024-01-22T17:00:00.000Z"},"reason":"no_response"}],"messages":[{"from":"customer","timestamp":{"$date":"2024-01-22T15:30:00.000Z"},"text":"Item arrived damaged"},{"from":"agent","timestamp":{"$date":"2024-01-22T15:45:00.000Z"},"text":"Please send photos"}],"satisfaction":null}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT007', '{"_id":"TKT007","ticketNumber":"CS-2024-00007","customerId":"C006","createdAt":{"$date":"2024-01-25T09:00:00.000Z"},"resolvedAt":{"$date":"2024-01-25T10:30:00.000Z"},"status":"closed","priority":"low","category":"technical","subcategory":"feature_request","assignedTo":"agent_005","escalations":[],"messages":[{"from":"customer","timestamp":{"$date":"2024-01-25T09:00:00.000Z"},"text":"Can you add dark mode?"}],"satisfaction":{"score":4,"feedback":null}}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT008', '{"_id":"TKT008","ticketNumber":"CS-2024-00008","customerId":"C002","createdAt":{"$date":"2024-01-28T16:45:00.000Z"},"resolvedAt":{"$date":"2024-01-29T09:00:00.000Z"},"status":"resolved","priority":"medium","category":"shipping","subcategory":"wrong_item","assignedTo":"agent_003","escalations":[],"messages":[{"from":"customer","timestamp":{"$date":"2024-01-28T16:45:00.000Z"},"text":"Received wrong item"},{"from":"agent","timestamp":{"$date":"2024-01-28T17:00:00.000Z"},"text":"Sending replacement"}],"satisfaction":{"score":4,"feedback":"Quick resolution"}}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT009', '{"_id":"TKT009","ticketNumber":"CS-2024-00009","customerId":"C007","createdAt":{"$date":"2024-02-01T10:00:00.000Z"},"resolvedAt":null,"status":"waiting_customer","priority":"medium","category":"account","subcategory":"profile_update","assignedTo":"agent_004","escalations":[],"messages":[{"from":"customer","timestamp":{"$date":"2024-02-01T10:00:00.000Z"},"text":"Cannot update address"},{"from":"agent","timestamp":{"$date":"2024-02-01T10:15:00.000Z"},"text":"Please verify your identity"}],"satisfaction":null}');
INSERT INTO SUPPORT_TICKETS (id, data) VALUES ('TKT010', '{"_id":"TKT010","ticketNumber":"CS-2024-00010","customerId":"C003","createdAt":{"$date":"2024-02-05T13:30:00.000Z"},"resolvedAt":{"$date":"2024-02-05T14:00:00.000Z"},"status":"resolved","priority":"urgent","category":"billing","subcategory":"refund_request","assignedTo":"agent_001","escalations":[],"messages":[{"from":"customer","timestamp":{"$date":"2024-02-05T13:30:00.000Z"},"text":"Cancel and refund my order"},{"from":"agent","timestamp":{"$date":"2024-02-05T13:35:00.000Z"},"text":"Refund processed"}],"satisfaction":{"score":5,"feedback":"Excellent service"}}');

COMMIT;
-- =====================================================
-- Helper function for dynamic JSON field access
-- Used by $getField operator when field name comes from another path
-- =====================================================

CREATE OR REPLACE FUNCTION get_dynamic_json_field(
    p_doc JSON,
    p_object_path VARCHAR2,
    p_key_path VARCHAR2
) RETURN JSON
IS
    v_key_value VARCHAR2(4000);
    v_full_path VARCHAR2(4000);
    v_result JSON;
BEGIN
    -- First, get the key value from the document
    EXECUTE IMMEDIATE
        'SELECT JSON_VALUE(:1, ''' || p_key_path || ''') FROM DUAL'
        INTO v_key_value
        USING p_doc;

    -- If key is null, return null
    IF v_key_value IS NULL THEN
        RETURN NULL;
    END IF;

    -- Build the full path by appending the key to the object path
    v_full_path := p_object_path || '.' || v_key_value;

    -- Get the value at the full path
    EXECUTE IMMEDIATE
        'SELECT JSON_QUERY(:1, ''' || v_full_path || ''' RETURNING JSON WITH WRAPPER) FROM DUAL'
        INTO v_result
        USING p_doc;

    RETURN v_result;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END get_dynamic_json_field;
/
