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
SELECT 'commission_schedule', COUNT(*) FROM commission_schedule;

PROMPT ============================================================

EXIT;
