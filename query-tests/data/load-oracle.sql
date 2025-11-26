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
SELECT 'products', COUNT(*) FROM products;

PROMPT ============================================================

EXIT;
