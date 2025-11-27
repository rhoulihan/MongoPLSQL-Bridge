// MongoDB Test Data Loader for MongoPLSQL-Bridge Query Validation
// Usage: mongosh testdb -u admin -p admin123 --authenticationDatabase admin < load-mongodb.js

print('============================================================');
print('  Loading MongoPLSQL-Bridge Test Data into MongoDB');
print('============================================================');
print('');

db = db.getSiblingDB('testdb');

// ============================================================
// Sales Collection
// ============================================================
print('Loading sales collection...');
db.sales.drop();

db.sales.insertMany([
  {
    _id: "S001",
    orderId: 1001,
    customerId: "C001",
    customerName: "John Doe",
    status: "completed",
    category: "electronics",
    region: "north",
    amount: 150.00,
    quantity: 2,
    discount: 10,
    tax: 15.00,
    orderDate: "2024-01-15",
    items: [
      {product: "Widget", qty: 1, price: 100},
      {product: "Gadget", qty: 1, price: 50}
    ],
    tags: ["premium", "express"],
    metadata: {source: "web", campaign: "winter-sale"}
  },
  {
    _id: "S002",
    orderId: 1002,
    customerId: "C002",
    customerName: "Jane Smith",
    status: "completed",
    category: "electronics",
    region: "south",
    amount: 250.00,
    quantity: 5,
    discount: 25,
    tax: 25.00,
    orderDate: "2024-01-16",
    items: [
      {product: "Widget", qty: 5, price: 50}
    ],
    tags: ["bulk"],
    metadata: {source: "mobile", campaign: "winter-sale"}
  },
  {
    _id: "S003",
    orderId: 1003,
    customerId: "C001",
    customerName: "John Doe",
    status: "pending",
    category: "clothing",
    region: "north",
    amount: 75.50,
    quantity: 3,
    discount: 0,
    tax: 7.55,
    orderDate: "2024-01-17",
    items: [
      {product: "Shirt", qty: 2, price: 25},
      {product: "Pants", qty: 1, price: 25.50}
    ],
    tags: [],
    metadata: {source: "web", campaign: null}
  },
  {
    _id: "S004",
    orderId: 1004,
    customerId: "C003",
    customerName: "Alice Brown",
    status: "completed",
    category: "electronics",
    region: "east",
    amount: 500.00,
    quantity: 1,
    discount: 50,
    tax: 50.00,
    orderDate: "2024-01-18",
    items: [
      {product: "Premium Widget", qty: 1, price: 500}
    ],
    tags: ["premium", "vip"],
    metadata: {source: "store", campaign: "vip-exclusive"}
  },
  {
    _id: "S005",
    orderId: 1005,
    customerId: "C004",
    customerName: "Bob Wilson",
    status: "cancelled",
    category: "furniture",
    region: "west",
    amount: 1200.00,
    quantity: 2,
    discount: 100,
    tax: 120.00,
    orderDate: "2024-01-19",
    items: [
      {product: "Chair", qty: 2, price: 600}
    ],
    tags: ["bulky"],
    metadata: {source: "web", campaign: "furniture-fest"}
  },
  {
    _id: "S006",
    orderId: 1006,
    customerId: "C002",
    customerName: "Jane Smith",
    status: "completed",
    category: "clothing",
    region: "south",
    amount: 89.99,
    quantity: 4,
    discount: 5,
    tax: 9.00,
    orderDate: "2024-01-20",
    items: [
      {product: "Socks", qty: 4, price: 22.50}
    ],
    tags: ["clearance"],
    metadata: {source: "mobile", campaign: "clearance"}
  },
  {
    _id: "S007",
    orderId: 1007,
    customerId: "C005",
    customerName: "Charlie Green",
    status: "pending",
    category: "electronics",
    region: "north",
    amount: 0,
    quantity: 0,
    discount: 0,
    tax: 0,
    orderDate: "2024-01-21",
    items: [],
    tags: [],
    metadata: {source: "api", campaign: null}
  },
  {
    _id: "S008",
    orderId: 1008,
    customerId: "C001",
    customerName: "John Doe",
    status: "refunded",
    category: "electronics",
    region: "north",
    amount: -150.00,
    quantity: -2,
    discount: 0,
    tax: -15.00,
    orderDate: "2024-01-22",
    items: [
      {product: "Widget", qty: -1, price: 100},
      {product: "Gadget", qty: -1, price: 50}
    ],
    tags: ["refund"],
    metadata: {source: "support", campaign: null}
  },
  {
    _id: "S009",
    orderId: 1009,
    customerId: "C006",
    customerName: "Diana Prince",
    status: "completed",
    category: "jewelry",
    region: "east",
    amount: 9999.99,
    quantity: 1,
    discount: 500,
    tax: 999.99,
    orderDate: "2024-01-23",
    items: [
      {product: "Diamond Ring", qty: 1, price: 9999.99}
    ],
    tags: ["luxury", "premium", "vip"],
    metadata: {source: "store", campaign: "valentine"}
  },
  {
    _id: "S010",
    orderId: 1010,
    customerId: "C007",
    customerName: "Eve Johnson",
    status: "processing",
    category: "electronics",
    region: "west",
    amount: 299.99,
    quantity: 3,
    discount: null,
    tax: 30.00,
    orderDate: "2024-01-24",
    items: [
      {product: "Headphones", qty: 1, price: 199.99},
      {product: "Case", qty: 2, price: 50}
    ],
    tags: ["new-customer"],
    metadata: null
  }
]);

print('  Inserted ' + db.sales.countDocuments() + ' sales documents');

// Create indexes
db.sales.createIndex({status: 1});
db.sales.createIndex({category: 1});
db.sales.createIndex({region: 1});
db.sales.createIndex({customerId: 1});
db.sales.createIndex({amount: 1});

// ============================================================
// Employees Collection
// ============================================================
print('Loading employees collection...');
db.employees.drop();

db.employees.insertMany([
  {_id: "E001", name: "Alice", department: "Engineering", team: "Backend", salary: 95000, bonus: 10000, yearsOfService: 5, active: true, rating: 4.5},
  {_id: "E002", name: "Bob", department: "Engineering", team: "Frontend", salary: 85000, bonus: 8000, yearsOfService: 3, active: true, rating: 4.0},
  {_id: "E003", name: "Carol", department: "Engineering", team: "Backend", salary: 105000, bonus: 15000, yearsOfService: 7, active: true, rating: 4.8},
  {_id: "E004", name: "David", department: "Sales", team: "Enterprise", salary: 75000, bonus: 25000, yearsOfService: 4, active: true, rating: 4.2},
  {_id: "E005", name: "Eve", department: "Sales", team: "SMB", salary: 65000, bonus: 15000, yearsOfService: 2, active: true, rating: 3.8},
  {_id: "E006", name: "Frank", department: "Sales", team: "Enterprise", salary: 80000, bonus: 30000, yearsOfService: 6, active: false, rating: 4.5},
  {_id: "E007", name: "Grace", department: "Marketing", team: "Digital", salary: 70000, bonus: 5000, yearsOfService: 1, active: true, rating: 3.5},
  {_id: "E008", name: "Henry", department: "Marketing", team: "Content", salary: 72000, bonus: 6000, yearsOfService: 2, active: true, rating: 4.0},
  {_id: "E009", name: "Ivy", department: "Engineering", team: "DevOps", salary: 98000, bonus: 12000, yearsOfService: 4, active: true, rating: 4.3},
  {_id: "E010", name: "Jack", department: "HR", team: "Recruiting", salary: 60000, bonus: 3000, yearsOfService: 1, active: true, rating: null}
]);

print('  Inserted ' + db.employees.countDocuments() + ' employee documents');

db.employees.createIndex({department: 1});
db.employees.createIndex({salary: 1});
db.employees.createIndex({active: 1});

// ============================================================
// Products Collection
// ============================================================
print('Loading products collection...');
db.products.drop();

db.products.insertMany([
  {_id: "P001", name: "Widget", category: "tools", subcategory: "hand-tools", price: 25.00, cost: 10.00, stock: 100, active: true, rating: 4.5, tags: ["bestseller", "featured"]},
  {_id: "P002", name: "Gadget", category: "electronics", subcategory: "accessories", price: 50.00, cost: 20.00, stock: 75, active: true, rating: 4.2, tags: ["new"]},
  {_id: "P003", name: "Premium Widget", category: "tools", subcategory: "hand-tools", price: 500.00, cost: 200.00, stock: 10, active: true, rating: 4.9, tags: ["premium", "featured"]},
  {_id: "P004", name: "Basic Widget", category: "tools", subcategory: "hand-tools", price: 10.00, cost: 5.00, stock: 500, active: true, rating: 3.5, tags: ["budget"]},
  {_id: "P005", name: "Discontinued Item", category: "misc", subcategory: null, price: 0, cost: 15.00, stock: 0, active: false, rating: null, tags: []},
  {_id: "P006", name: "Headphones", category: "electronics", subcategory: "audio", price: 199.99, cost: 80.00, stock: 50, active: true, rating: 4.7, tags: ["premium", "bestseller"]},
  {_id: "P007", name: "USB Cable", category: "electronics", subcategory: "accessories", price: 9.99, cost: 2.00, stock: 1000, active: true, rating: 4.0, tags: ["essential"]},
  {_id: "P008", name: "Chair", category: "furniture", subcategory: "office", price: 299.99, cost: 150.00, stock: 25, active: true, rating: 4.4, tags: ["ergonomic"]}
]);

print('  Inserted ' + db.products.countDocuments() + ' product documents');

db.products.createIndex({category: 1});
db.products.createIndex({price: 1});
db.products.createIndex({active: 1});

// ============================================================
// Customers Collection (for $lookup tests)
// ============================================================
print('Loading customers collection...');
db.customers.drop();

db.customers.insertMany([
  {_id: "C001", name: "John Doe", email: "john.doe@example.com", tier: "gold", joinDate: "2023-06-15T10:30:00.000Z", address: {city: "New York", state: "NY", zip: "10001"}},
  {_id: "C002", name: "Jane Smith", email: "jane.smith@example.com", tier: "silver", joinDate: "2023-08-20T14:45:00.000Z", address: {city: "Los Angeles", state: "CA", zip: "90001"}},
  {_id: "C003", name: "Alice Brown", email: "alice.brown@example.com", tier: "gold", joinDate: "2023-03-10T09:00:00.000Z", address: {city: "Chicago", state: "IL", zip: "60601"}},
  {_id: "C004", name: "Bob Wilson", email: "bob.wilson@example.com", tier: "bronze", joinDate: "2024-01-05T16:20:00.000Z", address: {city: "Houston", state: "TX", zip: "77001"}},
  {_id: "C005", name: "Charlie Green", email: "charlie.green@example.com", tier: "silver", joinDate: "2023-11-30T11:15:00.000Z", address: {city: "Phoenix", state: "AZ", zip: "85001"}},
  {_id: "C006", name: "Diana Prince", email: "diana.prince@example.com", tier: "platinum", joinDate: "2022-12-01T08:00:00.000Z", address: {city: "Miami", state: "FL", zip: "33101"}},
  {_id: "C007", name: "Eve Johnson", email: "eve.johnson@example.com", tier: "bronze", joinDate: "2024-01-20T13:30:00.000Z", address: {city: "Seattle", state: "WA", zip: "98101"}}
]);

print('  Inserted ' + db.customers.countDocuments() + ' customer documents');

db.customers.createIndex({tier: 1});
db.customers.createIndex({email: 1});

// ============================================================
// Events Collection (for date operator tests with ISODate)
// ============================================================
print('Loading events collection...');
db.events.drop();

db.events.insertMany([
  {_id: "EV001", title: "Product Launch", eventDate: new Date("2024-03-15T14:30:00.000Z"), category: "marketing", attendees: 150, tags: ["launch", "product"]},
  {_id: "EV002", title: "Team Meeting", eventDate: new Date("2024-01-22T09:00:00.000Z"), category: "internal", attendees: 25, tags: ["recurring", "team"]},
  {_id: "EV003", title: "Customer Webinar", eventDate: new Date("2024-06-10T16:00:00.000Z"), category: "sales", attendees: 500, tags: ["webinar", "customers"]},
  {_id: "EV004", title: "Q1 Review", eventDate: new Date("2024-04-01T10:00:00.000Z"), category: "internal", attendees: 50, tags: ["quarterly", "review"]},
  {_id: "EV005", title: "Trade Show", eventDate: new Date("2024-09-20T08:00:00.000Z"), category: "marketing", attendees: 1000, tags: ["trade-show", "networking"]},
  {_id: "EV006", title: "Holiday Party", eventDate: new Date("2024-12-20T18:00:00.000Z"), category: "social", attendees: 200, tags: ["party", "annual"]},
  {_id: "EV007", title: "Training Session", eventDate: new Date("2024-02-28T13:00:00.000Z"), category: "internal", attendees: 30, tags: ["training", "onboarding"]},
  {_id: "EV008", title: "Board Meeting", eventDate: new Date("2024-07-15T11:00:00.000Z"), category: "executive", attendees: 10, tags: ["board", "quarterly"]}
]);

print('  Inserted ' + db.events.countDocuments() + ' event documents');

db.events.createIndex({eventDate: 1});
db.events.createIndex({category: 1});

// ============================================================
// Inventory Collection (for $lookup with products)
// ============================================================
print('Loading inventory collection...');
db.inventory.drop();

db.inventory.insertMany([
  {_id: "INV001", productId: "P001", warehouse: "WH-EAST", quantity: 50, lastRestocked: "2024-01-10"},
  {_id: "INV002", productId: "P001", warehouse: "WH-WEST", quantity: 50, lastRestocked: "2024-01-12"},
  {_id: "INV003", productId: "P002", warehouse: "WH-EAST", quantity: 40, lastRestocked: "2024-01-08"},
  {_id: "INV004", productId: "P002", warehouse: "WH-CENTRAL", quantity: 35, lastRestocked: "2024-01-15"},
  {_id: "INV005", productId: "P003", warehouse: "WH-EAST", quantity: 5, lastRestocked: "2024-01-05"},
  {_id: "INV006", productId: "P003", warehouse: "WH-WEST", quantity: 5, lastRestocked: "2024-01-06"},
  {_id: "INV007", productId: "P004", warehouse: "WH-CENTRAL", quantity: 500, lastRestocked: "2024-01-01"},
  {_id: "INV008", productId: "P006", warehouse: "WH-EAST", quantity: 25, lastRestocked: "2024-01-18"},
  {_id: "INV009", productId: "P006", warehouse: "WH-WEST", quantity: 25, lastRestocked: "2024-01-19"},
  {_id: "INV010", productId: "P007", warehouse: "WH-CENTRAL", quantity: 1000, lastRestocked: "2024-01-20"},
  {_id: "INV011", productId: "P008", warehouse: "WH-EAST", quantity: 15, lastRestocked: "2024-01-14"},
  {_id: "INV012", productId: "P008", warehouse: "WH-WEST", quantity: 10, lastRestocked: "2024-01-16"}
]);

print('  Inserted ' + db.inventory.countDocuments() + ' inventory documents');

db.inventory.createIndex({productId: 1});
db.inventory.createIndex({warehouse: 1});

// ============================================================
// Summary
// ============================================================
print('');
print('============================================================');
print('  MongoDB Test Data Load Complete');
print('============================================================');
print('  Collections:');
print('    - sales: ' + db.sales.countDocuments() + ' documents');
print('    - employees: ' + db.employees.countDocuments() + ' documents');
print('    - products: ' + db.products.countDocuments() + ' documents');
print('    - customers: ' + db.customers.countDocuments() + ' documents');
print('    - events: ' + db.events.countDocuments() + ' documents');
print('    - inventory: ' + db.inventory.countDocuments() + ' documents');
print('============================================================');
