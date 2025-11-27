#!/usr/bin/env node
/**
 * Reload ecommerce_products data to Oracle using PL/SQL for large CLOBs
 */
const fs = require('fs');
const path = require('path');

// Load the product data
const dataFile = path.join(__dirname, 'data/ecommerce_products/batch_00001.json');
const documents = JSON.parse(fs.readFileSync(dataFile, 'utf8'));

let sql = '';
sql += 'SET DEFINE OFF\n'; // Disable & substitution
sql += 'SET SERVEROUTPUT ON\n';
sql += '\n';
sql += '-- Drop and recreate table\n';
sql += 'BEGIN EXECUTE IMMEDIATE \'DROP TABLE ecommerce_products CASCADE CONSTRAINTS\'; EXCEPTION WHEN OTHERS THEN NULL; END;\n';
sql += '/\n';
sql += 'CREATE TABLE ecommerce_products (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, data CLOB CONSTRAINT ecommerce_products_json CHECK (data IS JSON));\n';
sql += '\n';
sql += 'DECLARE\n';
sql += '  v_clob CLOB;\n';
sql += 'BEGIN\n';

documents.forEach((doc, i) => {
  // Escape single quotes in JSON
  const jsonStr = JSON.stringify(doc).replace(/'/g, "''");

  // Split into chunks of 2000 characters to avoid line length limits
  const chunkSize = 2000;
  const chunks = [];
  for (let j = 0; j < jsonStr.length; j += chunkSize) {
    chunks.push(jsonStr.substring(j, j + chunkSize));
  }

  sql += `  -- Product ${i + 1}\n`;
  sql += '  v_clob := \'\';\n';
  chunks.forEach((chunk, ci) => {
    sql += `  v_clob := v_clob || '${chunk}';\n`;
  });
  sql += '  INSERT INTO ecommerce_products (data) VALUES (v_clob);\n';
  sql += '\n';
});

sql += '  COMMIT;\n';
sql += '  DBMS_OUTPUT.PUT_LINE(\'Inserted \' || SQL%ROWCOUNT || \' products\');\n';
sql += 'END;\n';
sql += '/\n';
sql += '\n';
sql += 'SELECT COUNT(*) AS product_count FROM ecommerce_products;\n';
sql += 'EXIT;\n';

const outputFile = '/tmp/reload_products.sql';
fs.writeFileSync(outputFile, sql);
console.log(`SQL file created: ${outputFile}`);
console.log(`Contains ${documents.length} products`);
console.log(`File size: ${(sql.length / 1024).toFixed(1)} KB`);
