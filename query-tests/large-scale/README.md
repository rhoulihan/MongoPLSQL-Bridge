# Large-Scale Cross-Database Comparison Tests

This directory contains tools for generating large datasets with deeply nested documents and comparing aggregation pipeline results between MongoDB and Oracle databases.

## Overview

The test suite validates that complex aggregation pipelines produce identical results when executed on MongoDB versus translated and executed on Oracle. This ensures the translator correctly handles:

- Deeply nested document structures
- Complex aggregation operations
- Multi-stage pipelines with lookups
- Large datasets (up to ~4GB)

## Files

| File | Description |
|------|-------------|
| `generate-data.js` | Generates test data with configurable sizes |
| `load-data.js` | Loads generated data into MongoDB and Oracle using SQL |
| `reload-products.js` | Utility to reload ecommerce_products table |
| `complex-pipelines.json` | Pipeline definitions for testing |

## Quick Start

```bash
# Generate data only
node generate-data.js --size medium --output ./data

# Load data into MongoDB only
node load-data.js --target mongodb --data-dir ./data --drop

# Load data into Oracle only (uses SQL with PL/SQL for large CLOBs)
node load-data.js --target oracle --data-dir ./data --drop

# Load data into both databases
node load-data.js --target both --data-dir ./data --drop

# Run pipeline comparisons (from query-tests directory)
cd .. && node scripts/export-results.js --large-scale-only
```

## Data Sizes

| Size | Total Documents | Estimated Size |
|------|-----------------|----------------|
| small | ~219,000 | ~100MB |
| medium | ~1,095,000 | ~500MB |
| large | ~4,470,000 | ~2GB |
| xlarge | ~10,450,000 | ~4GB |

## Collections Generated

The generator creates 10 collections with deeply nested document structures:

### E-commerce Domain
- **ecommerce_products** - Products with variants, specifications, pricing, ratings
- **ecommerce_customers** - Customers with addresses, preferences, loyalty data
- **ecommerce_orders** - Orders with line items, shipping, payment details
- **ecommerce_reviews** - Reviews with ratings, media, helpful votes

### Analytics Domain
- **analytics_sessions** - User sessions with device, traffic, engagement data
- **analytics_events** - Page events with context, properties, performance

### Social Media Domain
- **social_users** - Users with followers, preferences, settings
- **social_posts** - Posts with media, reactions, nested comments

### IoT Domain
- **iot_devices** - Devices with sensors, configuration, maintenance
- **iot_readings** - Time-series sensor readings with alerts

## Complex Pipelines

The test suite includes 10 complex pipelines testing various operators:

| ID | Name | Operators Tested |
|----|------|-----------------|
| PIPE001 | E-commerce Order Revenue Analysis | $match, $group, $addFields, $sort, $limit |
| PIPE002 | Product Performance Analysis | $group, $addFields, $sort, $limit |
| PIPE003 | Customer Lifetime Value Analysis | $match, $group, $addFields, $sort |
| PIPE004 | Review Sentiment and Quality Analysis | $match, $group, $addFields, $sort, $limit |
| PIPE005 | Analytics Session Funnel Analysis | $match, $group, $addFields, $sort, $limit |
| PIPE006 | Social Post Engagement Analysis | $match, $group, $addFields, $sort |
| PIPE007 | IoT Device Health Analysis by Building | $group, $addFields, $sort, $limit |
| PIPE008 | IoT Time-Series Aggregation | $group, $addFields, $sort, $limit |
| PIPE009 | User Follower Network Analysis | $match, $bucket, $addFields |
| PIPE010 | Order Analysis by Payment and Shipping | $group, $addFields, $sort, $limit |

## Document Nesting Examples

### Product Document (6 levels deep)
```javascript
{
  _id: 1,
  name: "Product Name",
  description: {
    features: [...],
    specifications: {
      weight: { value: 1.5, unit: "kg" },
      dimensions: {
        length: 10,
        width: 20,
        height: 5,
        unit: "cm"
      }
    }
  },
  variants: [{
    attributes: { color: "red", size: "M" },
    inventory: {
      quantity: 100,
      warehouse: "WH-EAST"
    },
    images: [{
      url: "...",
      dimensions: { width: 800, height: 800 }
    }]
  }],
  ratings: {
    distribution: { '5': 100, '4': 50, ... }
  },
  metadata: {
    analytics: {
      views: 1000,
      conversions: 100
    }
  }
}
```

### Social Post Document (with recursive comments)
```javascript
{
  _id: 1,
  content: {
    mentions: [{ userId: 5, position: 10 }],
    hashtags: ["#example"],
    links: [{
      url: "...",
      thumbnail: "..."
    }]
  },
  media: [{
    dimensions: { width: 1920, height: 1080 }
  }],
  poll: {
    options: [{ id: "1", text: "Option A", votes: 100 }]
  },
  comments: [{
    reactions: { like: 10, love: 5 },
    replies: [{
      reactions: { ... },
      replies: [{ ... }]  // 3 levels deep
    }]
  }]
}
```

## Environment Variables

```bash
# MongoDB
export MONGODB_URI="mongodb://localhost:27017"
export MONGODB_DB="largescale_test"

# Oracle
export ORACLE_USER="mongouser"
export ORACLE_PASSWORD="mongopass"
export ORACLE_CONNECT_STRING="localhost:1521/FREEPDB1"
```

## Output

Results are saved to `results/comparison-report-<timestamp>.json`:

```json
{
  "timestamp": "2024-01-15T10:30:00.000Z",
  "summary": {
    "total": 10,
    "passed": 10,
    "failed": 0,
    "errors": 0,
    "totalMongoTime": 1234,
    "totalOracleTime": 5678
  },
  "results": [
    {
      "id": "PIPE001",
      "name": "E-commerce Revenue Analysis",
      "status": "passed",
      "mongodb": {
        "count": 50,
        "executionTime": 150
      },
      "oracle": {
        "count": 50,
        "executionTime": 300
      },
      "comparison": {
        "identical": true,
        "differences": []
      }
    }
  ]
}
```

## Oracle Data Loading

The `load-data.js` script uses SQL with PL/SQL for Oracle data loading:

- **No SODA dependency** - Uses native SQL INSERT statements
- **CLOB handling** - Large JSON documents are split into 2000-character chunks
- **Docker execution** - Runs SQL via `docker exec` and `sqlplus`
- **JSON validation** - Tables created with `IS JSON` constraint

Example Oracle table structure:
```sql
CREATE TABLE ecommerce_products (
  id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  data CLOB CONSTRAINT ecommerce_products_json CHECK (data IS JSON)
);
```

## Requirements

- Node.js 16+
- MongoDB 6.0+
- Oracle Database 23ai Free (running in Docker container `mongo-translator-oracle`)
- npm packages: `mongodb`

## Notes

- For `xlarge` dataset, ensure at least 8GB RAM available
- Oracle loading uses SQL*Plus with 2000-char chunks to avoid line length limits
- Some pipelines may require MongoDB `allowDiskUse: true`
