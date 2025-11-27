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
| `load-data.js` | Loads generated data into MongoDB and Oracle |
| `compare-pipelines.js` | Executes and compares pipeline results |
| `complex-pipelines.json` | Pipeline definitions for testing |
| `run-comparison.sh` | Main entry script to run full test suite |

## Quick Start

```bash
# Run full test suite with small dataset (~100MB)
./run-comparison.sh --size small

# Generate data only
node generate-data.js --size medium --output ./data

# Load data into databases
node load-data.js --target both --data-dir ./data --drop

# Run pipeline comparisons
node compare-pipelines.js --verbose
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
| PIPE001 | E-commerce Revenue Analysis | $match, $unwind, $lookup, $group, $project, $sort, $limit |
| PIPE002 | Product Variant Analysis | $match, $unwind, $project, $group, $addFields, $sort |
| PIPE003 | Customer LTV Analysis | $match, $project, $addFields, $group, $sort |
| PIPE004 | Review Sentiment Analysis | $match, $lookup, $unwind, $project, $group |
| PIPE005 | Analytics Funnel Analysis | $match, $project, $group, $addFields |
| PIPE006 | Social Engagement Analysis | $match, $project, $addFields, $group |
| PIPE007 | IoT Device Health | $unwind, $project, $group, $addFields |
| PIPE008 | IoT Time-Series Aggregation | $match, $project, $group, $addFields |
| PIPE009 | User Network Analysis | $match, $project, $addFields, $bucket |
| PIPE010 | Order-to-Review Journey | $match, $unwind, $lookup (correlated), $project, $group |

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

## Requirements

- Node.js 16+
- MongoDB 6.0+
- Oracle Database 23ai/26ai
- npm packages: `mongodb`, `oracledb`

## Notes

- For `xlarge` dataset, ensure at least 8GB RAM available
- Oracle SODA requires thick mode client for optimal performance
- Some pipelines may require MongoDB `allowDiskUse: true`
