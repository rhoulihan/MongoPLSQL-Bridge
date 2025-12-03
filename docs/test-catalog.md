# MongoPLSQL-Bridge Test Catalog

This document contains a comprehensive catalog of all integration tests, including the MongoDB pipeline used and the Oracle SQL generated.

> **Note:** The Oracle SQL shown below uses dot notation for field access (e.g., `base.data.fieldName`) per IMPL-031. SQL is regenerated fresh each time this catalog is built.

## Test Suites Overview

| Suite | Test Count | Description |
|-------|------------|-------------|
| Unit Integration Tests | 142 | Operator-level validation tests |
| Large-Scale Pipelines | 10 | Complex cross-database validation tests |
| **Total** | **152** | |

---

## Unit Integration Tests by Category

| Category | Test Count |
|----------|------------|
| Accumulator | 10 |
| AddFields | 2 |
| Arithmetic | 10 |
| Array | 14 |
| Bucket | 2 |
| BucketAuto | 2 |
| Comparison | 10 |
| Complex | 8 |
| Conditional | 3 |
| Count | 3 |
| Date | 11 |
| Edge | 3 |
| Expression | 2 |
| Facet | 3 |
| GraphLookup | 1 |
| Logical | 6 |
| Lookup | 4 |
| Null handling | 5 |
| Object | 1 |
| Redact | 2 |
| ReplaceRoot | 1 |
| Sample | 2 |
| SetWindowFields | 4 |
| Stage | 7 |
| String | 14 |
| TypeConversion | 5 |
| UnionWith | 3 |
| Unwind | 2 |
| Window | 2 |
| **Total** | **142** |

---

## Accumulator Tests

### AGG001: Group with count

**Description:** Tests $group with $sum: 1 for counting  
**Collection:** `sales`  
**Operator:** `$count`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$status",
      "count": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.status AS "_id", SUM(:1) AS count
FROM sales base
GROUP BY base.data.status
ORDER BY "_id"
```

---

### AGG002: Group with sum

**Description:** Tests $group with $sum on numeric field  
**Collection:** `sales`  
**Operator:** `$sum`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$group": {
      "_id": "$category",
      "totalAmount": {
        "$sum": "$amount"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.category AS "_id", SUM(base.data.amount) AS totalAmount
FROM sales base
WHERE base.data.status = :1
GROUP BY base.data.category
ORDER BY "_id"
```

---

### AGG003: Group with average

**Description:** Tests $group with $avg on numeric field  
**Collection:** `employees`  
**Operator:** `$avg`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$department",
      "avgSalary": {
        "$avg": "$salary"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.department AS "_id", AVG(base.data.salary) AS avgSalary
FROM employees base
GROUP BY base.data.department
ORDER BY "_id"
```

---

### AGG004: Group with min

**Description:** Tests $group with $min on numeric field  
**Collection:** `products`  
**Operator:** `$min`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$group": {
      "_id": "$category",
      "minPrice": {
        "$min": "$price"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.category AS "_id", MIN(base.data.price) AS minPrice
FROM products base
WHERE base.data.active = :1
GROUP BY base.data.category
ORDER BY "_id"
```

---

### AGG005: Group with max

**Description:** Tests $group with $max on numeric field  
**Collection:** `employees`  
**Operator:** `$max`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$department",
      "maxSalary": {
        "$max": "$salary"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.department AS "_id", MAX(base.data.salary) AS maxSalary
FROM employees base
GROUP BY base.data.department
ORDER BY "_id"
```

---

### AGG006: Group with multiple accumulators

**Description:** Tests $group with multiple accumulators  
**Collection:** `employees`  
**Operator:** `$sum/$avg/$min/$max`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$department",
      "count": {
        "$sum": 1
      },
      "totalSalary": {
        "$sum": "$salary"
      },
      "avgSalary": {
        "$avg": "$salary"
      },
      "minSalary": {
        "$min": "$salary"
      },
      "maxSalary": {
        "$max": "$salary"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.department AS "_id", SUM(:1) AS count, SUM(base.data.salary) AS totalSalary, AVG(base.data.salary) AS avgSalary, MIN(base.data.salary) AS minSalary, MAX(base.data.salary) AS maxSalary
FROM employees base
GROUP BY base.data.department
ORDER BY "_id"
```

---

### AGG007: Group with $push

**Description:** Tests $group with $push to collect values into array  
**Collection:** `employees`  
**Operator:** `$push`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$department",
      "employees": {
        "$push": "$name"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.department AS "_id", JSON_ARRAYAGG(base.data.name) AS employees
FROM employees base
GROUP BY base.data.department
ORDER BY "_id"
```

---

### AGG008: Group with $addToSet

**Description:** Tests $group with $addToSet to collect unique values  
**Collection:** `sales`  
**Operator:** `$addToSet`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$region",
      "statuses": {
        "$addToSet": "$status"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.region AS "_id", JSON_QUERY('[' || LISTAGG(DISTINCT '"' || base.data.status || '"', ',') WITHIN GROUP (ORDER BY base.data.status) || ']', '$' RETURNING CLOB) AS statuses
FROM sales base
GROUP BY base.data.region
ORDER BY "_id"
```

---

### AGG009: First in group

**Description:** Tests $first accumulator to get first value in each group  
**Collection:** `employees`  
**Operator:** `$first`  

**MongoDB Pipeline:**
```json
[
  {
    "$sort": {
      "salary": -1
    }
  },
  {
    "$group": {
      "_id": "$department",
      "highestPaidEmployee": {
        "$first": "$name"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.department AS "_id", MIN(base.data.name) AS highestPaidEmployee
FROM employees base
GROUP BY base.data.department
ORDER BY "_id"
```

---

### AGG010: Last in group

**Description:** Tests $last accumulator to get last value in each group  
**Collection:** `employees`  
**Operator:** `$last`  

**MongoDB Pipeline:**
```json
[
  {
    "$sort": {
      "salary": -1
    }
  },
  {
    "$group": {
      "_id": "$department",
      "lowestPaidEmployee": {
        "$last": "$name"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.department AS "_id", MAX(base.data.name) AS lowestPaidEmployee
FROM employees base
GROUP BY base.data.department
ORDER BY "_id"
```

---

## AddFields Tests

### ADDFIELDS001: Add computed field

**Description:** Tests $addFields stage to add computed column  
**Collection:** `employees`  
**Operator:** `$addFields`  

**MongoDB Pipeline:**
```json
[
  {
    "$addFields": {
      "totalCompensation": {
        "$add": [
          "$salary",
          "$bonus"
        ]
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "totalCompensation": 1
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, (base.data.salary + base.data.bonus) AS totalCompensation, (base.data.salary + base.data.bonus) AS totalCompensation
FROM employees base
ORDER BY base.data."_id"
```

---

### ADDFIELDS002: $set as alias for $addFields

**Description:** Tests $set stage (alias for $addFields)  
**Collection:** `products`  
**Operator:** `$set`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$set": {
      "profitMargin": {
        "$subtract": [
          "$price",
          "$cost"
        ]
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "profitMargin": 1
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, (base.data.price - base.data.cost) AS profitMargin, (base.data.price - base.data.cost) AS profitMargin
FROM products base
WHERE base.data.active = :1
ORDER BY base.data."_id"
```

---

## Arithmetic Tests

### ARITH001: Addition in project

**Description:** Tests $add operator in projection  
**Collection:** `sales`  
**Operator:** `$add`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "total": {
        "$add": [
          "$amount",
          "$tax"
        ]
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, (base.data.amount + base.data.tax) AS total
FROM sales base
WHERE base.data.status = :1
ORDER BY base.data.orderId
```

---

### ARITH002: Subtraction in project

**Description:** Tests $subtract operator in projection  
**Collection:** `products`  
**Operator:** `$subtract`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "profit": {
        "$subtract": [
          "$price",
          "$cost"
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, (base.data.price - base.data.cost) AS profit
FROM products base
WHERE base.data.active = :1
ORDER BY base.data."_id"
```

---

### ARITH003: Multiplication in project

**Description:** Tests $multiply operator in projection  
**Collection:** `employees`  
**Operator:** `$multiply`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "totalComp": {
        "$add": [
          "$salary",
          {
            "$multiply": [
              "$bonus",
              1
            ]
          }
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, (base.data.salary + (base.data.bonus * :1)) AS totalComp
FROM employees base
ORDER BY base.data."_id"
```

---

### ARITH004: Division in project

**Description:** Tests $divide operator in projection  
**Collection:** `products`  
**Operator:** `$divide`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "cost": {
        "$gt": 0
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "margin": {
        "$divide": [
          {
            "$subtract": [
              "$price",
              "$cost"
            ]
          },
          "$cost"
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, ((base.data.price - base.data.cost) / base.data.cost) AS margin
FROM products base
WHERE CAST(base.data.cost AS NUMBER) > :1
ORDER BY base.data."_id"
```

---

### ARITH005: Modulo in project

**Description:** Tests $mod operator in projection  
**Collection:** `sales`  
**Operator:** `$mod`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "orderIdMod3": {
        "$mod": [
          "$orderId",
          3
        ]
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, MOD(base.data.orderId, :1) AS orderIdMod3
FROM sales base
ORDER BY base.data.orderId
```

---

### ARITH006: Absolute value

**Description:** Tests $abs operator to get absolute value of negative numbers  
**Collection:** `sales`  
**Operator:** `$abs`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "amount": 1,
      "absoluteAmount": {
        "$abs": "$amount"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.amount AS amount, ABS(base.data.amount) AS absoluteAmount
FROM sales base
```

---

### ARITH007: Ceiling function

**Description:** Tests $ceil operator to round up to nearest integer  
**Collection:** `sales`  
**Operator:** `$ceil`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "tax": 1,
      "taxCeiled": {
        "$ceil": "$tax"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.tax AS tax, CEIL(base.data.tax) AS taxCeiled
FROM sales base
```

---

### ARITH008: Floor function

**Description:** Tests $floor operator to round down to nearest integer  
**Collection:** `sales`  
**Operator:** `$floor`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "amount": 1,
      "amountFloored": {
        "$floor": "$amount"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.amount AS amount, FLOOR(base.data.amount) AS amountFloored
FROM sales base
```

---

### ARITH009: Round function

**Description:** Tests $round operator to round to specified decimal places  
**Collection:** `sales`  
**Operator:** `$round`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "amount": 1,
      "amountRounded": {
        "$round": [
          "$amount",
          0
        ]
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.amount AS amount, ROUND(base.data.amount, :1) AS amountRounded
FROM sales base
```

---

### ARITH010: Truncate function

**Description:** Tests $trunc operator to truncate decimal places  
**Collection:** `sales`  
**Operator:** `$trunc`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "amount": 1,
      "amountTruncated": {
        "$trunc": [
          "$amount",
          1
        ]
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.amount AS amount, TRUNC(base.data.amount, :1) AS amountTruncated
FROM sales base
```

---

## Array Tests

### ARR001: Get first array element

**Description:** Tests $arrayElemAt to get first element  
**Collection:** `products`  
**Operator:** `$arrayElemAt`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "tags": {
        "$ne": []
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "firstTag": {
        "$arrayElemAt": [
          "$tags",
          0
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, JSON_VALUE(base.data, '$.tags[0]') AS firstTag
FROM products base
WHERE JSON_VALUE(base.data, '$.tags.size()') > 0
ORDER BY base.data."_id"
```

---

### ARR002: Get array size

**Description:** Tests $size operator to get array length  
**Collection:** `sales`  
**Operator:** `$size`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "itemCount": {
        "$size": "$items"
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, JSON_VALUE(base.data, '$.items.size()') AS itemCount
FROM sales base
ORDER BY base.data.orderId
```

---

### ARR003: Array first element with $first

**Description:** Tests $first operator as array accessor  
**Collection:** `products`  
**Operator:** `$first`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "tags": {
        "$ne": []
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "firstTag": {
        "$first": "$tags"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, JSON_VALUE(base.data, '$.tags[0]') AS firstTag
FROM products base
WHERE JSON_VALUE(base.data, '$.tags.size()') > 0
ORDER BY base.data."_id"
```

---

### ARR004: Array last element with $last

**Description:** Tests $last operator as array accessor  
**Collection:** `sales`  
**Operator:** `$last`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "tags": {
        "$ne": []
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "lastTag": {
        "$last": "$tags"
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, JSON_VALUE(base.data, '$.tags[last]') AS lastTag
FROM sales base
WHERE JSON_VALUE(base.data, '$.tags.size()') > 0
ORDER BY base.data.orderId
```

---

### ARR005: $filter - filter array elements

**Description:** Tests $filter operator to filter array elements by condition  
**Collection:** `sales`  
**Operator:** `$filter`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "highValueItems": {
        "$filter": {
          "input": "$items",
          "as": "item",
          "cond": {
            "$gt": [
              "$$item.qty",
              1
            ]
          }
        }
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, (
  SELECT JSON_ARRAYAGG(val) FROM JSON_TABLE(data, '$.items[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))
WHERE base.data.item.qty > :1) AS highValueItems
FROM sales base
ORDER BY base.data.orderId
FETCH FIRST 5 ROWS ONLY
```

---

### ARR006: $map - transform array elements

**Description:** Tests $map operator to transform array elements  
**Collection:** `sales`  
**Operator:** `$map`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "itemProducts": {
        "$map": {
          "input": "$items",
          "as": "item",
          "in": "$$item.product"
        }
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, (
  SELECT JSON_ARRAYAGG(base.data.item.product) FROM JSON_TABLE(data, '$.items[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))) AS itemProducts
FROM sales base
ORDER BY base.data.orderId
FETCH FIRST 5 ROWS ONLY
```

---

### ARR007: $reduce - reduce array to single value

**Description:** Tests $reduce operator to reduce array to single value  
**Collection:** `sales`  
**Operator:** `$reduce`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "totalQty": {
        "$reduce": {
          "input": "$items",
          "initialValue": 0,
          "in": {
            "$add": [
              "$$value",
              "$$this.qty"
            ]
          }
        }
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, /* $reduce not fully supported */ NULL AS totalQty
FROM sales base
ORDER BY base.data.orderId
FETCH FIRST 5 ROWS ONLY
```

---

### ARR008: $concatArrays - concatenate arrays

**Description:** Tests $concatArrays operator to concatenate multiple arrays  
**Collection:** `sales`  
**Operator:** `$concatArrays`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "tags": {
        "$exists": true
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "allLabels": {
        "$concatArrays": [
          "$tags",
          [
            "extra"
          ]
        ]
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, (
  SELECT JSON_ARRAYAGG(val
ORDER BY rn) FROM (
  SELECT val, ROWNUM + 0 AS rn FROM JSON_TABLE(base.data, '$.tags[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))
UNION ALL SELECT val, ROWNUM + 1000 AS rn FROM JSON_TABLE('["extra"]', '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))) AS allLabels
FROM sales base
WHERE JSON_EXISTS(base.data, '$.tags')
ORDER BY base.data.orderId
FETCH FIRST 5 ROWS ONLY
```

---

### ARR009: $slice - get subset of array

**Description:** Tests $slice operator to get array subset  
**Collection:** `products`  
**Operator:** `$slice`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "tags": {
        "$exists": true
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "firstTwoTags": {
        "$slice": [
          "$tags",
          2
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, JSON_QUERY(base.data, '$.tags[0 to 1]') AS firstTwoTags
FROM products base
WHERE JSON_EXISTS(base.data, '$.tags')
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### ARR010: $slice - get last elements

**Description:** Tests $slice operator to get last N elements  
**Collection:** `products`  
**Operator:** `$slice`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "tags": {
        "$exists": true
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "lastTwoTags": {
        "$slice": [
          "$tags",
          -2
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, JSON_QUERY(base.data, '$.tags[last-1 to last]') AS lastTwoTags
FROM products base
WHERE JSON_EXISTS(base.data, '$.tags')
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### ARR011: Reverse array

**Description:** Tests $reverseArray operator to reverse array element order  
**Collection:** `sales`  
**Operator:** `$reverseArray`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "_id": "S001"
    }
  },
  {
    "$project": {
      "_id": 1,
      "tags": 1,
      "reversedTags": {
        "$reverseArray": "$tags"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.tags AS tags, (
  SELECT JSON_ARRAYAGG(val
ORDER BY rn DESC) FROM JSON_TABLE(base.data, '$.tags[*]' COLUMNS (val VARCHAR2(4000) PATH '$', rn FOR ORDINALITY))) AS reversedTags
FROM sales base
WHERE base.data."_id" = :1
```

---

### ARR012: Is array check

**Description:** Tests $isArray operator to check if field is an array  
**Collection:** `sales`  
**Operator:** `$isArray`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "isTagsArray": {
        "$isArray": "$tags"
      },
      "isAmountArray": {
        "$isArray": "$amount"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, CASE WHEN JSON_EXISTS(base.data, '$.tags[0]') THEN 1 ELSE 0 END AS isTagsArray, CASE WHEN JSON_EXISTS(base.data, '$.amount[0]') THEN 1 ELSE 0 END AS isAmountArray
FROM sales base
```

---

### ARR013: Set union

**Description:** Tests $setUnion operator for union of two arrays  
**Collection:** `sales`  
**Operator:** `$setUnion`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "_id": "S001"
    }
  },
  {
    "$project": {
      "_id": 1,
      "tags": 1,
      "unionResult": {
        "$setUnion": [
          "$tags",
          [
            "new-tag",
            "premium"
          ]
        ]
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.tags AS tags, (
  SELECT JSON_ARRAYAGG(val) FROM (
  SELECT DISTINCT val FROM (
  SELECT val FROM JSON_TABLE(JSON_QUERY(base.data, '$.tags'), '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))
UNION SELECT val FROM JSON_TABLE(:1, '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))))) AS unionResult
FROM sales base
WHERE base.data."_id" = :2
```

---

### ARR014: Set intersection

**Description:** Tests $setIntersection operator for intersection of arrays  
**Collection:** `sales`  
**Operator:** `$setIntersection`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "_id": "S001"
    }
  },
  {
    "$project": {
      "_id": 1,
      "tags": 1,
      "intersectResult": {
        "$setIntersection": [
          "$tags",
          [
            "premium",
            "discount"
          ]
        ]
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.tags AS tags, (
  SELECT JSON_ARRAYAGG(val) FROM (
  SELECT DISTINCT val FROM JSON_TABLE(base.data, '$.tags[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))
INTERSECT SELECT DISTINCT val FROM JSON_TABLE(:1, '$[*]' COLUMNS (val VARCHAR2(4000) PATH '$')))) AS intersectResult
FROM sales base
WHERE base.data."_id" = :2
```

---

## Bucket Tests

### BUCKET001: Basic $bucket - price ranges

**Description:** Tests $bucket to group products by price ranges (no default needed - all values covered)  
**Collection:** `products`  
**Operator:** `$bucket`  

**MongoDB Pipeline:**
```json
[
  {
    "$bucket": {
      "groupBy": "$price",
      "boundaries": [
        0,
        25,
        100,
        1000
      ],
      "output": {
        "count": {
          "$sum": 1
        },
        "products": {
          "$push": "$name"
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT CASE WHEN base.data.price >= 0 AND base.data.price < 25 THEN 0 WHEN base.data.price >= 25 AND base.data.price < 100 THEN 25 WHEN base.data.price >= 100 AND base.data.price < 1000 THEN 100 END AS "_id", SUM(:1) AS count, JSON_ARRAYAGG(base.data.name) AS products
FROM products base
GROUP BY CASE WHEN base.data.price >= 0 AND base.data.price < 25 THEN 0 WHEN base.data.price >= 25 AND base.data.price < 100 THEN 25 WHEN base.data.price >= 100 AND base.data.price < 1000 THEN 100 END
ORDER BY "_id"
```

---

### BUCKET002: $bucket - salary bands

**Description:** Tests $bucket to group employees by salary bands (all values covered)  
**Collection:** `employees`  
**Operator:** `$bucket`  

**MongoDB Pipeline:**
```json
[
  {
    "$bucket": {
      "groupBy": "$salary",
      "boundaries": [
        60000,
        75000,
        90000,
        110000
      ],
      "output": {
        "count": {
          "$sum": 1
        },
        "avgBonus": {
          "$avg": "$bonus"
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT CASE WHEN base.data.salary >= 60000 AND base.data.salary < 75000 THEN 60000 WHEN base.data.salary >= 75000 AND base.data.salary < 90000 THEN 75000 WHEN base.data.salary >= 90000 AND base.data.salary < 110000 THEN 90000 END AS "_id", SUM(:1) AS count, AVG(base.data.bonus) AS avgBonus
FROM employees base
GROUP BY CASE WHEN base.data.salary >= 60000 AND base.data.salary < 75000 THEN 60000 WHEN base.data.salary >= 75000 AND base.data.salary < 90000 THEN 75000 WHEN base.data.salary >= 90000 AND base.data.salary < 110000 THEN 90000 END
ORDER BY "_id"
```

---

## BucketAuto Tests

### BUCKETAUTO001: Basic $bucketAuto - auto price groups

**Description:** Tests $bucketAuto to automatically group products by price  
**Collection:** `products`  
**Operator:** `$bucketAuto`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$bucketAuto": {
      "groupBy": "$price",
      "buckets": 3,
      "output": {
        "count": {
          "$sum": 1
        },
        "avgPrice": {
          "$avg": "$price"
        }
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT bucket_id, COUNT(*) AS count, AVG(groupby_value) AS avgPrice FROM (
  SELECT base.data.price AS groupby_value, NTILE(3) OVER (ORDER BY base.data.price) AS bucket_id
FROM products base
WHERE base.data.active = :1)
GROUP BY bucket_id
ORDER BY bucket_id
```

---

### BUCKETAUTO002: $bucketAuto - employee salary distribution

**Description:** Tests $bucketAuto to distribute employees into salary quartiles  
**Collection:** `employees`  
**Operator:** `$bucketAuto`  

**MongoDB Pipeline:**
```json
[
  {
    "$bucketAuto": {
      "groupBy": "$salary",
      "buckets": 4,
      "output": {
        "count": {
          "$sum": 1
        },
        "minSalary": {
          "$min": "$salary"
        },
        "maxSalary": {
          "$max": "$salary"
        }
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT bucket_id, COUNT(*) AS count, MIN(groupby_value) AS minSalary, MAX(groupby_value) AS maxSalary FROM (
  SELECT base.data.salary AS groupby_value, NTILE(4) OVER (ORDER BY base.data.salary) AS bucket_id
FROM employees base)
GROUP BY bucket_id
ORDER BY bucket_id
```

---

## Comparison Tests

### CMP001: Equality match - string

**Description:** Tests $eq operator with string field  
**Collection:** `sales`  
**Operator:** `$eq`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "status": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.status AS status
FROM sales base
WHERE base.data.status = :1
```

---

### CMP002: Greater than - numeric

**Description:** Tests $gt operator with numeric field  
**Collection:** `sales`  
**Operator:** `$gt`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "amount": {
        "$gt": 200
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "amount": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.amount AS amount
FROM sales base
WHERE CAST(base.data.amount AS NUMBER) > :1
```

---

### CMP003: Greater than or equal - numeric

**Description:** Tests $gte operator with numeric field  
**Collection:** `employees`  
**Operator:** `$gte`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "salary": {
        "$gte": 90000
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "salary": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.salary AS salary
FROM employees base
WHERE CAST(base.data.salary AS NUMBER) >= :1
```

---

### CMP004: Less than - numeric

**Description:** Tests $lt operator with numeric field  
**Collection:** `products`  
**Operator:** `$lt`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "price": {
        "$lt": 50
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "price": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.price AS price
FROM products base
WHERE CAST(base.data.price AS NUMBER) < :1
```

---

### CMP005: Less than or equal - numeric

**Description:** Tests $lte operator with numeric field  
**Collection:** `employees`  
**Operator:** `$lte`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "yearsOfService": {
        "$lte": 2
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "yearsOfService": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.yearsOfService AS yearsOfService
FROM employees base
WHERE CAST(base.data.yearsOfService AS NUMBER) <= :1
```

---

### CMP006: Not equal - string

**Description:** Tests $ne operator with string field  
**Collection:** `sales`  
**Operator:** `$ne`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": {
        "$ne": "completed"
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "status": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.status AS status
FROM sales base
WHERE base.data.status <> :1
```

---

### CMP007: In array - string values

**Description:** Tests $in operator with array of strings  
**Collection:** `sales`  
**Operator:** `$in`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": {
        "$in": [
          "completed",
          "pending"
        ]
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "status": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.status AS status
FROM sales base
WHERE base.data.status IN (:1, :2)
```

---

### CMP008: Not in array - string values

**Description:** Tests $nin operator with array of strings  
**Collection:** `sales`  
**Operator:** `$nin`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "region": {
        "$nin": [
          "north",
          "south"
        ]
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "region": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.region AS region
FROM sales base
WHERE base.data.region NOT IN (:1, :2)
```

---

### CMP009: Field exists - true

**Description:** Tests $exists operator to match documents where field exists  
**Collection:** `sales`  
**Operator:** `$exists`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "metadata": {
        "$exists": true
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId
FROM sales base
WHERE JSON_EXISTS(base.data, '$.metadata')
```

---

### CMP010: Field exists - false

**Description:** Tests $exists operator to match documents where field does not exist  
**Collection:** `sales`  
**Operator:** `$exists`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "metadata": {
        "$exists": false
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId
FROM sales base
WHERE NOT JSON_EXISTS(base.data, '$.metadata')
```

---

## Complex Tests

### COMPLEX001: Complex pipeline - match, group, sort

**Description:** Tests complex pipeline with multiple stages  
**Collection:** `sales`  
**Operator:** `multiple`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": {
        "$in": [
          "completed",
          "processing"
        ]
      }
    }
  },
  {
    "$group": {
      "_id": "$region",
      "totalSales": {
        "$sum": "$amount"
      },
      "orderCount": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "totalSales": -1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.region AS "_id", SUM(base.data.amount) AS totalSales, SUM(:1) AS orderCount
FROM sales base
WHERE base.data.status IN (:2, :3)
GROUP BY base.data.region
ORDER BY totalSales DESC
```

---

### COMPLEX002: Complex pipeline - filter, project, sort, limit

**Description:** Tests complex pipeline with filter, projection, sort, and limit  
**Collection:** `employees`  
**Operator:** `multiple`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "department": 1,
      "totalComp": {
        "$add": [
          "$salary",
          "$bonus"
        ]
      }
    }
  },
  {
    "$sort": {
      "totalComp": -1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.department AS department, (base.data.salary + base.data.bonus) AS totalComp
FROM employees base
WHERE base.data.active = :1
ORDER BY base.data.totalComp DESC
FETCH FIRST 5 ROWS ONLY
```

---

### COMPLEX003: Complex pipeline - nested AND/OR

**Description:** Tests complex pipeline with nested logical operators  
**Collection:** `sales`  
**Operator:** `multiple`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "$and": [
        {
          "$or": [
            {
              "category": "electronics"
            },
            {
              "category": "jewelry"
            }
          ]
        },
        {
          "amount": {
            "$gte": 100
          }
        }
      ]
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "category": 1,
      "amount": 1
    }
  },
  {
    "$sort": {
      "amount": -1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.category AS category, base.data.amount AS amount
FROM sales base
WHERE ((base.data.category = :1) OR (base.data.category = :2)) AND (CAST(base.data.amount AS NUMBER) >= :3)
ORDER BY base.data.amount DESC
```

---

### COMPLEX004: Complex pipeline - lookup with group

**Description:** Tests complex pipeline combining $lookup and $group  
**Collection:** `sales`  
**Operator:** `multiple`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$lookup": {
      "from": "customers",
      "localField": "customerId",
      "foreignField": "_id",
      "as": "customer"
    }
  },
  {
    "$unwind": "$customer"
  },
  {
    "$group": {
      "_id": "$customer.tier",
      "totalAmount": {
        "$sum": "$amount"
      },
      "orderCount": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT customers_1.data.tier AS "_id", SUM(base.data.amount) AS totalAmount, SUM(:1) AS orderCount
FROM sales base LEFT
OUTER JOIN customers customers_1 ON JSON_VALUE(base.data, '$.customerId') = JSON_VALUE(customers_1.data, '$._id')
WHERE base.data.status = :2
GROUP BY customers_1.data.tier
ORDER BY "_id"
```

---

### COMPLEX005: Complex pipeline - string operations with grouping

**Description:** Tests complex pipeline with string operations and grouping  
**Collection:** `employees`  
**Operator:** `multiple`  

**MongoDB Pipeline:**
```json
[
  {
    "$addFields": {
      "deptUpper": {
        "$toUpper": "$department"
      }
    }
  },
  {
    "$group": {
      "_id": "$deptUpper",
      "avgSalary": {
        "$avg": "$salary"
      },
      "headcount": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT UPPER(base.data.department) AS "_id", AVG(base.data.salary) AS avgSalary, SUM(:1) AS headcount, UPPER(base.data.department) AS deptUpper
FROM employees base
GROUP BY UPPER(base.data.department)
ORDER BY "_id"
```

---

### COMPLEX006: Unwind then group

**Description:** Tests flattening array with $unwind then aggregating with $group  
**Collection:** `sales`  
**Operator:** `$unwind+$group`  

**MongoDB Pipeline:**
```json
[
  {
    "$unwind": "$items"
  },
  {
    "$group": {
      "_id": "$items.product",
      "totalQuantity": {
        "$sum": "$items.qty"
      },
      "totalRevenue": {
        "$sum": {
          "$multiply": [
            "$items.qty",
            "$items.price"
          ]
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT unwind_1.value.product AS "_id", SUM(unwind_1.value.qty) AS totalQuantity, SUM((unwind_1.value.qty * unwind_1.value.price)) AS totalRevenue
FROM sales base, JSON_TABLE(base.data, '$.items[*]' COLUMNS (value JSON PATH '$')) unwind_1
GROUP BY unwind_1.value.product
ORDER BY "_id"
```

---

### COMPLEX007: Group sort limit - Top N

**Description:** Tests Top N pattern - aggregate, sort, and limit  
**Collection:** `sales`  
**Operator:** `$group+$sort+$limit`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$category",
      "totalAmount": {
        "$sum": "$amount"
      },
      "orderCount": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "totalAmount": -1
    }
  },
  {
    "$limit": 3
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.category AS "_id", SUM(base.data.amount) AS totalAmount, SUM(:1) AS orderCount
FROM sales base
GROUP BY base.data.category
ORDER BY totalAmount DESC
FETCH FIRST 3 ROWS ONLY
```

---

### COMPLEX008: Union then aggregate

**Description:** Tests combining collections with $unionWith then aggregating  
**Collection:** `sales`  
**Operator:** `$unionWith+$group`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "source": "sales",
      "amount": 1
    }
  },
  {
    "$unionWith": {
      "coll": "products",
      "pipeline": [
        {
          "$project": {
            "source": "products",
            "amount": "$price"
          }
        }
      ]
    }
  },
  {
    "$group": {
      "_id": "$source",
      "totalAmount": {
        "$sum": "$amount"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT source AS "_id", SUM(amount) AS totalAmount FROM (
  SELECT :1 AS source, base.data.amount AS amount
FROM sales base
UNION ALL SELECT :2 AS source, base.data.price AS amount
FROM products base)
GROUP BY source
```

---

## Conditional Tests

### COND001: Conditional with $cond

**Description:** Tests $cond operator for conditional logic  
**Collection:** `products`  
**Operator:** `$cond`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "priceCategory": {
        "$cond": {
          "if": {
            "$gte": [
              "$price",
              100
            ]
          },
          "then": "expensive",
          "else": "affordable"
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, CASE WHEN base.data.price >= :1 THEN :2 ELSE :3 END AS priceCategory
FROM products base
ORDER BY base.data."_id"
```

---

### COND002: IfNull handling

**Description:** Tests $ifNull operator for null handling  
**Collection:** `sales`  
**Operator:** `$ifNull`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "discountApplied": {
        "$ifNull": [
          "$discount",
          0
        ]
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, NVL(CAST(base.data.discount AS NUMBER), :1) AS discountApplied
FROM sales base
ORDER BY base.data.orderId
```

---

### COND003: Nested $cond expressions

**Description:** Tests nested $cond for multi-branch logic  
**Collection:** `products`  
**Operator:** `$cond`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "priceRange": {
        "$cond": {
          "if": {
            "$gte": [
              "$price",
              200
            ]
          },
          "then": "high",
          "else": {
            "$cond": {
              "if": {
                "$gte": [
                  "$price",
                  50
                ]
              },
              "then": "medium",
              "else": "low"
            }
          }
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, CASE WHEN base.data.price >= :1 THEN :2 ELSE CASE WHEN base.data.price >= :3 THEN :4 ELSE :5 END END AS priceRange
FROM products base
ORDER BY base.data."_id"
```

---

## Count Tests

### COUNT001: $count - count all documents

**Description:** Tests $count stage to count documents  
**Collection:** `employees`  
**Operator:** `$count`  

**MongoDB Pipeline:**
```json
[
  {
    "$count": "totalEmployees"
  }
]
```

**Generated SQL:**
```sql
SELECT JSON_OBJECT('totalEmployees' VALUE COUNT(*)) AS data
FROM employees base
```

---

### COUNT002: $count - count after filter

**Description:** Tests $count stage after $match filter  
**Collection:** `sales`  
**Operator:** `$count`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$count": "completedOrders"
  }
]
```

**Generated SQL:**
```sql
SELECT JSON_OBJECT('completedOrders' VALUE COUNT(*)) AS data
FROM sales base
WHERE base.data.status = :1
```

---

### COUNT003: $count - count with complex filter

**Description:** Tests $count stage with complex $match conditions  
**Collection:** `employees`  
**Operator:** `$count`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "$and": [
        {
          "active": true
        },
        {
          "salary": {
            "$gte": 80000
          }
        }
      ]
    }
  },
  {
    "$count": "highEarningActiveEmployees"
  }
]
```

**Generated SQL:**
```sql
SELECT JSON_OBJECT('highEarningActiveEmployees' VALUE COUNT(*)) AS data
FROM employees base
WHERE (base.data.active = :1) AND (CAST(base.data.salary AS NUMBER) >= :2)
```

---

## Date Tests

### DATE001: Extract year from date

**Description:** Tests $year operator to extract year component  
**Collection:** `events`  
**Operator:** `$year`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "title": 1,
      "eventYear": {
        "$year": "$eventDate"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.title AS title, EXTRACT(YEAR FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS eventYear
FROM events base
ORDER BY base.data."_id"
```

---

### DATE002: Extract month from date

**Description:** Tests $month operator to extract month component  
**Collection:** `events`  
**Operator:** `$month`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "title": 1,
      "eventMonth": {
        "$month": "$eventDate"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.title AS title, EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS eventMonth
FROM events base
ORDER BY base.data."_id"
```

---

### DATE003: Extract day from date

**Description:** Tests $dayOfMonth operator to extract day component  
**Collection:** `events`  
**Operator:** `$dayOfMonth`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "title": 1,
      "eventDay": {
        "$dayOfMonth": "$eventDate"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.title AS title, EXTRACT(DAY FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS eventDay
FROM events base
ORDER BY base.data."_id"
```

---

### DATE004: Extract hour from date

**Description:** Tests $hour operator to extract hour component  
**Collection:** `events`  
**Operator:** `$hour`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "title": 1,
      "eventHour": {
        "$hour": "$eventDate"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.title AS title, EXTRACT(HOUR FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS eventHour
FROM events base
ORDER BY base.data."_id"
```

---

### DATE005: Group events by month

**Description:** Tests $month with $group for monthly aggregation  
**Collection:** `events`  
**Operator:** `$month/$group`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": {
        "$month": "$eventDate"
      },
      "eventCount": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS "_id", SUM(:1) AS eventCount
FROM events base
GROUP BY EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
ORDER BY "_id"
```

---

### DATE006: Extract minute from date

**Description:** Tests $minute operator to extract minute component from date  
**Collection:** `events`  
**Operator:** `$minute`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "eventName": 1,
      "minute": {
        "$minute": "$eventDate"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.eventName AS eventName, EXTRACT(MINUTE FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS minute
FROM events base
```

---

### DATE007: Extract second from date

**Description:** Tests $second operator to extract second component from date  
**Collection:** `events`  
**Operator:** `$second`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "eventName": 1,
      "second": {
        "$second": "$eventDate"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.eventName AS eventName, EXTRACT(SECOND FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS second
FROM events base
```

---

### DATE008: Day of week

**Description:** Tests $dayOfWeek operator - returns 1 (Sunday) to 7 (Saturday)  
**Collection:** `events`  
**Operator:** `$dayOfWeek`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "eventName": 1,
      "dayOfWeek": {
        "$dayOfWeek": "$eventDate"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.eventName AS eventName, TO_NUMBER(TO_CHAR(TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'), 'D')) AS dayOfWeek
FROM events base
```

---

### DATE009: Day of year

**Description:** Tests $dayOfYear operator - returns day of year (1-366)  
**Collection:** `events`  
**Operator:** `$dayOfYear`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "eventName": 1,
      "dayOfYear": {
        "$dayOfYear": "$eventDate"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.eventName AS eventName, TO_NUMBER(TO_CHAR(TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'), 'DDD')) AS dayOfYear
FROM events base
```

---

### DATE010: Week of year

**Description:** Tests $week operator - returns ISO week number  
**Collection:** `events`  
**Operator:** `$week`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "eventName": 1,
      "week": {
        "$week": "$eventDate"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.eventName AS eventName, TO_NUMBER(TO_CHAR(TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'), 'IW')) AS week
FROM events base
```

---

### DATE012: Group by month from date

**Description:** Tests grouping by month extracted from date field  
**Collection:** `events`  
**Operator:** `$month`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": {
        "$month": "$eventDate"
      },
      "eventCount": {
        "$sum": 1
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS "_id", SUM(:1) AS eventCount
FROM events base
GROUP BY EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
ORDER BY "_id"
```

---

## Edge Tests

### EDGE001: Zero values in aggregation

**Description:** Tests aggregation with zero values  
**Collection:** `sales`  
**Operator:** `$sum`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "quantity": 0
    }
  },
  {
    "$group": {
      "_id": null,
      "totalAmount": {
        "$sum": "$amount"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT SUM(base.data.amount) AS totalAmount
FROM sales base
WHERE CAST(base.data.quantity AS NUMBER) = :1
```

---

### EDGE002: Negative values in aggregation

**Description:** Tests aggregation with negative values  
**Collection:** `sales`  
**Operator:** `$sum`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "amount": {
        "$lt": 0
      }
    }
  },
  {
    "$group": {
      "_id": null,
      "totalRefunds": {
        "$sum": "$amount"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT SUM(base.data.amount) AS totalRefunds
FROM sales base
WHERE CAST(base.data.amount AS NUMBER) < :1
```

---

### EDGE003: Empty result set

**Description:** Tests query that returns no results  
**Collection:** `sales`  
**Operator:** `$match`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "nonexistent_status"
    }
  },
  {
    "$project": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id"
FROM sales base
WHERE base.data.status = :1
```

---

## Expression Tests

### EXPR001: Switch expression

**Description:** Tests $switch operator for multi-branch conditional logic  
**Collection:** `employees`  
**Operator:** `$switch`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "salary": 1,
      "salaryBand": {
        "$switch": {
          "branches": [
            {
              "case": {
                "$lt": [
                  "$salary",
                  60000
                ]
              },
              "then": "Junior"
            },
            {
              "case": {
                "$lt": [
                  "$salary",
                  90000
                ]
              },
              "then": "Mid"
            },
            {
              "case": {
                "$gte": [
                  "$salary",
                  90000
                ]
              },
              "then": "Senior"
            }
          ],
          "default": "Unknown"
        }
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.salary AS salary, CASE WHEN base.data.salary < :1 THEN :2 WHEN base.data.salary < :3 THEN :4 WHEN base.data.salary >= :5 THEN :6 ELSE :7 END AS salaryBand
FROM employees base
```

---

### EXPR002: Nested conditional

**Description:** Tests deeply nested $cond expressions  
**Collection:** `sales`  
**Operator:** `$cond`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "status": 1,
      "priority": {
        "$cond": {
          "if": {
            "$eq": [
              "$status",
              "completed"
            ]
          },
          "then": "low",
          "else": {
            "$cond": {
              "if": {
                "$eq": [
                  "$status",
                  "pending"
                ]
              },
              "then": "high",
              "else": "medium"
            }
          }
        }
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.status AS status, CASE WHEN base.data.status = :1 THEN :2 ELSE CASE WHEN base.data.status = :3 THEN :4 ELSE :5 END END AS priority
FROM sales base
```

---

## Facet Tests

### FACET001: Basic $facet - multiple aggregations

**Description:** Tests $facet for parallel aggregation pipelines  
**Collection:** `sales`  
**Operator:** `$facet`  

**MongoDB Pipeline:**
```json
[
  {
    "$facet": {
      "byStatus": [
        {
          "$group": {
            "_id": "$status",
            "count": {
              "$sum": 1
            }
          }
        },
        {
          "$sort": {
            "_id": 1
          }
        }
      ],
      "byRegion": [
        {
          "$group": {
            "_id": "$region",
            "totalAmount": {
              "$sum": "$amount"
            }
          }
        },
        {
          "$sort": {
            "_id": 1
          }
        }
      ]
    }
  }
]
```

**Generated SQL:**
```sql
SELECT JSON_OBJECT('byStatus' VALUE (
  SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE "_id", 'count' VALUE count)
ORDER BY "_id") FROM (
  SELECT base.data.status AS "_id", SUM(:1) AS count
FROM sales base
GROUP BY base.data.status)), 'byRegion' VALUE (
  SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE "_id", 'totalAmount' VALUE totalAmount)
ORDER BY "_id") FROM (
  SELECT base.data.region AS "_id", SUM(base.data.amount) AS totalAmount
FROM sales base
GROUP BY base.data.region))) AS data FROM DUAL
```

---

### FACET002: $facet - product analysis

**Description:** Tests $facet for multi-faceted product analysis  
**Collection:** `products`  
**Operator:** `$facet`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$facet": {
      "categorySummary": [
        {
          "$group": {
            "_id": "$category",
            "count": {
              "$sum": 1
            },
            "avgPrice": {
              "$avg": "$price"
            }
          }
        },
        {
          "$sort": {
            "_id": 1
          }
        }
      ],
      "priceStats": [
        {
          "$group": {
            "_id": null,
            "minPrice": {
              "$min": "$price"
            },
            "maxPrice": {
              "$max": "$price"
            },
            "avgPrice": {
              "$avg": "$price"
            }
          }
        }
      ]
    }
  }
]
```

**Generated SQL:**
```sql
SELECT JSON_OBJECT('categorySummary' VALUE (
  SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE "_id", 'count' VALUE count, 'avgPrice' VALUE avgPrice)
ORDER BY "_id") FROM (
  SELECT base.data.category AS "_id", SUM(:1) AS count, AVG(base.data.price) AS avgPrice
FROM products base
WHERE base.data.active = :2
GROUP BY base.data.category)), 'priceStats' VALUE (
  SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE NULL, 'minPrice' VALUE minPrice, 'maxPrice' VALUE maxPrice, 'avgPrice' VALUE avgPrice)) FROM (
  SELECT MIN(base.data.price) AS minPrice, MAX(base.data.price) AS maxPrice, AVG(base.data.price) AS avgPrice
FROM products base
WHERE base.data.active = :3))) AS data FROM DUAL
```

---

### FACET003: $facet - employee dashboard

**Description:** Tests $facet for employee dashboard with multiple views  
**Collection:** `employees`  
**Operator:** `$facet`  

**MongoDB Pipeline:**
```json
[
  {
    "$facet": {
      "departmentCounts": [
        {
          "$group": {
            "_id": "$department",
            "count": {
              "$sum": 1
            }
          }
        },
        {
          "$sort": {
            "count": -1
          }
        }
      ],
      "topEarners": [
        {
          "$sort": {
            "salary": -1
          }
        },
        {
          "$limit": 3
        },
        {
          "$project": {
            "_id": 1,
            "name": 1,
            "salary": 1
          }
        }
      ],
      "totalStats": [
        {
          "$group": {
            "_id": null,
            "totalEmployees": {
              "$sum": 1
            },
            "avgSalary": {
              "$avg": "$salary"
            },
            "totalPayroll": {
              "$sum": "$salary"
            }
          }
        }
      ]
    }
  }
]
```

**Generated SQL:**
```sql
SELECT JSON_OBJECT('departmentCounts' VALUE (
  SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE "_id", 'count' VALUE count)
ORDER BY count DESC) FROM (
  SELECT base.data.department AS "_id", SUM(:1) AS count
FROM employees base
GROUP BY base.data.department)), 'topEarners' VALUE (
  SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE "_id", 'name' VALUE name, 'salary' VALUE salary)
ORDER BY salary DESC) FROM (
  SELECT id AS "_id", base.data.name AS name, base.data.salary AS salary
FROM employees base
ORDER BY base.data.salary DESC
FETCH FIRST 3 ROWS ONLY)), 'totalStats' VALUE (
  SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE NULL, 'totalEmployees' VALUE totalEmployees, 'avgSalary' VALUE avgSalary, 'totalPayroll' VALUE totalPayroll)) FROM (
  SELECT SUM(:2) AS totalEmployees, AVG(base.data.salary) AS avgSalary, SUM(base.data.salary) AS totalPayroll
FROM employees base))) AS data FROM DUAL
```

---

## GraphLookup Tests

### GRAPHLOOKUP001: $graphLookup - with restrictSearchWithMatch

**Description:** Tests $graphLookup with restrictSearchWithMatch filter  
**Collection:** `employees`  
**Operator:** `$graphLookup`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "_id": 1
    }
  },
  {
    "$graphLookup": {
      "from": "employees",
      "startWith": "$department",
      "connectFromField": "department",
      "connectToField": "department",
      "as": "colleagues",
      "maxDepth": 0,
      "restrictSearchWithMatch": {
        "active": true
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "colleagueCount": {
        "$size": "$colleagues"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, JSON_VALUE(base.data, '$.colleagues.size()') AS colleagueCount, colleagues_cte.colleagues AS colleagues
FROM employees base LEFT
OUTER JOIN LATERAL (
  SELECT JSON_ARRAYAGG(g.data) AS colleagues
FROM employees g
WHERE JSON_VALUE(g.data, '$.department') = JSON_VALUE(base.data, '$.department') AND JSON_VALUE(g.data, '$.active') = true) colleagues_cte ON 1=1
WHERE CAST(base.data."_id" AS NUMBER) = :1
```

---

## Logical Tests

### LOG001: Logical AND - two conditions

**Description:** Tests $and operator with two conditions  
**Collection:** `sales`  
**Operator:** `$and`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "$and": [
        {
          "status": "completed"
        },
        {
          "category": "electronics"
        }
      ]
    }
  },
  {
    "$project": {
      "_id": 1,
      "status": 1,
      "category": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.status AS status, base.data.category AS category
FROM sales base
WHERE (base.data.status = :1) AND (base.data.category = :2)
```

---

### LOG002: Logical AND - three conditions

**Description:** Tests $and operator with three conditions  
**Collection:** `employees`  
**Operator:** `$and`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "$and": [
        {
          "department": "Engineering"
        },
        {
          "active": true
        },
        {
          "salary": {
            "$gte": 90000
          }
        }
      ]
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "department": 1,
      "salary": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.department AS department, base.data.salary AS salary
FROM employees base
WHERE (base.data.department = :1) AND (base.data.active = :2) AND (CAST(base.data.salary AS NUMBER) >= :3)
```

---

### LOG003: Logical OR - two conditions

**Description:** Tests $or operator with two conditions  
**Collection:** `sales`  
**Operator:** `$or`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "$or": [
        {
          "status": "cancelled"
        },
        {
          "status": "refunded"
        }
      ]
    }
  },
  {
    "$project": {
      "_id": 1,
      "status": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.status AS status
FROM sales base
WHERE (base.data.status = :1) OR (base.data.status = :2)
```

---

### LOG004: Logical OR with AND

**Description:** Tests combination of $or and $and operators  
**Collection:** `employees`  
**Operator:** `$or/$and`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "$or": [
        {
          "department": "Engineering"
        },
        {
          "$and": [
            {
              "department": "Sales"
            },
            {
              "active": true
            }
          ]
        }
      ]
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "department": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.department AS department
FROM employees base
WHERE (base.data.department = :1) OR ((base.data.department = :2) AND (base.data.active = :3))
```

---

### LOG005: Logical NOT

**Description:** Tests $not operator  
**Collection:** `products`  
**Operator:** `$not`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "price": {
        "$not": {
          "$gt": 100
        }
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "price": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.price AS price
FROM products base
WHERE NOT (CAST(base.data.price AS NUMBER) > :1)
```

---

### LOG006: Nor condition

**Description:** Tests $nor operator - none of the conditions match  
**Collection:** `sales`  
**Operator:** `$nor`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "$nor": [
        {
          "status": "completed"
        },
        {
          "status": "pending"
        }
      ]
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "status": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.status AS status
FROM sales base
WHERE NOT ((base.data.status = :1) OR (base.data.status = :2))
```

---

## Lookup Tests

### LOOKUP001: Basic $lookup join

**Description:** Tests $lookup stage for basic left outer join  
**Collection:** `sales`  
**Operator:** `$lookup`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$lookup": {
      "from": "customers",
      "localField": "customerId",
      "foreignField": "_id",
      "as": "customerInfo"
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "customerTier": {
        "$arrayElemAt": [
          "$customerInfo.tier",
          0
        ]
      }
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, JSON_VALUE(base.data, '$.customerInfo.tier[0]') AS customerTier
FROM sales base LEFT
OUTER JOIN customers customers_1 ON JSON_VALUE(base.data, '$.customerId') = JSON_VALUE(customers_1.data, '$._id')
WHERE base.data.status = :1
ORDER BY base.data.orderId
```

---

### LOOKUP002: $lookup with inventory

**Description:** Tests $lookup stage joining products with inventory  
**Collection:** `products`  
**Operator:** `$lookup`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$lookup": {
      "from": "inventory",
      "localField": "_id",
      "foreignField": "productId",
      "as": "inventoryRecords"
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "warehouseCount": {
        "$size": "$inventoryRecords"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, (
  SELECT COUNT(*)
FROM inventory
WHERE JSON_VALUE(inventory.data, '$.productId') = JSON_VALUE(base.data, '$._id')) AS warehouseCount
FROM products base
WHERE base.data.active = :1
ORDER BY base.data."_id"
```

---

### LOOKUP003: Self join lookup

**Description:** Tests $lookup self-join on same collection  
**Collection:** `employees`  
**Operator:** `$lookup`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "_id": 1
    }
  },
  {
    "$lookup": {
      "from": "employees",
      "localField": "department",
      "foreignField": "department",
      "as": "colleagues"
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "department": 1,
      "colleagueCount": {
        "$size": "$colleagues"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.department AS department, (
  SELECT COUNT(*)
FROM employees
WHERE JSON_VALUE(employees.data, '$.department') = JSON_VALUE(base.data, '$.department')) AS colleagueCount
FROM employees base
WHERE CAST(base.data."_id" AS NUMBER) = :1
```

---

### LOOKUP004: Multiple lookups in pipeline

**Description:** Tests chain of multiple $lookup stages  
**Collection:** `sales`  
**Operator:** `$lookup`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "_id": "S001"
    }
  },
  {
    "$lookup": {
      "from": "customers",
      "localField": "customerId",
      "foreignField": "customerId",
      "as": "customer"
    }
  },
  {
    "$lookup": {
      "from": "inventory",
      "localField": "orderId",
      "foreignField": "productId",
      "as": "inventoryItems"
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "hasCustomer": {
        "$gt": [
          {
            "$size": "$customer"
          },
          0
        ]
      },
      "inventoryCount": {
        "$size": "$inventoryItems"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, (
  SELECT COUNT(*)
FROM customers
WHERE JSON_VALUE(customers.data, '$.customerId') = JSON_VALUE(base.data, '$.customerId')) > :1 AS hasCustomer, (
  SELECT COUNT(*)
FROM inventory
WHERE JSON_VALUE(inventory.data, '$.productId') = JSON_VALUE(base.data, '$.orderId')) AS inventoryCount
FROM sales base
WHERE base.data."_id" = :2
```

---

## Null handling Tests

### NULL001: Null equality comparison

**Description:** Tests matching documents where field equals null explicitly  
**Collection:** `sales`  
**Operator:** `$eq`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "discount": null
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "discount": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.discount AS discount
FROM sales base
WHERE NOT JSON_EXISTS(base.data, '$.discount?(@ != null)')
```

---

### NULL002: IfNull default value

**Description:** Tests $ifNull operator to replace null with default value  
**Collection:** `sales`  
**Operator:** `$ifNull`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "discountApplied": {
        "$ifNull": [
          "$discount",
          0
        ]
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, NVL(CAST(base.data.discount AS NUMBER), :1) AS discountApplied
FROM sales base
```

---

### NULL003: Null campaign field handling

**Description:** Tests matching nested field with null value in metadata.campaign  
**Collection:** `sales`  
**Operator:** `$eq`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "metadata.campaign": null
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "campaign": "$metadata.campaign"
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.metadata.campaign AS campaign
FROM sales base
WHERE NOT JSON_EXISTS(base.data, '$.metadata.campaign?(@ != null)')
```

---

### NULL004: Null in accumulator - sum

**Description:** Tests $sum accumulator behavior with null values in group  
**Collection:** `sales`  
**Operator:** `$sum`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$region",
      "totalDiscount": {
        "$sum": "$discount"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.region AS "_id", SUM(base.data.discount) AS totalDiscount
FROM sales base
GROUP BY base.data.region
ORDER BY "_id"
```

---

### NULL005: Null in accumulator - avg

**Description:** Tests $avg accumulator behavior with null values - nulls are ignored  
**Collection:** `sales`  
**Operator:** `$avg`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": "$category",
      "avgDiscount": {
        "$avg": "$discount"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.category AS "_id", AVG(base.data.discount) AS avgDiscount
FROM sales base
GROUP BY base.data.category
ORDER BY "_id"
```

---

## Object Tests

### OBJ001: Merge objects in projection

**Description:** Tests $mergeObjects operator to combine objects in projection  
**Collection:** `sales`  
**Operator:** `$mergeObjects`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "_id": "S001"
    }
  },
  {
    "$project": {
      "_id": 1,
      "merged": {
        "$mergeObjects": [
          {
            "orderId": "$orderId"
          },
          "$metadata"
        ]
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", JSON_MERGEPATCH(JSON_OBJECT('orderId' VALUE base.data.orderId), JSON_QUERY(base.data, '$.metadata')) AS merged
FROM sales base
WHERE base.data."_id" = :1
```

---

## Redact Tests

### REDACT001: $redact - document level filtering with PRUNE

**Description:** Tests $redact stage to filter documents based on condition  
**Collection:** `employees`  
**Operator:** `$redact`  

**MongoDB Pipeline:**
```json
[
  {
    "$redact": {
      "$cond": {
        "if": {
          "$gte": [
            "$salary",
            80000
          ]
        },
        "then": "$$KEEP",
        "else": "$$PRUNE"
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "salary": 1
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.salary AS salary
FROM employees base
WHERE CASE WHEN base.data.salary >= :1 THEN :2 ELSE :3 END <> :4
ORDER BY base.data."_id"
```

---

### REDACT002: $redact - with DESCEND for nested documents

**Description:** Tests $redact stage with DESCEND option  
**Collection:** `sales`  
**Operator:** `$redact`  

**MongoDB Pipeline:**
```json
[
  {
    "$redact": {
      "$cond": {
        "if": {
          "$eq": [
            "$status",
            "completed"
          ]
        },
        "then": "$$DESCEND",
        "else": "$$PRUNE"
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "status": 1
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.status AS status
FROM sales base
WHERE CASE WHEN base.data.status = :1 THEN :2 ELSE :3 END <> :4
ORDER BY base.data.orderId
```

---

## ReplaceRoot Tests

### REPLACEROOT001: Replace root with nested object

**Description:** Tests $replaceRoot to promote nested object to root  
**Collection:** `sales`  
**Operator:** `$replaceRoot`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "metadata": {
        "$ne": null
      }
    }
  },
  {
    "$replaceRoot": {
      "newRoot": {
        "orderId": "$orderId",
        "source": "$metadata.source",
        "campaign": "$metadata.campaign"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.orderId AS orderId, base.data.metadata.source AS source, base.data.metadata.campaign AS campaign
FROM sales base
WHERE JSON_EXISTS(base.data, '$.metadata?(@ != null)')
```

---

## Sample Tests

### SAMPLE001: $sample - random document selection

**Description:** Tests $sample stage to randomly select documents  
**Collection:** `products`  
**Operator:** `$sample`  

**MongoDB Pipeline:**
```json
[
  {
    "$sample": {
      "size": 3
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name
FROM products base
ORDER BY DBMS_RANDOM.VALUE
FETCH FIRST 3 ROWS ONLY
```

---

### SAMPLE002: $sample - with subsequent processing

**Description:** Tests $sample stage followed by other stages  
**Collection:** `employees`  
**Operator:** `$sample`  

**MongoDB Pipeline:**
```json
[
  {
    "$sample": {
      "size": 5
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "department": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.department AS department
FROM employees base
ORDER BY DBMS_RANDOM.VALUE
FETCH FIRST 5 ROWS ONLY
```

---

## SetWindowFields Tests

### WINDOW001: $setWindowFields - rank by salary

**Description:** Tests $setWindowFields with $rank partitioned by department  
**Collection:** `employees`  
**Operator:** `$setWindowFields`  

**MongoDB Pipeline:**
```json
[
  {
    "$setWindowFields": {
      "partitionBy": "$department",
      "sortBy": {
        "salary": -1
      },
      "output": {
        "salaryRank": {
          "$rank": {}
        }
      }
    }
  },
  {
    "$match": {
      "salaryRank": 1
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "department": 1,
      "salary": 1,
      "salaryRank": 1
    }
  },
  {
    "$sort": {
      "department": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT id AS "_id", JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.department') AS department, JSON_VALUE(data, '$.salary') AS salary, salaryRank AS salaryRank FROM (
  SELECT id, data, RANK() OVER (PARTITION BY JSON_VALUE(base.data, '$.department')
ORDER BY JSON_VALUE(base.data, '$.salary') DESC) AS salaryRank
FROM employees base)
WHERE salaryRank = :1
ORDER BY JSON_VALUE(data, '$.department')
```

---

### WINDOW002: $setWindowFields - cumulative sum

**Description:** Tests $setWindowFields with cumulative $sum  
**Collection:** `sales`  
**Operator:** `$setWindowFields`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$setWindowFields": {
      "sortBy": {
        "orderDate": 1
      },
      "output": {
        "runningTotal": {
          "$sum": "$amount",
          "window": {
            "documents": [
              "unbounded",
              "current"
            ]
          }
        }
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "amount": 1,
      "runningTotal": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.amount AS amount, base.data.runningTotal AS runningTotal, SUM(JSON_VALUE(base.data, '$.amount' RETURNING NUMBER)) OVER (ORDER BY JSON_VALUE(base.data, '$.orderDate') ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS runningTotal
FROM sales base
WHERE base.data.status = :1
```

---

### WINDOW003: $setWindowFields - document number

**Description:** Tests $setWindowFields with $documentNumber  
**Collection:** `products`  
**Operator:** `$setWindowFields`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$setWindowFields": {
      "partitionBy": "$category",
      "sortBy": {
        "price": -1
      },
      "output": {
        "priceRankInCategory": {
          "$documentNumber": {}
        }
      }
    }
  },
  {
    "$match": {
      "priceRankInCategory": 1
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "category": 1,
      "price": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT id AS "_id", JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.category') AS category, JSON_VALUE(data, '$.price') AS price FROM (
  SELECT id, data, ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(base.data, '$.category')
ORDER BY JSON_VALUE(base.data, '$.price') DESC) AS priceRankInCategory
FROM products base
WHERE base.data.active = :1)
WHERE priceRankInCategory = :2
```

---

### WINDOW004: $setWindowFields - dense rank

**Description:** Tests $setWindowFields with $denseRank  
**Collection:** `employees`  
**Operator:** `$setWindowFields`  

**MongoDB Pipeline:**
```json
[
  {
    "$setWindowFields": {
      "sortBy": {
        "salary": -1
      },
      "output": {
        "denseRank": {
          "$denseRank": {}
        }
      }
    }
  },
  {
    "$match": {
      "denseRank": {
        "$lte": 3
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "salary": 1,
      "denseRank": 1
    }
  },
  {
    "$sort": {
      "denseRank": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT id AS "_id", JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary') AS salary, denseRank AS denseRank FROM (
  SELECT id, data, DENSE_RANK() OVER (ORDER BY JSON_VALUE(base.data, '$.salary') DESC) AS denseRank
FROM employees base)
WHERE denseRank <= :1
ORDER BY denseRank
```

---

## Stage Tests

### STG001: Limit results

**Description:** Tests $limit stage  
**Collection:** `sales`  
**Operator:** `$limit`  

**MongoDB Pipeline:**
```json
[
  {
    "$sort": {
      "orderId": 1
    }
  },
  {
    "$limit": 3
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId
FROM sales base
ORDER BY base.data.orderId
FETCH FIRST 3 ROWS ONLY
```

---

### STG002: Skip results

**Description:** Tests $skip stage  
**Collection:** `employees`  
**Operator:** `$skip`  

**MongoDB Pipeline:**
```json
[
  {
    "$sort": {
      "name": 1
    }
  },
  {
    "$skip": 5
  },
  {
    "$project": {
      "_id": 1,
      "name": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name
FROM employees base
ORDER BY base.data.name
OFFSET 5 ROWS
```

---

### STG003: Skip and limit combined

**Description:** Tests $skip and $limit stages combined (pagination)  
**Collection:** `products`  
**Operator:** `$skip/$limit`  

**MongoDB Pipeline:**
```json
[
  {
    "$sort": {
      "price": -1
    }
  },
  {
    "$skip": 2
  },
  {
    "$limit": 3
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "price": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.price AS price
FROM products base
ORDER BY base.data.price DESC
OFFSET 2 ROWS
FETCH FIRST 3 ROWS ONLY
```

---

### STG004: Sort ascending

**Description:** Tests $sort stage with ascending order  
**Collection:** `employees`  
**Operator:** `$sort`  

**MongoDB Pipeline:**
```json
[
  {
    "$sort": {
      "salary": 1
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "salary": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.salary AS salary
FROM employees base
ORDER BY base.data.salary
```

---

### STG005: Sort descending

**Description:** Tests $sort stage with descending order  
**Collection:** `products`  
**Operator:** `$sort`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "active": true
    }
  },
  {
    "$sort": {
      "price": -1
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "price": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.price AS price
FROM products base
WHERE base.data.active = :1
ORDER BY base.data.price DESC
```

---

### STG006: Sort with multiple fields

**Description:** Tests $sort stage with multiple fields  
**Collection:** `employees`  
**Operator:** `$sort`  

**MongoDB Pipeline:**
```json
[
  {
    "$sort": {
      "department": 1,
      "salary": -1
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "department": 1,
      "salary": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.department AS department, base.data.salary AS salary
FROM employees base
ORDER BY base.data.department, base.data.salary DESC
```

---

### STG007: Project include fields

**Description:** Tests $project stage with field inclusion  
**Collection:** `sales`  
**Operator:** `$project`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$project": {
      "orderId": 1,
      "amount": 1,
      "tax": 1,
      "_id": 0
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data.orderId AS orderId, base.data.amount AS amount, base.data.tax AS tax
FROM sales base
WHERE base.data.status = :1
ORDER BY base.data.orderId
```

---

## String Tests

### STR001: String concatenation

**Description:** Tests $concat operator to join strings  
**Collection:** `employees`  
**Operator:** `$concat`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "fullInfo": {
        "$concat": [
          "$name",
          " - ",
          "$department"
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", (base.data.name || :1 || base.data.department) AS fullInfo
FROM employees base
ORDER BY base.data."_id"
```

---

### STR002: String to lowercase

**Description:** Tests $toLower operator  
**Collection:** `employees`  
**Operator:** `$toLower`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "deptLower": {
        "$toLower": "$department"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", LOWER(base.data.department) AS deptLower
FROM employees base
ORDER BY base.data."_id"
```

---

### STR003: String to uppercase

**Description:** Tests $toUpper operator  
**Collection:** `products`  
**Operator:** `$toUpper`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "nameUpper": {
        "$toUpper": "$name"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", UPPER(base.data.name) AS nameUpper
FROM products base
ORDER BY base.data."_id"
```

---

### STR004: Substring extraction

**Description:** Tests $substr operator to extract part of string  
**Collection:** `customers`  
**Operator:** `$substr`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "namePrefix": {
        "$substr": [
          "$name",
          0,
          4
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", SUBSTR(base.data.name, :1, :2) AS namePrefix
FROM customers base
ORDER BY base.data."_id"
```

---

### STR005: String length

**Description:** Tests $strLenCP operator to get string length  
**Collection:** `products`  
**Operator:** `$strLenCP`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "nameLength": {
        "$strLenCP": "$name"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, LENGTH(base.data.name) AS nameLength
FROM products base
ORDER BY base.data."_id"
```

---

### STR006: String trim

**Description:** Tests $trim operator (using existing data)  
**Collection:** `employees`  
**Operator:** `$trim`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "trimmedName": {
        "$trim": {
          "input": "$name"
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", TRIM(base.data.name) AS trimmedName
FROM employees base
ORDER BY base.data."_id"
```

---

### STR007: $split - split string into array

**Description:** Tests $split operator to split strings by delimiter  
**Collection:** `employees`  
**Operator:** `$split`  

**MongoDB Pipeline:**
```json
[
  {
    "$addFields": {
      "nameParts": {
        "$split": [
          "$name",
          " "
        ]
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "firstName": {
        "$arrayElemAt": [
          {
            "$split": [
              "$name",
              " "
            ]
          },
          0
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, REGEXP_SUBSTR(base.data.name, '[^'||:1||']+', 1, 1) AS firstName, (
  SELECT JSON_ARRAYAGG(REGEXP_SUBSTR(base.data.name, '[^' || :2 || ']+', 1, LEVEL))
FROM DUAL CONNECT BY REGEXP_SUBSTR(base.data.name, '[^' || :3 || ']+', 1, LEVEL) IS NOT NULL) AS nameParts
FROM employees base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### STR008: $indexOfCP - find substring position

**Description:** Tests $indexOfCP operator to find position of substring  
**Collection:** `employees`  
**Operator:** `$indexOfCP`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "aPosition": {
        "$indexOfCP": [
          "$name",
          "a"
        ]
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, CASE WHEN INSTR(base.data.name, :1) = 0 THEN -1 ELSE INSTR(base.data.name, :2) - 1 END AS aPosition
FROM employees base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### STR009: $regexMatch - regex pattern matching

**Description:** Tests $regexMatch operator for pattern matching  
**Collection:** `employees`  
**Operator:** `$regexMatch`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "hasVowelStart": {
        "$regexMatch": {
          "input": "$name",
          "regex": "^[AEIOUaeiou]"
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, CASE WHEN REGEXP_LIKE(base.data.name, :1) THEN 1 ELSE 0 END AS hasVowelStart
FROM employees base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### STR010: $replaceOne - replace first occurrence

**Description:** Tests $replaceOne operator to replace first match  
**Collection:** `employees`  
**Operator:** `$replaceOne`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "modifiedDept": {
        "$replaceOne": {
          "input": "$department",
          "find": "Engineering",
          "replacement": "Tech"
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, REGEXP_REPLACE(base.data.department, :1, :2, 1, 1) AS modifiedDept
FROM employees base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### STR011: $replaceAll - replace all occurrences

**Description:** Tests $replaceAll operator to replace all matches  
**Collection:** `employees`  
**Operator:** `$replaceAll`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "nameNoSpaces": {
        "$replaceAll": {
          "input": "$name",
          "find": " ",
          "replacement": "_"
        }
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, REGEXP_REPLACE(base.data.name, :1, :2) AS nameNoSpaces
FROM employees base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### STR012: Left trim

**Description:** Tests $ltrim operator to remove leading whitespace  
**Collection:** `employees`  
**Operator:** `$ltrim`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "trimmedName": {
        "$ltrim": {
          "input": "$name"
        }
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, TRIM(base.data.name) AS trimmedName
FROM employees base
```

---

### STR013: Right trim

**Description:** Tests $rtrim operator to remove trailing whitespace  
**Collection:** `employees`  
**Operator:** `$rtrim`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "trimmedName": {
        "$rtrim": {
          "input": "$name"
        }
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, TRIM(base.data.name) AS trimmedName
FROM employees base
```

---

### STR014: String case comparison

**Description:** Tests $strcasecmp operator for case-insensitive string comparison  
**Collection:** `employees`  
**Operator:** `$strcasecmp`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "department": 1,
      "compareResult": {
        "$strcasecmp": [
          "$department",
          "ENGINEERING"
        ]
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.department AS department, CASE WHEN UPPER(base.data.department) < UPPER(:1) THEN -1 WHEN UPPER(base.data.department) > UPPER(:2) THEN 1 ELSE 0 END AS compareResult
FROM employees base
```

---

## TypeConversion Tests

### TYPE001: $type - get field type

**Description:** Tests $type operator to get BSON type of a field  
**Collection:** `sales`  
**Operator:** `$type`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "statusType": {
        "$type": "$status"
      },
      "amountType": {
        "$type": "$amount"
      }
    }
  },
  {
    "$limit": 3
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", CASE WHEN base.data.status IS NULL THEN 'null' WHEN base.data.status IN ('true', 'false') THEN 'bool' WHEN REGEXP_LIKE(base.data.status, '^-?[0-9]+$') THEN 'int' WHEN REGEXP_LIKE(base.data.status, '^-?[0-9]+\.[0-9]+$') THEN 'double' ELSE 'string' END AS statusType, CASE WHEN base.data.amount IS NULL THEN 'null' WHEN base.data.amount IN ('true', 'false') THEN 'bool' WHEN REGEXP_LIKE(base.data.amount, '^-?[0-9]+$') THEN 'int' WHEN REGEXP_LIKE(base.data.amount, '^-?[0-9]+\.[0-9]+$') THEN 'double' ELSE 'string' END AS amountType
FROM sales base
ORDER BY base.data."_id"
FETCH FIRST 3 ROWS ONLY
```

---

### TYPE002: $toInt - convert string to integer

**Description:** Tests $toInt operator to convert values to integers  
**Collection:** `products`  
**Operator:** `$toInt`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "priceInt": {
        "$toInt": "$price"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, TRUNC(TO_NUMBER(base.data.price)) AS priceInt
FROM products base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### TYPE003: $toString - convert number to string

**Description:** Tests $toString operator to convert values to strings  
**Collection:** `employees`  
**Operator:** `$toString`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "salaryStr": {
        "$toString": "$salary"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, TO_CHAR(base.data.salary) AS salaryStr
FROM employees base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### TYPE004: $toDouble - convert to double

**Description:** Tests $toDouble operator to convert values to doubles  
**Collection:** `products`  
**Operator:** `$toDouble`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "priceDouble": {
        "$toDouble": "$price"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, TO_BINARY_DOUBLE(base.data.price) AS priceDouble
FROM products base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

### TYPE005: $toBool - convert to boolean

**Description:** Tests $toBool operator to convert values to booleans  
**Collection:** `employees`  
**Operator:** `$toBool`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "isActive": {
        "$toBool": "$active"
      }
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  },
  {
    "$limit": 5
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, CASE WHEN base.data.active IS NULL OR TO_CHAR(base.data.active) IN ('0', 'false') THEN 'false' ELSE 'true' END AS isActive
FROM employees base
ORDER BY base.data."_id"
FETCH FIRST 5 ROWS ONLY
```

---

## UnionWith Tests

### UNION001: Basic $unionWith - two collections

**Description:** Tests $unionWith to combine employees and customers (names from both)  
**Collection:** `employees`  
**Operator:** `$unionWith`  

**MongoDB Pipeline:**
```json
[
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "source": {
        "$literal": "employee"
      }
    }
  },
  {
    "$unionWith": {
      "coll": "customers",
      "pipeline": [
        {
          "$project": {
            "_id": 1,
            "name": 1,
            "source": {
              "$literal": "customer"
            }
          }
        }
      ]
    }
  },
  {
    "$sort": {
      "source": 1,
      "name": 1
    }
  },
  {
    "$limit": 10
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, :1 AS source
FROM employees base
UNION ALL SELECT base.data."_id" AS "_id", base.data.name AS name, :2 AS source
FROM customers base
ORDER BY source ASC, name ASC
FETCH FIRST 10 ROWS ONLY
```

---

### UNION002: $unionWith with match filter

**Description:** Tests $unionWith with $match in sub-pipeline  
**Collection:** `sales`  
**Operator:** `$unionWith`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$project": {
      "_id": 1,
      "amount": 1,
      "category": 1
    }
  },
  {
    "$unionWith": {
      "coll": "sales",
      "pipeline": [
        {
          "$match": {
            "status": "pending"
          }
        },
        {
          "$project": {
            "_id": 1,
            "amount": 1,
            "category": 1
          }
        }
      ]
    }
  },
  {
    "$sort": {
      "_id": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.amount AS amount, base.data.category AS category
FROM sales base
WHERE base.data.status = :1
UNION ALL SELECT base.data."_id" AS "_id", base.data.amount AS amount, base.data.category AS category
FROM sales base
WHERE base.data.status = :2
ORDER BY "_id" ASC
```

---

### UNION003: $unionWith with aggregation

**Description:** Tests $unionWith followed by group aggregation  
**Collection:** `products`  
**Operator:** `$unionWith`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "category": "electronics"
    }
  },
  {
    "$project": {
      "_id": 1,
      "price": 1
    }
  },
  {
    "$unionWith": {
      "coll": "products",
      "pipeline": [
        {
          "$match": {
            "category": "tools"
          }
        },
        {
          "$project": {
            "_id": 1,
            "price": 1
          }
        }
      ]
    }
  },
  {
    "$group": {
      "_id": null,
      "totalProducts": {
        "$sum": 1
      },
      "avgPrice": {
        "$avg": "$price"
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT COUNT(*) AS totalProducts, AVG(price) AS avgPrice FROM (
  SELECT base.data."_id" AS "_id", base.data.price AS price
FROM products base
WHERE base.data.category = :1
UNION ALL SELECT base.data."_id" AS "_id", base.data.price AS price
FROM products base
WHERE base.data.category = :2)
```

---

## Unwind Tests

### UNWIND001: Basic $unwind array

**Description:** Tests $unwind stage to flatten array  
**Collection:** `sales`  
**Operator:** `$unwind`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "orderId": 1001
    }
  },
  {
    "$unwind": "$items"
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "product": "$items.product",
      "qty": "$items.qty"
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, unwind_1.value.product AS product, unwind_1.value.qty AS qty
FROM sales base, JSON_TABLE(base.data, '$.items[*]' COLUMNS (value JSON PATH '$')) unwind_1
WHERE CAST(base.data.orderId AS NUMBER) = :1
```

---

### UNWIND002: $unwind with preserveNullAndEmptyArrays

**Description:** Tests $unwind with empty array preservation  
**Collection:** `sales`  
**Operator:** `$unwind`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "orderId": {
        "$in": [
          1001,
          1007
        ]
      }
    }
  },
  {
    "$unwind": {
      "path": "$tags",
      "preserveNullAndEmptyArrays": true
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "tag": "$tags"
    }
  },
  {
    "$sort": {
      "orderId": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, unwind_1.value AS tag
FROM sales base LEFT
OUTER JOIN JSON_TABLE(base.data, '$.tags[*]' COLUMNS (value JSON PATH '$')) unwind_1 ON 1=1
WHERE CAST(base.data.orderId AS NUMBER) IN (:1, :2)
ORDER BY base.data.orderId
```

---

## Window Tests

### WINDOW005: Window running total

**Description:** Tests running total with window function  
**Collection:** `sales`  
**Operator:** `$setWindowFields`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": "completed"
    }
  },
  {
    "$setWindowFields": {
      "sortBy": {
        "orderDate": 1
      },
      "output": {
        "runningTotal": {
          "$sum": "$amount",
          "window": {
            "documents": [
              "unbounded",
              "current"
            ]
          }
        }
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "orderId": 1,
      "amount": 1,
      "runningTotal": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.orderId AS orderId, base.data.amount AS amount, base.data.runningTotal AS runningTotal, SUM(JSON_VALUE(base.data, '$.amount' RETURNING NUMBER)) OVER (ORDER BY JSON_VALUE(base.data, '$.orderDate') ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS runningTotal
FROM sales base
WHERE base.data.status = :1
```

---

### WINDOW006: Window moving average

**Description:** Tests moving average with 3-row window  
**Collection:** `employees`  
**Operator:** `$setWindowFields`  

**MongoDB Pipeline:**
```json
[
  {
    "$setWindowFields": {
      "sortBy": {
        "salary": 1
      },
      "output": {
        "movingAvgSalary": {
          "$avg": "$salary",
          "window": {
            "documents": [
              -1,
              1
            ]
          }
        }
      }
    }
  },
  {
    "$project": {
      "_id": 1,
      "name": 1,
      "salary": 1,
      "movingAvgSalary": 1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT base.data."_id" AS "_id", base.data.name AS name, base.data.salary AS salary, base.data.movingAvgSalary AS movingAvgSalary, AVG(JSON_VALUE(base.data, '$.salary' RETURNING NUMBER)) OVER (ORDER BY JSON_VALUE(base.data, '$.salary') ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS movingAvgSalary
FROM employees base
```

---

## Large-Scale Complex Pipelines

These are complex aggregation pipelines designed for cross-database validation testing with deeply nested documents and large datasets (219K+ documents).

### PIPE001: E-commerce Order Revenue Analysis

**Description:** Order-level revenue analysis by status and time period (without array unwinding)  
**Collection:** `ecommerce_orders`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "status": {
        "$in": [
          "delivered",
          "shipped"
        ]
      }
    }
  },
  {
    "$group": {
      "_id": {
        "status": "$status",
        "month": {
          "$month": "$timestamps.createdAt"
        },
        "year": {
          "$year": "$timestamps.createdAt"
        }
      },
      "totalRevenue": {
        "$sum": "$pricing.subtotal"
      },
      "orderCount": {
        "$sum": 1
      },
      "avgShipping": {
        "$avg": "$pricing.shipping"
      },
      "avgTax": {
        "$avg": "$pricing.tax"
      }
    }
  },
  {
    "$addFields": {
      "avgRevenuePerOrder": {
        "$round": [
          {
            "$divide": [
              "$totalRevenue",
              "$orderCount"
            ]
          },
          2
        ]
      }
    }
  },
  {
    "$sort": {
      "totalRevenue": -1
    }
  },
  {
    "$limit": 50
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND((totalRevenue / orderCount), :1) AS avgRevenuePerOrder FROM (
  SELECT base.data.status AS status, EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.timestamps.createdAt'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS month, EXTRACT(YEAR FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.timestamps.createdAt'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS year, SUM(base.data.pricing.subtotal) AS totalRevenue, SUM(:2) AS orderCount, AVG(base.data.pricing.shipping) AS avgShipping, AVG(base.data.pricing.tax) AS avgTax
FROM ecommerce_orders base
WHERE base.data.status IN (:3, :4)
GROUP BY base.data.status, EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.timestamps.createdAt'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')), EXTRACT(YEAR FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.timestamps.createdAt'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
) inner_query
ORDER BY totalRevenue DESC
FETCH FIRST 50 ROWS ONLY
```

---

### PIPE002: Product Performance Analysis

**Description:** Analyze all products by category and brand with rating metrics (without array unwinding)  
**Collection:** `ecommerce_products`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": {
        "category": "$category.primary",
        "brand": "$brand.name"
      },
      "productCount": {
        "$sum": 1
      },
      "avgRating": {
        "$avg": "$ratings.average"
      },
      "totalReviews": {
        "$sum": "$ratings.count"
      },
      "avgPrice": {
        "$avg": "$pricing.basePrice"
      },
      "minPrice": {
        "$min": "$pricing.basePrice"
      },
      "maxPrice": {
        "$max": "$pricing.basePrice"
      }
    }
  },
  {
    "$addFields": {
      "priceRange": {
        "$round": [
          {
            "$subtract": [
              "$maxPrice",
              "$minPrice"
            ]
          },
          2
        ]
      }
    }
  },
  {
    "$sort": {
      "totalReviews": -1
    }
  },
  {
    "$limit": 30
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND((maxPrice - minPrice), :1) AS priceRange FROM (
  SELECT base.data.category.primary AS category, base.data.brand.name AS brand, SUM(:2) AS productCount, AVG(base.data.ratings.average) AS avgRating, SUM(base.data.ratings.count) AS totalReviews, AVG(base.data.pricing.basePrice) AS avgPrice, MIN(base.data.pricing.basePrice) AS minPrice, MAX(base.data.pricing.basePrice) AS maxPrice
FROM ecommerce_products base
GROUP BY base.data.category.primary, base.data.brand.name
) inner_query
ORDER BY totalReviews DESC
FETCH FIRST 30 ROWS ONLY
```

---

### PIPE003: Customer Lifetime Value Analysis

**Description:** Calculate customer value with loyalty tier analysis and purchase patterns  
**Collection:** `ecommerce_customers`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "metadata.status": "active",
      "orderHistory.totalOrders": {
        "$gte": 1
      }
    }
  },
  {
    "$group": {
      "_id": "$loyalty.tier",
      "customerCount": {
        "$sum": 1
      },
      "avgTotalSpent": {
        "$avg": "$orderHistory.totalSpent"
      },
      "avgTotalOrders": {
        "$avg": "$orderHistory.totalOrders"
      },
      "avgOrderValue": {
        "$avg": "$orderHistory.averageOrderValue"
      },
      "avgReturnRate": {
        "$avg": "$orderHistory.returnRate"
      }
    }
  },
  {
    "$addFields": {
      "avgReturnRatePercent": {
        "$round": [
          {
            "$multiply": [
              "$avgReturnRate",
              100
            ]
          },
          1
        ]
      }
    }
  },
  {
    "$sort": {
      "avgTotalSpent": -1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND((avgReturnRate * :1), :2) AS avgReturnRatePercent FROM (
  SELECT base.data.loyalty.tier AS "_id", SUM(:3) AS customerCount, AVG(base.data.orderHistory.totalSpent) AS avgTotalSpent, AVG(base.data.orderHistory.totalOrders) AS avgTotalOrders, AVG(base.data.orderHistory.averageOrderValue) AS avgOrderValue, AVG(base.data.orderHistory.returnRate) AS avgReturnRate
FROM ecommerce_customers base
WHERE (base.data.metadata.status = :4) AND (CAST(base.data.orderHistory.totalOrders AS NUMBER) >= :5)
GROUP BY base.data.loyalty.tier
) inner_query
ORDER BY avgTotalSpent DESC
```

---

### PIPE004: Review Sentiment and Quality Analysis

**Description:** Analyze review quality with aspect ratings and helpfulness metrics  
**Collection:** `ecommerce_reviews`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "metadata.status": "published",
      "verified": true
    }
  },
  {
    "$group": {
      "_id": {
        "sentiment": "$metadata.sentiment"
      },
      "reviewCount": {
        "$sum": 1
      },
      "avgOverallRating": {
        "$avg": "$rating.overall"
      },
      "avgQualityRating": {
        "$avg": "$rating.aspects.quality"
      },
      "avgValueRating": {
        "$avg": "$rating.aspects.value"
      },
      "avgShippingRating": {
        "$avg": "$rating.aspects.shipping"
      },
      "avgPackagingRating": {
        "$avg": "$rating.aspects.packaging"
      },
      "totalUpvotes": {
        "$sum": "$helpful.upvotes"
      },
      "totalDownvotes": {
        "$sum": "$helpful.downvotes"
      }
    }
  },
  {
    "$addFields": {
      "helpfulnessRatio": {
        "$round": [
          {
            "$cond": [
              {
                "$gt": [
                  {
                    "$add": [
                      "$totalUpvotes",
                      "$totalDownvotes"
                    ]
                  },
                  0
                ]
              },
              {
                "$divide": [
                  "$totalUpvotes",
                  {
                    "$add": [
                      "$totalUpvotes",
                      "$totalDownvotes"
                    ]
                  }
                ]
              },
              0
            ]
          },
          2
        ]
      }
    }
  },
  {
    "$sort": {
      "reviewCount": -1
    }
  },
  {
    "$limit": 40
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND(CASE WHEN (totalUpvotes + totalDownvotes) > :1 THEN (totalUpvotes / (totalUpvotes + totalDownvotes)) ELSE :2 END, :3) AS helpfulnessRatio FROM (
  SELECT base.data.metadata.sentiment AS sentiment, SUM(:4) AS reviewCount, AVG(base.data.rating.overall) AS avgOverallRating, AVG(base.data.rating.aspects.quality) AS avgQualityRating, AVG(base.data.rating.aspects.value) AS avgValueRating, AVG(base.data.rating.aspects.shipping) AS avgShippingRating, AVG(base.data.rating.aspects.packaging) AS avgPackagingRating, SUM(base.data.helpful.upvotes) AS totalUpvotes, SUM(base.data.helpful.downvotes) AS totalDownvotes
FROM ecommerce_reviews base
WHERE (base.data.metadata.status = :5) AND (base.data.verified = :6)
GROUP BY base.data.metadata.sentiment
) inner_query
ORDER BY reviewCount DESC
FETCH FIRST 40 ROWS ONLY
```

---

### PIPE005: Analytics Session Funnel Analysis

**Description:** Analyze conversion funnel with device and traffic source breakdown  
**Collection:** `analytics_sessions`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "duration": {
        "$gte": 10
      }
    }
  },
  {
    "$group": {
      "_id": {
        "deviceType": "$device.type",
        "trafficSource": "$traffic.source"
      },
      "sessionCount": {
        "$sum": 1
      },
      "avgPageViews": {
        "$avg": "$engagement.pageViews"
      },
      "avgUniquePages": {
        "$avg": "$engagement.uniquePages"
      },
      "avgEvents": {
        "$avg": "$engagement.events"
      },
      "avgScrollDepth": {
        "$avg": "$engagement.scrollDepth"
      },
      "avgDuration": {
        "$avg": "$duration"
      },
      "conversions": {
        "$sum": {
          "$cond": [
            "$conversion.converted",
            1,
            0
          ]
        }
      },
      "totalRevenue": {
        "$sum": "$conversion.revenue"
      },
      "totalTransactions": {
        "$sum": "$conversion.transactions"
      },
      "bounceCount": {
        "$sum": {
          "$cond": [
            "$bounced",
            1,
            0
          ]
        }
      },
      "newUserCount": {
        "$sum": {
          "$cond": [
            "$isNewUser",
            1,
            0
          ]
        }
      }
    }
  },
  {
    "$addFields": {
      "conversionRate": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$conversions",
                  "$sessionCount"
                ]
              },
              100
            ]
          },
          2
        ]
      },
      "bounceRate": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$bounceCount",
                  "$sessionCount"
                ]
              },
              100
            ]
          },
          2
        ]
      },
      "newUserRate": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$newUserCount",
                  "$sessionCount"
                ]
              },
              100
            ]
          },
          2
        ]
      },
      "avgRevenuePerSession": {
        "$round": [
          {
            "$divide": [
              "$totalRevenue",
              "$sessionCount"
            ]
          },
          2
        ]
      }
    }
  },
  {
    "$sort": {
      "conversionRate": -1,
      "sessionCount": -1
    }
  },
  {
    "$limit": 30
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND(((conversions / sessionCount) * :1), :2) AS conversionRate, ROUND(((bounceCount / sessionCount) * :3), :4) AS bounceRate, ROUND(((newUserCount / sessionCount) * :5), :6) AS newUserRate, ROUND((totalRevenue / sessionCount), :7) AS avgRevenuePerSession FROM (
  SELECT base.data.device.type AS deviceType, base.data.traffic.source AS trafficSource, SUM(:8) AS sessionCount, AVG(base.data.engagement.pageViews) AS avgPageViews, AVG(base.data.engagement.uniquePages) AS avgUniquePages, AVG(base.data.engagement.events) AS avgEvents, AVG(base.data.engagement.scrollDepth) AS avgScrollDepth, AVG(base.data.duration) AS avgDuration, SUM(CASE WHEN base.data.conversion.converted THEN :9 ELSE :10 END) AS conversions, SUM(base.data.conversion.revenue) AS totalRevenue, SUM(base.data.conversion.transactions) AS totalTransactions, SUM(CASE WHEN base.data.bounced THEN :11 ELSE :12 END) AS bounceCount, SUM(CASE WHEN base.data.isNewUser THEN :13 ELSE :14 END) AS newUserCount
FROM analytics_sessions base
WHERE CAST(base.data.duration AS NUMBER) >= :15
GROUP BY base.data.device.type, base.data.traffic.source
) inner_query
ORDER BY conversionRate DESC, sessionCount DESC
FETCH FIRST 30 ROWS ONLY
```

---

### PIPE006: Social Post Engagement Analysis

**Description:** Analyze post engagement including reaction and comment metrics  
**Collection:** `social_posts`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "metadata.status": "published",
      "visibility": {
        "$in": [
          "public",
          "followers"
        ]
      }
    }
  },
  {
    "$group": {
      "_id": {
        "postType": "$type"
      },
      "postCount": {
        "$sum": 1
      },
      "avgLikes": {
        "$avg": "$reactions.like"
      },
      "avgLoves": {
        "$avg": "$reactions.love"
      },
      "avgLaughs": {
        "$avg": "$reactions.laugh"
      },
      "avgWows": {
        "$avg": "$reactions.wow"
      },
      "avgViews": {
        "$avg": "$engagement.views"
      },
      "avgReach": {
        "$avg": "$engagement.reach"
      },
      "avgImpressions": {
        "$avg": "$engagement.impressions"
      },
      "avgSaves": {
        "$avg": "$engagement.saves"
      },
      "totalShares": {
        "$sum": "$shares.count"
      },
      "sponsoredCount": {
        "$sum": {
          "$cond": [
            "$metadata.sponsored",
            1,
            0
          ]
        }
      }
    }
  },
  {
    "$addFields": {
      "avgTotalReactions": {
        "$round": [
          {
            "$add": [
              {
                "$ifNull": [
                  "$avgLikes",
                  0
                ]
              },
              {
                "$ifNull": [
                  "$avgLoves",
                  0
                ]
              },
              {
                "$ifNull": [
                  "$avgLaughs",
                  0
                ]
              },
              {
                "$ifNull": [
                  "$avgWows",
                  0
                ]
              }
            ]
          },
          0
        ]
      },
      "sponsoredPercentage": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$sponsoredCount",
                  "$postCount"
                ]
              },
              100
            ]
          },
          1
        ]
      }
    }
  },
  {
    "$sort": {
      "avgViews": -1
    }
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND((NVL(avgLikes, :1) + NVL(avgLoves, :2) + NVL(avgLaughs, :3) + NVL(avgWows, :4)), :5) AS avgTotalReactions, ROUND(((sponsoredCount / postCount) * :6), :7) AS sponsoredPercentage FROM (
  SELECT base.data.type AS postType, SUM(:8) AS postCount, AVG(base.data.reactions.like) AS avgLikes, AVG(base.data.reactions.love) AS avgLoves, AVG(base.data.reactions.laugh) AS avgLaughs, AVG(base.data.reactions.wow) AS avgWows, AVG(base.data.engagement.views) AS avgViews, AVG(base.data.engagement.reach) AS avgReach, AVG(base.data.engagement.impressions) AS avgImpressions, AVG(base.data.engagement.saves) AS avgSaves, SUM(base.data.shares.count) AS totalShares, SUM(CASE WHEN base.data.metadata.sponsored THEN :9 ELSE :10 END) AS sponsoredCount
FROM social_posts base
WHERE (base.data.metadata.status = :11) AND (base.data.visibility IN (:12, :13))
GROUP BY base.data.type
) inner_query
ORDER BY avgViews DESC
```

---

### PIPE007: IoT Device Health Analysis by Building

**Description:** Analyze device health status by building (without array unwinding)  
**Collection:** `iot_devices`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": {
        "building": "$location.building",
        "floor": "$location.floor"
      },
      "deviceCount": {
        "$sum": 1
      },
      "onlineCount": {
        "$sum": {
          "$cond": [
            "$status.online",
            1,
            0
          ]
        }
      },
      "healthyCount": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$status.health",
                "healthy"
              ]
            },
            1,
            0
          ]
        }
      },
      "avgBattery": {
        "$avg": "$status.battery"
      },
      "avgSignalStrength": {
        "$avg": "$status.signalStrength"
      },
      "avgSamplingRate": {
        "$avg": "$configuration.samplingRate"
      }
    }
  },
  {
    "$addFields": {
      "onlinePercentage": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$onlineCount",
                  "$deviceCount"
                ]
              },
              100
            ]
          },
          1
        ]
      },
      "healthyPercentage": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$healthyCount",
                  "$deviceCount"
                ]
              },
              100
            ]
          },
          1
        ]
      }
    }
  },
  {
    "$sort": {
      "healthyPercentage": 1,
      "deviceCount": -1
    }
  },
  {
    "$limit": 40
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND(((onlineCount / deviceCount) * :1), :2) AS onlinePercentage, ROUND(((healthyCount / deviceCount) * :3), :4) AS healthyPercentage FROM (
  SELECT base.data.location.building AS building, base.data.location.floor AS floor, SUM(:5) AS deviceCount, SUM(CASE WHEN base.data.status.online THEN :6 ELSE :7 END) AS onlineCount, SUM(CASE WHEN base.data.status.health = :8 THEN :9 ELSE :10 END) AS healthyCount, AVG(base.data.status.battery) AS avgBattery, AVG(base.data.status.signalStrength) AS avgSignalStrength, AVG(base.data.configuration.samplingRate) AS avgSamplingRate
FROM iot_devices base
GROUP BY base.data.location.building, base.data.location.floor
) inner_query
ORDER BY healthyPercentage, deviceCount DESC
FETCH FIRST 40 ROWS ONLY
```

---

### PIPE008: IoT Time-Series Aggregation

**Description:** Aggregate sensor readings by hour (single group stage)  
**Collection:** `iot_readings`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": {
        "hourOfDay": {
          "$hour": "$timestamp"
        }
      },
      "readingCount": {
        "$sum": 1
      },
      "avgTemperature": {
        "$avg": "$readings.temperature.value"
      },
      "minTemperature": {
        "$min": "$readings.temperature.value"
      },
      "maxTemperature": {
        "$max": "$readings.temperature.value"
      },
      "avgHumidity": {
        "$avg": "$readings.humidity.value"
      },
      "avgPressure": {
        "$avg": "$readings.pressure.value"
      },
      "goodTempReadings": {
        "$sum": {
          "$cond": [
            {
              "$eq": [
                "$readings.temperature.quality",
                "good"
              ]
            },
            1,
            0
          ]
        }
      },
      "avgTransmissionDelay": {
        "$avg": "$metadata.transmissionDelay"
      }
    }
  },
  {
    "$addFields": {
      "temperatureRange": {
        "$subtract": [
          "$maxTemperature",
          "$minTemperature"
        ]
      },
      "dataQualityRate": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$goodTempReadings",
                  "$readingCount"
                ]
              },
              100
            ]
          },
          1
        ]
      }
    }
  },
  {
    "$sort": {
      "readingCount": -1
    }
  },
  {
    "$limit": 24
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, (maxTemperature - minTemperature) AS temperatureRange, ROUND(((goodTempReadings / readingCount) * :1), :2) AS dataQualityRate FROM (
  SELECT EXTRACT(HOUR FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.timestamp'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS hourOfDay, SUM(:3) AS readingCount, AVG(base.data.readings.temperature.value) AS avgTemperature, MIN(base.data.readings.temperature.value) AS minTemperature, MAX(base.data.readings.temperature.value) AS maxTemperature, AVG(base.data.readings.humidity.value) AS avgHumidity, AVG(base.data.readings.pressure.value) AS avgPressure, SUM(CASE WHEN base.data.readings.temperature.quality = :4 THEN :5 ELSE :6 END) AS goodTempReadings, AVG(base.data.metadata.transmissionDelay) AS avgTransmissionDelay
FROM iot_readings base
GROUP BY EXTRACT(HOUR FROM TO_TIMESTAMP(JSON_VALUE(base.data, '$.timestamp'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
) inner_query
ORDER BY readingCount DESC
FETCH FIRST 24 ROWS ONLY
```

---

### PIPE009: User Follower Network Analysis

**Description:** Analyze social user networks with engagement and activity patterns using bucket aggregation  
**Collection:** `social_users`  

**MongoDB Pipeline:**
```json
[
  {
    "$match": {
      "metadata.status": "active"
    }
  },
  {
    "$bucket": {
      "groupBy": "$stats.followers",
      "boundaries": [
        0,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000
      ],
      "default": 10000001,
      "output": {
        "userCount": {
          "$sum": 1
        },
        "verifiedCount": {
          "$sum": {
            "$cond": [
              "$profile.verified",
              1,
              0
            ]
          }
        },
        "privateCount": {
          "$sum": {
            "$cond": [
              "$profile.private",
              1,
              0
            ]
          }
        },
        "avgFollowing": {
          "$avg": "$stats.following"
        },
        "avgPosts": {
          "$avg": "$stats.posts"
        },
        "avgLikes": {
          "$avg": "$stats.likes"
        },
        "avgComments": {
          "$avg": "$stats.comments"
        },
        "avgShares": {
          "$avg": "$stats.shares"
        },
        "avgLoginStreak": {
          "$avg": "$activity.loginStreak"
        },
        "avgTimeSpent": {
          "$avg": "$activity.totalTimeSpent"
        }
      }
    }
  },
  {
    "$addFields": {
      "verifiedPercentage": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$verifiedCount",
                  "$userCount"
                ]
              },
              100
            ]
          },
          1
        ]
      },
      "privatePercentage": {
        "$round": [
          {
            "$multiply": [
              {
                "$divide": [
                  "$privateCount",
                  "$userCount"
                ]
              },
              100
            ]
          },
          1
        ]
      },
      "avgEngagement": {
        "$round": [
          {
            "$add": [
              "$avgLikes",
              "$avgComments",
              "$avgShares"
            ]
          },
          0
        ]
      }
    }
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND(((verifiedCount / userCount) * :1), :2) AS verifiedPercentage, ROUND(((privateCount / userCount) * :3), :4) AS privatePercentage, ROUND((avgLikes + avgComments + avgShares), :5) AS avgEngagement FROM (
  SELECT CASE WHEN base.data.stats.followers >= 0 AND base.data.stats.followers < 100 THEN 0 WHEN base.data.stats.followers >= 100 AND base.data.stats.followers < 1000 THEN 100 WHEN base.data.stats.followers >= 1000 AND base.data.stats.followers < 10000 THEN 1000 WHEN base.data.stats.followers >= 10000 AND base.data.stats.followers < 100000 THEN 10000 WHEN base.data.stats.followers >= 100000 AND base.data.stats.followers < 1000000 THEN 100000 WHEN base.data.stats.followers >= 1000000 AND base.data.stats.followers < 10000000 THEN 1000000 ELSE 10000001 END AS "_id", SUM(:6) AS userCount, SUM(CASE WHEN base.data.profile.verified THEN :7 ELSE :8 END) AS verifiedCount, SUM(CASE WHEN base.data.profile.private THEN :9 ELSE :10 END) AS privateCount, AVG(base.data.stats.following) AS avgFollowing, AVG(base.data.stats.posts) AS avgPosts, AVG(base.data.stats.likes) AS avgLikes, AVG(base.data.stats.comments) AS avgComments, AVG(base.data.stats.shares) AS avgShares, AVG(base.data.activity.loginStreak) AS avgLoginStreak, AVG(base.data.activity.totalTimeSpent) AS avgTimeSpent
FROM social_users base
WHERE base.data.metadata.status = :11
GROUP BY CASE WHEN base.data.stats.followers >= 0 AND base.data.stats.followers < 100 THEN 0 WHEN base.data.stats.followers >= 100 AND base.data.stats.followers < 1000 THEN 100 WHEN base.data.stats.followers >= 1000 AND base.data.stats.followers < 10000 THEN 1000 WHEN base.data.stats.followers >= 10000 AND base.data.stats.followers < 100000 THEN 10000 WHEN base.data.stats.followers >= 100000 AND base.data.stats.followers < 1000000 THEN 100000 WHEN base.data.stats.followers >= 1000000 AND base.data.stats.followers < 10000000 THEN 1000000 ELSE 10000001 END
) inner_query
```

---

### PIPE010: Order Analysis by Payment and Shipping Method

**Description:** Analyze all orders by payment method with totals metrics (without array unwinding)  
**Collection:** `ecommerce_orders`  

**MongoDB Pipeline:**
```json
[
  {
    "$group": {
      "_id": {
        "paymentMethod": "$payment.method",
        "shippingMethod": "$shipping.method"
      },
      "orderCount": {
        "$sum": 1
      },
      "totalRevenue": {
        "$sum": "$pricing.subtotal"
      },
      "totalShipping": {
        "$sum": "$pricing.shipping"
      },
      "totalTax": {
        "$sum": "$pricing.tax"
      },
      "avgOrderValue": {
        "$avg": "$pricing.subtotal"
      },
      "maxOrderValue": {
        "$max": "$pricing.subtotal"
      },
      "minOrderValue": {
        "$min": "$pricing.subtotal"
      }
    }
  },
  {
    "$addFields": {
      "avgRevenuePerOrder": {
        "$round": [
          {
            "$divide": [
              "$totalRevenue",
              "$orderCount"
            ]
          },
          2
        ]
      },
      "shippingPercent": {
        "$round": [
          {
            "$cond": [
              {
                "$gt": [
                  "$totalRevenue",
                  0
                ]
              },
              {
                "$multiply": [
                  {
                    "$divide": [
                      "$totalShipping",
                      "$totalRevenue"
                    ]
                  },
                  100
                ]
              },
              0
            ]
          },
          1
        ]
      }
    }
  },
  {
    "$sort": {
      "totalRevenue": -1
    }
  },
  {
    "$limit": 50
  }
]
```

**Generated SQL:**
```sql
SELECT inner_query.*, ROUND((totalRevenue / orderCount), :1) AS avgRevenuePerOrder, ROUND(CASE WHEN totalRevenue > :2 THEN ((totalShipping / totalRevenue) * :3) ELSE :4 END, :5) AS shippingPercent FROM (
  SELECT base.data.payment.method AS paymentMethod, base.data.shipping.method AS shippingMethod, SUM(:6) AS orderCount, SUM(base.data.pricing.subtotal) AS totalRevenue, SUM(base.data.pricing.shipping) AS totalShipping, SUM(base.data.pricing.tax) AS totalTax, AVG(base.data.pricing.subtotal) AS avgOrderValue, MAX(base.data.pricing.subtotal) AS maxOrderValue, MIN(base.data.pricing.subtotal) AS minOrderValue
FROM ecommerce_orders base
GROUP BY base.data.payment.method, base.data.shipping.method
) inner_query
ORDER BY totalRevenue DESC
FETCH FIRST 50 ROWS ONLY
```

---

