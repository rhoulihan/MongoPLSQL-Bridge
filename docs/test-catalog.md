# MongoPLSQL-Bridge Test Catalog

This document contains a comprehensive catalog of all integration tests, including the MongoDB pipeline used and the Oracle SQL generated.

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
SELECT JSON_VALUE(data, '$.status') AS grp_id, COUNT(*) AS cnt FROM sales GROUP BY JSON_VALUE(data, '$.status') ORDER BY JSON_VALUE(data, '$.status')
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
SELECT JSON_VALUE(data, '$.category') AS grp_id, SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS totalAmount FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed' GROUP BY JSON_VALUE(data, '$.category') ORDER BY JSON_VALUE(data, '$.category')
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
SELECT JSON_VALUE(data, '$.department') AS grp_id, AVG(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS avgSalary FROM employees GROUP BY JSON_VALUE(data, '$.department') ORDER BY JSON_VALUE(data, '$.department')
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
SELECT JSON_VALUE(data, '$.category') AS grp_id, MIN(JSON_VALUE(data, '$.price' RETURNING NUMBER)) AS minPrice FROM products WHERE JSON_VALUE(data, '$.active') = 'true' GROUP BY JSON_VALUE(data, '$.category') ORDER BY JSON_VALUE(data, '$.category')
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
SELECT JSON_VALUE(data, '$.department') AS grp_id, MAX(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS maxSalary FROM employees GROUP BY JSON_VALUE(data, '$.department') ORDER BY JSON_VALUE(data, '$.department')
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
SELECT JSON_VALUE(data, '$.department') AS grp_id, COUNT(*) AS cnt, SUM(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS totalSalary, AVG(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS avgSalary, MIN(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS minSalary, MAX(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS maxSalary FROM employees GROUP BY JSON_VALUE(data, '$.department') ORDER BY JSON_VALUE(data, '$.department')
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
SELECT JSON_VALUE(data, '$.department') AS grp_id, JSON_ARRAYAGG(JSON_VALUE(data, '$.name')) AS employees FROM employees GROUP BY JSON_VALUE(data, '$.department') ORDER BY JSON_VALUE(data, '$.department')
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
SELECT region AS grp_id, JSON_ARRAYAGG(status ORDER BY status) AS statuses FROM (SELECT DISTINCT JSON_VALUE(data, '$.region') AS region, JSON_VALUE(data, '$.status') AS status FROM sales) GROUP BY region ORDER BY region
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
SELECT department AS "_id", MAX(name) KEEP (DENSE_RANK FIRST ORDER BY salary DESC) AS highestPaidEmployee FROM (SELECT JSON_VALUE(data, '$.department') AS department, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary FROM employees) GROUP BY department ORDER BY department
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
SELECT department AS "_id", MAX(name) KEEP (DENSE_RANK LAST ORDER BY salary DESC) AS lowestPaidEmployee FROM (SELECT JSON_VALUE(data, '$.department') AS department, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary FROM employees) GROUP BY department ORDER BY department
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
SELECT id, JSON_VALUE(data, '$.name') AS name, (JSON_VALUE(data, '$.salary' RETURNING NUMBER) + JSON_VALUE(data, '$.bonus' RETURNING NUMBER)) AS totalCompensation FROM employees ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, (JSON_VALUE(data, '$.price' RETURNING NUMBER) - JSON_VALUE(data, '$.cost' RETURNING NUMBER)) AS profitMargin FROM products WHERE JSON_VALUE(data, '$.active') = 'true' ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, (JSON_VALUE(data, '$.amount' RETURNING NUMBER) + JSON_VALUE(data, '$.tax' RETURNING NUMBER)) AS total FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed' ORDER BY JSON_VALUE(data, '$.orderId' RETURNING NUMBER)
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
SELECT id, JSON_VALUE(data, '$.name') AS name, (JSON_VALUE(data, '$.price' RETURNING NUMBER) - JSON_VALUE(data, '$.cost' RETURNING NUMBER)) AS profit FROM products WHERE JSON_VALUE(data, '$.active') = 'true' ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, (JSON_VALUE(data, '$.salary' RETURNING NUMBER) + (JSON_VALUE(data, '$.bonus' RETURNING NUMBER) * 1)) AS totalComp FROM employees ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, ((JSON_VALUE(data, '$.price' RETURNING NUMBER) - JSON_VALUE(data, '$.cost' RETURNING NUMBER)) / JSON_VALUE(data, '$.cost' RETURNING NUMBER)) AS margin FROM products WHERE JSON_VALUE(data, '$.cost' RETURNING NUMBER) > 0 ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, MOD(JSON_VALUE(data, '$.orderId' RETURNING NUMBER), 3) AS orderIdMod3 FROM sales ORDER BY JSON_VALUE(data, '$.orderId' RETURNING NUMBER)
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, ABS(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS absoluteAmount FROM sales ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.tax' RETURNING NUMBER) AS tax, CEIL(JSON_VALUE(data, '$.tax' RETURNING NUMBER)) AS taxCeiled FROM sales ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, FLOOR(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS amountFloored FROM sales ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, ROUND(JSON_VALUE(data, '$.amount' RETURNING NUMBER), 0) AS amountRounded FROM sales ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, TRUNC(JSON_VALUE(data, '$.amount' RETURNING NUMBER), 1) AS amountTruncated FROM sales ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.tags[0]') AS firstTag FROM products WHERE JSON_EXISTS(data, '$.tags[0]') ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.items.size()' RETURNING NUMBER) AS itemCount FROM sales ORDER BY JSON_VALUE(data, '$.orderId' RETURNING NUMBER)
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.tags[0]') AS firstTag FROM products WHERE JSON_EXISTS(data, '$.tags[0]') ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.tags[last]') AS lastTag FROM sales WHERE JSON_EXISTS(data, '$.tags[0]') ORDER BY JSON_VALUE(data, '$.orderId' RETURNING NUMBER)
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.orderId') AS orderId, (SELECT JSON_ARRAYAGG(val) FROM JSON_TABLE(data, '$.items[*]' COLUMNS (val VARCHAR2(4000) PATH '$')) WHERE JSON_VALUE(base.data, '$.item.qty') > :1) AS highValueItems FROM sales base ORDER BY JSON_VALUE(base.data, '$.orderId') FETCH FIRST 5 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.orderId') AS orderId, (SELECT JSON_ARRAYAGG(JSON_VALUE(base.data, '$.item.product')) FROM JSON_TABLE(data, '$.items[*]' COLUMNS (val VARCHAR2(4000) PATH '$'))) AS itemProducts FROM sales base ORDER BY JSON_VALUE(base.data, '$.orderId') FETCH FIRST 5 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.orderId') AS orderId, /* $reduce not fully supported */ NULL AS totalQty FROM sales base ORDER BY JSON_VALUE(base.data, '$.orderId') FETCH FIRST 5 ROWS ONLY
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
null
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
null
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
null
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
SELECT id, JSON_QUERY(data, '$.tags') AS tags, JSON_QUERY(data, '$.tags') AS reversedTags FROM sales WHERE JSON_VALUE(data, '$._id') = 'S001' ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, CASE WHEN JSON_QUERY(data, '$.tags') IS NOT NULL AND JSON_VALUE(data, '$.tags[0]' ERROR ON ERROR) IS NOT NULL THEN 1 ELSE 0 END AS isTagsArray, 0 AS isAmountArray FROM sales ORDER BY id
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
SELECT id, JSON_QUERY(data, '$.tags') AS tags, JSON_QUERY(data, '$.tags') AS unionResult FROM sales WHERE JSON_VALUE(data, '$._id') = 'S001' ORDER BY id
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
SELECT id, JSON_QUERY(data, '$.tags') AS tags, JSON_QUERY(data, '$.tags') AS intersectResult FROM sales WHERE JSON_VALUE(data, '$._id') = 'S001' ORDER BY id
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
SELECT CASE WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 0 AND JSON_VALUE(data, '$.price' RETURNING NUMBER) < 25 THEN 0 WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 25 AND JSON_VALUE(data, '$.price' RETURNING NUMBER) < 100 THEN 25 WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 100 AND JSON_VALUE(data, '$.price' RETURNING NUMBER) < 1000 THEN 100 END AS bucket_id, COUNT(*) AS cnt, JSON_ARRAYAGG(JSON_VALUE(data, '$.name')) AS products FROM products WHERE JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 0 AND JSON_VALUE(data, '$.price' RETURNING NUMBER) < 1000 GROUP BY CASE WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 0 AND JSON_VALUE(data, '$.price' RETURNING NUMBER) < 25 THEN 0 WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 25 AND JSON_VALUE(data, '$.price' RETURNING NUMBER) < 100 THEN 25 WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 100 AND JSON_VALUE(data, '$.price' RETURNING NUMBER) < 1000 THEN 100 END ORDER BY bucket_id
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
SELECT CASE WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 60000 AND JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 75000 THEN 60000 WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 75000 AND JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 90000 THEN 75000 WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 90000 AND JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 110000 THEN 90000 END AS bucket_id, COUNT(*) AS cnt, AVG(JSON_VALUE(data, '$.bonus' RETURNING NUMBER)) AS avgBonus FROM employees WHERE JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 60000 AND JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 110000 GROUP BY CASE WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 60000 AND JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 75000 THEN 60000 WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 75000 AND JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 90000 THEN 75000 WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 90000 AND JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 110000 THEN 90000 END ORDER BY bucket_id
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
SELECT bucket_id, COUNT(*) AS cnt, AVG(price) AS avgPrice FROM (SELECT JSON_VALUE(data, '$.price' RETURNING NUMBER) AS price, NTILE(3) OVER (ORDER BY JSON_VALUE(data, '$.price' RETURNING NUMBER)) AS bucket_id FROM products WHERE JSON_VALUE(data, '$.active') = 'true') GROUP BY bucket_id ORDER BY bucket_id
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
SELECT bucket_id, COUNT(*) AS cnt, MIN(salary) AS minSalary, MAX(salary) AS maxSalary FROM (SELECT JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary, NTILE(4) OVER (ORDER BY JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS bucket_id FROM employees) GROUP BY bucket_id ORDER BY bucket_id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.status') AS status FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed' ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount FROM sales WHERE JSON_VALUE(data, '$.amount' RETURNING NUMBER) > 200 ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary FROM employees WHERE JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 90000 ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.price' RETURNING NUMBER) AS price FROM products WHERE JSON_VALUE(data, '$.price' RETURNING NUMBER) < 50 ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.yearsOfService' RETURNING NUMBER) AS yearsOfService FROM employees WHERE JSON_VALUE(data, '$.yearsOfService' RETURNING NUMBER) <= 2 ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.status') AS status FROM sales WHERE JSON_VALUE(data, '$.status') <> 'completed' ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.status') AS status FROM sales WHERE JSON_VALUE(data, '$.status') IN ('completed', 'pending') ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.region') AS region FROM sales WHERE JSON_VALUE(data, '$.region') NOT IN ('north', 'south') ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId FROM sales WHERE JSON_QUERY(data, '$.metadata') IS NOT NULL ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId FROM sales WHERE JSON_QUERY(data, '$.metadata') IS NULL ORDER BY id
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
SELECT JSON_VALUE(data, '$.region') AS grp_id, SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS totalSales, COUNT(*) AS orderCount FROM sales WHERE JSON_VALUE(data, '$.status') IN ('completed', 'processing') GROUP BY JSON_VALUE(data, '$.region') ORDER BY SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) DESC
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.department') AS department, (JSON_VALUE(data, '$.salary' RETURNING NUMBER) + JSON_VALUE(data, '$.bonus' RETURNING NUMBER)) AS totalComp FROM employees WHERE JSON_VALUE(data, '$.active') = 'true' ORDER BY (JSON_VALUE(data, '$.salary' RETURNING NUMBER) + JSON_VALUE(data, '$.bonus' RETURNING NUMBER)) DESC FETCH FIRST 5 ROWS ONLY
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.category') AS category, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount FROM sales WHERE (JSON_VALUE(data, '$.category') = 'electronics' OR JSON_VALUE(data, '$.category') = 'jewelry') AND JSON_VALUE(data, '$.amount' RETURNING NUMBER) >= 100 ORDER BY JSON_VALUE(data, '$.amount' RETURNING NUMBER) DESC
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
SELECT JSON_VALUE(c.data, '$.tier') AS grp_id, SUM(JSON_VALUE(s.data, '$.amount' RETURNING NUMBER)) AS totalAmount, COUNT(*) AS orderCount FROM sales s INNER JOIN customers c ON JSON_VALUE(s.data, '$.customerId') = JSON_VALUE(c.data, '$._id') WHERE JSON_VALUE(s.data, '$.status') = 'completed' GROUP BY JSON_VALUE(c.data, '$.tier') ORDER BY JSON_VALUE(c.data, '$.tier')
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
SELECT UPPER(JSON_VALUE(data, '$.department')) AS grp_id, AVG(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS avgSalary, COUNT(*) AS headcount FROM employees GROUP BY UPPER(JSON_VALUE(data, '$.department')) ORDER BY UPPER(JSON_VALUE(data, '$.department'))
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
SELECT item.product AS "_id", SUM(item.qty) AS totalQuantity, SUM(item.qty * item.price) AS totalRevenue FROM sales base, JSON_TABLE(base.data, '$.items[*]' COLUMNS (product VARCHAR2(100) PATH '$.product', qty NUMBER PATH '$.qty', price NUMBER PATH '$.price')) item GROUP BY item.product ORDER BY item.product
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
SELECT JSON_VALUE(data, '$.category') AS "_id", SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS totalAmount, COUNT(*) AS orderCount FROM sales GROUP BY JSON_VALUE(data, '$.category') ORDER BY SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) DESC FETCH FIRST 3 ROWS ONLY
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
SELECT source AS "_id", SUM(amount) AS totalAmount FROM (SELECT 'sales' AS source, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount FROM sales UNION ALL SELECT 'products' AS source, JSON_VALUE(data, '$.price' RETURNING NUMBER) AS amount FROM products) GROUP BY source ORDER BY source
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
SELECT id, JSON_VALUE(data, '$.name') AS name, CASE WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 100 THEN 'expensive' ELSE 'affordable' END AS priceCategory FROM products ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, NVL(JSON_VALUE(data, '$.discount' RETURNING NUMBER), 0) AS discountApplied FROM sales ORDER BY JSON_VALUE(data, '$.orderId' RETURNING NUMBER)
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
SELECT id, JSON_VALUE(data, '$.name') AS name, CASE WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 200 THEN 'high' WHEN JSON_VALUE(data, '$.price' RETURNING NUMBER) >= 50 THEN 'medium' ELSE 'low' END AS priceRange FROM products ORDER BY id
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
SELECT JSON_OBJECT('totalEmployees' VALUE COUNT(*)) AS data FROM employees base
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
SELECT JSON_OBJECT('completedOrders' VALUE COUNT(*)) AS data FROM sales base WHERE JSON_VALUE(base.data, '$.status') = :1
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
SELECT JSON_OBJECT('highEarningActiveEmployees' VALUE COUNT(*)) AS data FROM employees base WHERE (JSON_VALUE(base.data, '$.active') = :1) AND (JSON_VALUE(base.data, '$.salary' RETURNING NUMBER) >= :2)
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
SELECT id, JSON_VALUE(data, '$.title') AS title, EXTRACT(YEAR FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS eventYear FROM events ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.title') AS title, EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS eventMonth FROM events ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.title') AS title, EXTRACT(DAY FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS eventDay FROM events ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.title') AS title, EXTRACT(HOUR FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS eventHour FROM events ORDER BY id
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
SELECT EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) AS grp_id, COUNT(*) AS eventCount FROM events GROUP BY EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"')) ORDER BY EXTRACT(MONTH FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'))
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
SELECT id, JSON_VALUE(data, '$.eventName') AS eventName, EXTRACT(MINUTE FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS')) AS minute FROM events ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.eventName') AS eventName, EXTRACT(SECOND FROM TO_TIMESTAMP(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD"T"HH24:MI:SS')) AS second FROM events ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.eventName') AS eventName, TO_NUMBER(TO_CHAR(TO_DATE(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD'), 'D')) AS dayOfWeek FROM events ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.eventName') AS eventName, TO_NUMBER(TO_CHAR(TO_DATE(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD'), 'DDD')) AS dayOfYear FROM events ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.eventName') AS eventName, TO_NUMBER(TO_CHAR(TO_DATE(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD'), 'IW')) AS week FROM events ORDER BY id
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
SELECT EXTRACT(MONTH FROM TO_DATE(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD')) AS "_id", COUNT(*) AS eventCount FROM events GROUP BY EXTRACT(MONTH FROM TO_DATE(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD')) ORDER BY EXTRACT(MONTH FROM TO_DATE(JSON_VALUE(data, '$.eventDate'), 'YYYY-MM-DD'))
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
SELECT SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS totalAmount FROM sales WHERE JSON_VALUE(data, '$.quantity' RETURNING NUMBER) = 0
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
SELECT SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS totalRefunds FROM sales WHERE JSON_VALUE(data, '$.amount' RETURNING NUMBER) < 0
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
SELECT id FROM sales WHERE JSON_VALUE(data, '$.status') = 'nonexistent_status'
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary, CASE WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 60000 THEN 'Junior' WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) < 90000 THEN 'Mid' WHEN JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 90000 THEN 'Senior' ELSE 'Unknown' END AS salaryBand FROM employees ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.status') AS status, CASE WHEN JSON_VALUE(data, '$.status') = 'completed' THEN 'low' WHEN JSON_VALUE(data, '$.status') = 'pending' THEN 'high' ELSE 'medium' END AS priority FROM sales ORDER BY id
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
SELECT JSON_OBJECT('byStatus' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE status, 'count' VALUE cnt) ORDER BY status) FROM (SELECT JSON_VALUE(data, '$.status') AS status, COUNT(*) AS cnt FROM sales GROUP BY JSON_VALUE(data, '$.status'))), 'byRegion' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE region, 'totalAmount' VALUE totalAmount) ORDER BY region) FROM (SELECT JSON_VALUE(data, '$.region') AS region, SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) AS totalAmount FROM sales GROUP BY JSON_VALUE(data, '$.region')))) AS result FROM DUAL
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
SELECT JSON_OBJECT('categorySummary' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE category, 'count' VALUE cnt, 'avgPrice' VALUE avgPrice) ORDER BY category) FROM (SELECT JSON_VALUE(data, '$.category') AS category, COUNT(*) AS cnt, AVG(JSON_VALUE(data, '$.price' RETURNING NUMBER)) AS avgPrice FROM products WHERE JSON_VALUE(data, '$.active') = 'true' GROUP BY JSON_VALUE(data, '$.category'))), 'priceStats' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE NULL, 'minPrice' VALUE minPrice, 'maxPrice' VALUE maxPrice, 'avgPrice' VALUE avgPrice)) FROM (SELECT MIN(JSON_VALUE(data, '$.price' RETURNING NUMBER)) AS minPrice, MAX(JSON_VALUE(data, '$.price' RETURNING NUMBER)) AS maxPrice, AVG(JSON_VALUE(data, '$.price' RETURNING NUMBER)) AS avgPrice FROM products WHERE JSON_VALUE(data, '$.active') = 'true'))) AS result FROM DUAL
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
SELECT JSON_OBJECT('departmentCounts' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE dept, 'count' VALUE cnt) ORDER BY cnt DESC) FROM (SELECT JSON_VALUE(data, '$.department') AS dept, COUNT(*) AS cnt FROM employees GROUP BY JSON_VALUE(data, '$.department'))), 'topEarners' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE id, 'name' VALUE name, 'salary' VALUE salary) ORDER BY salary DESC) FROM (SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary FROM employees ORDER BY JSON_VALUE(data, '$.salary' RETURNING NUMBER) DESC FETCH FIRST 3 ROWS ONLY)), 'totalStats' VALUE (SELECT JSON_ARRAYAGG(JSON_OBJECT('_id' VALUE NULL, 'totalEmployees' VALUE cnt, 'avgSalary' VALUE avgSal, 'totalPayroll' VALUE totalPay)) FROM (SELECT COUNT(*) AS cnt, AVG(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS avgSal, SUM(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) AS totalPay FROM employees))) AS result FROM DUAL
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, JSON_VALUE(data, '$.colleagues.size()') AS colleagueCount, colleagues_cte.colleagues AS colleagues FROM employees base LEFT OUTER JOIN LATERAL (SELECT JSON_ARRAYAGG(g.data) AS colleagues FROM employees g WHERE JSON_VALUE(g.data, '$.department') = JSON_VALUE(base.data, '$.department') AND JSON_VALUE(g.data, '$.active') = true) colleagues_cte ON 1=1 WHERE JSON_VALUE(base.data, '$._id' RETURNING NUMBER) = :1
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
SELECT id, JSON_VALUE(data, '$.status') AS status, JSON_VALUE(data, '$.category') AS category FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed' AND JSON_VALUE(data, '$.category') = 'electronics' ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.department') AS department, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary FROM employees WHERE JSON_VALUE(data, '$.department') = 'Engineering' AND JSON_VALUE(data, '$.active') = 'true' AND JSON_VALUE(data, '$.salary' RETURNING NUMBER) >= 90000 ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.status') AS status FROM sales WHERE JSON_VALUE(data, '$.status') = 'cancelled' OR JSON_VALUE(data, '$.status') = 'refunded' ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.department') AS department FROM employees WHERE JSON_VALUE(data, '$.department') = 'Engineering' OR (JSON_VALUE(data, '$.department') = 'Sales' AND JSON_VALUE(data, '$.active') = 'true') ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.price' RETURNING NUMBER) AS price FROM products WHERE NOT (JSON_VALUE(data, '$.price' RETURNING NUMBER) > 100) ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.status') AS status FROM sales WHERE NOT (JSON_VALUE(data, '$.status') = 'completed' OR JSON_VALUE(data, '$.status') = 'pending') ORDER BY id
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
SELECT s.id, JSON_VALUE(s.data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(c.data, '$.tier') AS customerTier FROM sales s LEFT OUTER JOIN customers c ON JSON_VALUE(s.data, '$.customerId') = JSON_VALUE(c.data, '$._id') WHERE JSON_VALUE(s.data, '$.status') = 'completed' ORDER BY JSON_VALUE(s.data, '$.orderId' RETURNING NUMBER)
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
SELECT p.id, JSON_VALUE(p.data, '$.name') AS name, (SELECT COUNT(*) FROM inventory i WHERE JSON_VALUE(i.data, '$.productId') = JSON_VALUE(p.data, '$._id')) AS warehouseCount FROM products p WHERE JSON_VALUE(p.data, '$.active') = 'true' ORDER BY p.id
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
SELECT base.id, JSON_VALUE(base.data, '$.name') AS name, JSON_VALUE(base.data, '$.department') AS department, (SELECT COUNT(*) FROM employees e2 WHERE JSON_VALUE(e2.data, '$.department') = JSON_VALUE(base.data, '$.department')) AS colleagueCount FROM employees base WHERE JSON_VALUE(base.data, '$._id' RETURNING NUMBER) = 1 ORDER BY base.id
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
SELECT base.id, JSON_VALUE(base.data, '$.orderId' RETURNING NUMBER) AS orderId, CASE WHEN (SELECT COUNT(*) FROM customers c WHERE JSON_VALUE(c.data, '$.customerId') = JSON_VALUE(base.data, '$.customerId')) > 0 THEN 1 ELSE 0 END AS hasCustomer, (SELECT COUNT(*) FROM inventory i WHERE JSON_VALUE(i.data, '$.productId' RETURNING NUMBER) = JSON_VALUE(base.data, '$.orderId' RETURNING NUMBER)) AS inventoryCount FROM sales base WHERE JSON_VALUE(base.data, '$._id') = 'S001' ORDER BY base.id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.discount' RETURNING NUMBER) AS discount FROM sales WHERE JSON_VALUE(data, '$.discount') IS NULL ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, NVL(JSON_VALUE(data, '$.discount' RETURNING NUMBER), 0) AS discountApplied FROM sales ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.metadata.campaign') AS campaign FROM sales WHERE JSON_VALUE(data, '$.metadata.campaign') IS NULL ORDER BY id
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
SELECT JSON_VALUE(data, '$.region') AS "_id", SUM(JSON_VALUE(data, '$.discount' RETURNING NUMBER)) AS totalDiscount FROM sales GROUP BY JSON_VALUE(data, '$.region') ORDER BY JSON_VALUE(data, '$.region')
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
SELECT JSON_VALUE(data, '$.category') AS "_id", AVG(JSON_VALUE(data, '$.discount' RETURNING NUMBER)) AS avgDiscount FROM sales GROUP BY JSON_VALUE(data, '$.category') ORDER BY JSON_VALUE(data, '$.category')
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
SELECT id, JSON_OBJECT('orderId' VALUE JSON_VALUE(data, '$.orderId' RETURNING NUMBER), 'source' VALUE JSON_VALUE(data, '$.metadata.source'), 'campaign' VALUE JSON_VALUE(data, '$.metadata.campaign')) AS merged FROM sales WHERE JSON_VALUE(data, '$._id') = 'S001' ORDER BY id
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, JSON_VALUE(base.data, '$.salary') AS salary FROM employees base WHERE CASE WHEN JSON_VALUE(base.data, '$.salary') >= :1 THEN :2 ELSE :3 END <> :4 ORDER BY JSON_VALUE(base.data, '$._id')
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.orderId') AS orderId, JSON_VALUE(base.data, '$.status') AS status FROM sales base WHERE CASE WHEN JSON_VALUE(base.data, '$.status') = :1 THEN :2 ELSE :3 END <> :4 ORDER BY JSON_VALUE(base.data, '$.orderId')
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
SELECT JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.metadata.source') AS source, JSON_VALUE(data, '$.metadata.campaign') AS campaign FROM sales WHERE JSON_QUERY(data, '$.metadata') IS NOT NULL ORDER BY JSON_VALUE(data, '$.orderId' RETURNING NUMBER)
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name FROM products base ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 3 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, JSON_VALUE(base.data, '$.department') AS department FROM employees base ORDER BY DBMS_RANDOM.VALUE FETCH FIRST 5 ROWS ONLY
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.department') AS department, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary, salaryRank FROM (SELECT id, data, RANK() OVER (PARTITION BY JSON_VALUE(data, '$.department') ORDER BY JSON_VALUE(data, '$.salary' RETURNING NUMBER) DESC) AS salaryRank FROM employees) WHERE salaryRank = 1 ORDER BY JSON_VALUE(data, '$.department')
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) OVER (ORDER BY JSON_VALUE(data, '$.orderDate') ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS runningTotal FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed'
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.category') AS category, JSON_VALUE(data, '$.price' RETURNING NUMBER) AS price FROM (SELECT id, data, ROW_NUMBER() OVER (PARTITION BY JSON_VALUE(data, '$.category') ORDER BY JSON_VALUE(data, '$.price' RETURNING NUMBER) DESC) AS priceRankInCategory FROM products WHERE JSON_VALUE(data, '$.active') = 'true') WHERE priceRankInCategory = 1
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary, denseRank FROM (SELECT id, data, DENSE_RANK() OVER (ORDER BY JSON_VALUE(data, '$.salary' RETURNING NUMBER) DESC) AS denseRank FROM employees) WHERE denseRank <= 3 ORDER BY denseRank
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId FROM sales ORDER BY JSON_VALUE(data, '$.orderId' RETURNING NUMBER) FETCH FIRST 3 ROWS ONLY
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
SELECT id, JSON_VALUE(data, '$.name') AS name FROM employees ORDER BY JSON_VALUE(data, '$.name') OFFSET 5 ROWS
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.price' RETURNING NUMBER) AS price FROM products ORDER BY JSON_VALUE(data, '$.price' RETURNING NUMBER) DESC OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary FROM employees ORDER BY JSON_VALUE(data, '$.salary' RETURNING NUMBER) ASC
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.price' RETURNING NUMBER) AS price FROM products WHERE JSON_VALUE(data, '$.active') = 'true' ORDER BY JSON_VALUE(data, '$.price' RETURNING NUMBER) DESC
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.department') AS department, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary FROM employees ORDER BY JSON_VALUE(data, '$.department') ASC, JSON_VALUE(data, '$.salary' RETURNING NUMBER) DESC
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
SELECT JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, JSON_VALUE(data, '$.tax' RETURNING NUMBER) AS tax FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed' ORDER BY JSON_VALUE(data, '$.orderId' RETURNING NUMBER)
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
SELECT id, JSON_VALUE(data, '$.name') || ' - ' || JSON_VALUE(data, '$.department') AS fullInfo FROM employees ORDER BY id
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
SELECT id, LOWER(JSON_VALUE(data, '$.department')) AS deptLower FROM employees ORDER BY id
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
SELECT id, UPPER(JSON_VALUE(data, '$.name')) AS nameUpper FROM products ORDER BY id
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
SELECT id, SUBSTR(JSON_VALUE(data, '$.name'), 1, 4) AS namePrefix FROM customers ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, LENGTH(JSON_VALUE(data, '$.name')) AS nameLength FROM products ORDER BY id
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
SELECT id, TRIM(JSON_VALUE(data, '$.name')) AS trimmedName FROM employees ORDER BY id
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
null
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, CASE WHEN INSTR(JSON_VALUE(base.data, '$.name'), :1) = 0 THEN -1 ELSE INSTR(JSON_VALUE(base.data, '$.name'), :2) - 1 END AS aPosition FROM employees base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 5 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, CASE WHEN REGEXP_LIKE(JSON_VALUE(base.data, '$.name'), :1) THEN 1 ELSE 0 END AS hasVowelStart FROM employees base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 5 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, REGEXP_REPLACE(JSON_VALUE(base.data, '$.department'), :1, :2, 1, 1) AS modifiedDept FROM employees base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 5 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, REGEXP_REPLACE(JSON_VALUE(base.data, '$.name'), :1, :2) AS nameNoSpaces FROM employees base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 5 ROWS ONLY
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
SELECT id, JSON_VALUE(data, '$.name') AS name, LTRIM(JSON_VALUE(data, '$.name')) AS trimmedName FROM employees ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, RTRIM(JSON_VALUE(data, '$.name')) AS trimmedName FROM employees ORDER BY id
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.department') AS department, CASE WHEN UPPER(JSON_VALUE(data, '$.department')) = 'ENGINEERING' THEN 0 WHEN UPPER(JSON_VALUE(data, '$.department')) < 'ENGINEERING' THEN -1 ELSE 1 END AS compareResult FROM employees ORDER BY id
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", CASE WHEN JSON_VALUE(base.data, '$.status') IS NULL THEN 'null' WHEN JSON_VALUE(base.data, '$.status') IN ('true', 'false') THEN 'bool' WHEN REGEXP_LIKE(JSON_VALUE(base.data, '$.status'), '^-?[0-9]+$') THEN 'int' WHEN REGEXP_LIKE(JSON_VALUE(base.data, '$.status'), '^-?[0-9]+\.[0-9]+$') THEN 'double' ELSE 'string' END AS statusType, CASE WHEN JSON_VALUE(base.data, '$.amount') IS NULL THEN 'null' WHEN JSON_VALUE(base.data, '$.amount') IN ('true', 'false') THEN 'bool' WHEN REGEXP_LIKE(JSON_VALUE(base.data, '$.amount'), '^-?[0-9]+$') THEN 'int' WHEN REGEXP_LIKE(JSON_VALUE(base.data, '$.amount'), '^-?[0-9]+\.[0-9]+$') THEN 'double' ELSE 'string' END AS amountType FROM sales base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 3 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, TRUNC(TO_NUMBER(JSON_VALUE(base.data, '$.price'))) AS priceInt FROM products base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 5 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, TO_CHAR(JSON_VALUE(base.data, '$.salary')) AS salaryStr FROM employees base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 5 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, TO_BINARY_DOUBLE(JSON_VALUE(base.data, '$.price')) AS priceDouble FROM products base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 5 ROWS ONLY
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
SELECT JSON_VALUE(base.data, '$._id') AS "_id", JSON_VALUE(base.data, '$.name') AS name, CASE WHEN JSON_VALUE(base.data, '$.active') IS NULL OR TO_CHAR(JSON_VALUE(base.data, '$.active')) IN ('0', 'false') THEN 'false' ELSE 'true' END AS isActive FROM employees base ORDER BY JSON_VALUE(base.data, '$._id') FETCH FIRST 5 ROWS ONLY
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
SELECT id, JSON_VALUE(data, '$.name') AS name, 'employee' AS source FROM employees UNION ALL SELECT id, JSON_VALUE(data, '$.name') AS name, 'customer' AS source FROM customers ORDER BY source, name FETCH FIRST 10 ROWS ONLY
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
SELECT id, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, JSON_VALUE(data, '$.category') AS category FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed' UNION ALL SELECT id, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, JSON_VALUE(data, '$.category') AS category FROM sales WHERE JSON_VALUE(data, '$.status') = 'pending' ORDER BY id
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
SELECT COUNT(*) AS totalProducts, AVG(price) AS avgPrice FROM (SELECT JSON_VALUE(data, '$.price' RETURNING NUMBER) AS price FROM products WHERE JSON_VALUE(data, '$.category') = 'electronics' UNION ALL SELECT JSON_VALUE(data, '$.price' RETURNING NUMBER) AS price FROM products WHERE JSON_VALUE(data, '$.category') = 'tools')
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
SELECT s.id, JSON_VALUE(s.data, '$.orderId' RETURNING NUMBER) AS orderId, jt.product, jt.qty FROM sales s, JSON_TABLE(s.data, '$.items[*]' COLUMNS (product VARCHAR2(100) PATH '$.product', qty NUMBER PATH '$.qty')) jt WHERE JSON_VALUE(s.data, '$.orderId' RETURNING NUMBER) = 1001
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
SELECT s.id, JSON_VALUE(s.data, '$.orderId' RETURNING NUMBER) AS orderId, jt.tag FROM sales s LEFT OUTER JOIN JSON_TABLE(s.data, '$.tags[*]' COLUMNS (tag VARCHAR2(100) PATH '$')) jt ON 1=1 WHERE JSON_VALUE(s.data, '$.orderId' RETURNING NUMBER) IN (1001, 1007) ORDER BY JSON_VALUE(s.data, '$.orderId' RETURNING NUMBER)
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
SELECT id, JSON_VALUE(data, '$.orderId' RETURNING NUMBER) AS orderId, JSON_VALUE(data, '$.amount' RETURNING NUMBER) AS amount, SUM(JSON_VALUE(data, '$.amount' RETURNING NUMBER)) OVER (ORDER BY JSON_VALUE(data, '$.orderDate') ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS runningTotal FROM sales WHERE JSON_VALUE(data, '$.status') = 'completed' ORDER BY JSON_VALUE(data, '$.orderDate')
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
SELECT id, JSON_VALUE(data, '$.name') AS name, JSON_VALUE(data, '$.salary' RETURNING NUMBER) AS salary, AVG(JSON_VALUE(data, '$.salary' RETURNING NUMBER)) OVER (ORDER BY JSON_VALUE(data, '$.salary' RETURNING NUMBER) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS movingAvgSalary FROM employees ORDER BY JSON_VALUE(data, '$.salary' RETURNING NUMBER)
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

---

