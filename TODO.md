# TODO - MongoDB to Oracle SQL Translator

This file tracks improvements, enhancements, and features discovered during development.
Last updated: 2025-12-08

---

## MongoDB Compatibility Gap Analysis - Implementation Roadmap

Based on comparison with [MongoDB's official jstests](https://github.com/mongodb/mongo/tree/master/jstests/aggregation).
See `docs/TEST-GAP-ANALYSIS.md` for full analysis.

### Phase 1: Date Operations (Highest Impact)

Critical for analytics workloads. All have direct Oracle equivalents.

- [x] **$dateAdd** - Add time interval to date (completed: 2025-12-08)
  - Files: `DateArithmeticExpression.java`, `DateArithmeticOp.java`, `ExpressionParser.java`
  - Oracle: `TO_TIMESTAMP(...) + INTERVAL 'n' UNIT`
  - MongoDB: `{ $dateAdd: { startDate: "$date", unit: "day", amount: 5 } }`

- [x] **$dateSubtract** - Subtract time interval from date (completed: 2025-12-08)
  - Files: `DateArithmeticExpression.java`, `DateArithmeticOp.java`, `ExpressionParser.java`
  - Oracle: `TO_TIMESTAMP(...) - INTERVAL 'n' UNIT`
  - MongoDB: `{ $dateSubtract: { startDate: "$date", unit: "day", amount: 5 } }`

- [x] **$dateDiff** - Calculate difference between dates (completed: 2025-12-08)
  - Files: `DateArithmeticExpression.java`, `DateArithmeticOp.java`, `ExpressionParser.java`
  - Oracle: `MONTHS_BETWEEN()` for months, date subtraction for days/hours/etc.
  - MongoDB: `{ $dateDiff: { startDate: "$start", endDate: "$end", unit: "day" } }`

- [x] **$dateFromString** - Parse string to date (completed: 2025-12-08)
  - Files: `DateStringExpression.java`, `DateStringOp.java`, `ExpressionParser.java`
  - Oracle: `TO_TIMESTAMP(string, 'YYYY-MM-DD')` with MongoDB format conversion
  - MongoDB: `{ $dateFromString: { dateString: "$dateStr", format: "%Y-%m-%d" } }`

- [x] **$dateToString** - Format date as string (completed: 2025-12-08)
  - Files: `DateStringExpression.java`, `DateStringOp.java`, `ExpressionParser.java`
  - Oracle: `TO_CHAR(TO_TIMESTAMP(...), 'YYYY-MM-DD')` with MongoDB format conversion
  - MongoDB: `{ $dateToString: { date: "$date", format: "%Y-%m-%d" } }`

- [x] **$dateTrunc** - Truncate date to unit (completed: 2025-12-08)
  - Files: `DateTruncExpression.java`, `ExpressionParser.java`
  - Oracle: `TRUNC(TO_TIMESTAMP(...), 'MONTH')` with unit mapping
  - MongoDB: `{ $dateTrunc: { date: "$date", unit: "month" } }`

- [x] **$dateFromParts** - Construct date from parts (completed: 2025-12-08)
  - Files: `DatePartsExpression.java`, `ExpressionParser.java`
  - Oracle: `TO_TIMESTAMP(year||'-'||month||'-'||day||' '||hour||':'||minute||':'||second, 'YYYY-MM-DD HH24:MI:SS')`
  - MongoDB: `{ $dateFromParts: { year: 2023, month: 6, day: 15 } }`

- [x] **$dateToParts** - Extract parts from date (completed: 2025-12-08)
  - Files: `DatePartsExpression.java`, `ExpressionParser.java`
  - Oracle: `JSON_OBJECT('year' VALUE EXTRACT(YEAR FROM ts), 'month' VALUE EXTRACT(MONTH FROM ts), ...)`
  - MongoDB: `{ $dateToParts: { date: "$date" } }`

### Phase 2: Type Conversion Enhancement (COMPLETED)

All type conversion operators were previously implemented.

- [x] **$convert** - Generic type conversion with error handling (completed: previously)
  - Files: `TypeConversionExpression.java`, `TypeConversionOp.java`, `ExpressionParser.java`
  - Oracle: `NVL(...)` for onNull handling
  - MongoDB: `{ $convert: { input: "$field", to: "int", onError: 0, onNull: 0 } }`

- [x] **$isNumber** - Check if value is numeric (completed: previously)
  - Files: `TypeConversionExpression.java`, `TypeConversionOp.java`, `ExpressionParser.java`
  - Oracle: `CASE WHEN REGEXP_LIKE(TO_CHAR(...), '^-?[0-9]+(\.[0-9]+)?$') THEN 1 ELSE 0 END`
  - MongoDB: `{ $isNumber: "$field" }`

- [x] **$toDecimal** - Convert to decimal (completed: previously)
  - Files: `TypeConversionExpression.java`, `TypeConversionOp.java`, `ExpressionParser.java`
  - Oracle: `TO_NUMBER(...)`
  - MongoDB: `{ $toDecimal: "$field" }`

- [x] **$toLong** - Convert to long integer (completed: previously)
  - Files: `TypeConversionExpression.java`, `TypeConversionOp.java`, `ExpressionParser.java`
  - Oracle: `TRUNC(TO_NUMBER(...))`
  - MongoDB: `{ $toLong: "$field" }`

- [x] **$toObjectId** - Convert to ObjectId (completed: previously)
  - Files: `TypeConversionExpression.java`, `TypeConversionOp.java`, `ExpressionParser.java`
  - Note: Returns value as-is (ObjectId is stored as string in Oracle JSON)

### Phase 3: Array Enhancements (MOSTLY COMPLETED)

Most array operators were previously implemented. Only $range and $zip are pending.

- [x] **$indexOfArray** - Find index of element in array (completed: previously)
  - Files: `ArrayExpression.java`, `ArrayOp.java`, `ExpressionParser.java`
  - Oracle: Stub implementation
  - MongoDB: `{ $indexOfArray: [ "$arr", "value" ] }`

- [x] **$objectToArray** - Convert object to array of k-v pairs (completed: previously)
  - Files: `ObjectExpression.java`, `ObjectOp.java`, `ExpressionParser.java`
  - Oracle: JSON_TABLE to extract keys and values
  - MongoDB: `{ $objectToArray: "$obj" }`

- [x] **$arrayToObject** - Convert array of k-v pairs to object (completed: previously)
  - Files: `ObjectExpression.java`, `ObjectOp.java`, `ExpressionParser.java`
  - Oracle: JSON_OBJECT with aggregation
  - MongoDB: `{ $arrayToObject: "$arr" }`

- [x] **$range** - Generate array of integers (completed: 2025-12-08)
  - Files: `ArrayExpression.java`, `ArrayOp.java`, `ExpressionParser.java`
  - Oracle: `SELECT JSON_ARRAYAGG(n) FROM (SELECT start + (LEVEL-1)*step AS n FROM DUAL CONNECT BY ...)`
  - MongoDB: `{ $range: [ 0, 10, 2 ] }` (start, end, step)

- [x] **$sortArray** - Sort array elements (completed: previously)
  - Files: `ArrayExpression.java`, `ArrayOp.java`, `ExpressionParser.java`
  - Oracle: JSON_TABLE with ORDER BY and JSON_ARRAYAGG
  - MongoDB: `{ $sortArray: { input: "$arr", sortBy: { field: 1 } } }`

- [x] **$zip** - Merge arrays element-wise (completed: 2025-12-08)
  - Files: `ArrayExpression.java`, `ArrayOp.java`, `ExpressionParser.java`
  - Oracle: JSON_TABLE with row numbers and JOIN to zip arrays together
  - MongoDB: `{ $zip: { inputs: [ "$arr1", "$arr2" ], useLongestLength: true, defaults: [0, "N/A"] } }`

### Phase 4: Complete Stage Implementations (COMPLETED)

- [x] **$unset Stage** - Remove fields (alias for $project exclusion) (completed: previously)
  - Files: `UnsetStage.java`, `UnsetStageParser.java`, `StageParserRegistry.java`
  - Oracle: SELECT with excluded fields via ProjectStage
  - MongoDB: `{ $unset: ["field1", "field2"] }`

- [x] **$densify Stage** - Fill gaps in time series data (completed: 2025-12-08)
  - Files: `DensifyStage.java`, `DensifyStageParser.java`, `DensifyStageTest.java`, `DensifyStageParserTest.java`
  - Oracle: Recursive CTE with date/sequence generation
  - MongoDB: `{ $densify: { field: "date", range: { step: 1, unit: "day" } } }`

- [x] **$fill Stage** - Fill missing values (completed: 2025-12-08)
  - Files: `FillStage.java`, `FillStageParser.java`, `FillStageTest.java`, `FillStageParserTest.java`
  - Oracle: LAG/LEAD IGNORE NULLS window functions, COALESCE
  - MongoDB: `{ $fill: { output: { field: { method: "linear" } } } }`

### Phase 5: Mathematical Operations (COMPLETED)

All math operators were previously implemented.

- [x] **$exp** - Euler's number raised to power (completed: previously)
  - Files: `ArithmeticExpression.java`, `ArithmeticOp.java`, `ExpressionParser.java`
  - Oracle: `EXP(n)`
  - MongoDB: `{ $exp: "$field" }`

- [x] **$ln** - Natural logarithm (completed: previously)
  - Files: `ArithmeticExpression.java`, `ArithmeticOp.java`, `ExpressionParser.java`
  - Oracle: `LN(n)`
  - MongoDB: `{ $ln: "$field" }`

- [x] **$log10** - Base-10 logarithm (completed: previously)
  - Files: `ArithmeticExpression.java`, `ArithmeticOp.java`, `ExpressionParser.java`
  - Oracle: `LOG(10, n)` (Note: $log with custom base not yet implemented)
  - MongoDB: `{ $log10: "$field" }`

- [x] **$pow** - Raise to power (completed: previously)
  - Files: `ArithmeticExpression.java`, `ArithmeticOp.java`, `ExpressionParser.java`
  - Oracle: `POWER(base, exponent)`
  - MongoDB: `{ $pow: [ "$base", "$exp" ] }`

- [x] **$sqrt** - Square root (completed: previously)
  - Files: `ArithmeticExpression.java`, `ArithmeticOp.java`, `ExpressionParser.java`
  - Oracle: `SQRT(n)`
  - MongoDB: `{ $sqrt: "$field" }`

---

## Completed Implementations

### Recent Completions (2025-12-08)

- [x] **$setWindowFields Global Window (without partitionBy)** (completed: 2025-12-08)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/SetWindowFieldsStage.java`
  - Fix: Handle case where partitionBy is omitted (global window over entire result set)
  - Test: WINDOW010 ($documentNumber without partitionBy) now passing

- [x] **$unwind preserveNullAndEmptyArrays Option** (completed: 2025-12-08)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/UnwindStage.java`
  - Fix: LEFT OUTER JOIN pattern with COALESCE to preserve nulls
  - Test: UNWIND005 (unwind with preserveNullAndEmptyArrays) now passing

- [x] **$in Expression Operator in Expressions** (completed: 2025-12-08)
  - File: `core/src/main/java/com/oracle/mongodb/translator/parser/ExpressionParser.java`
  - Fix: Support $in as general comparison expression returning boolean
  - Test: COMPLEX012 ($in inside $cond expression) now passing

- [x] **$sum/$avg Array Operators** (completed: 2025-12-08)
  - Files: `ArrayExpression.java`, `ArrayOp.java`, `FieldPathExpression.java`
  - Added: Support for $sum and $avg operating on array elements

### High Priority - Stage Implementations

- [x] **$out Stage - Full Implementation** (completed: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:521-590`
  - Implemented: `INSERT INTO table (data) SELECT ...` pattern
  - Supports: database.collection notation
  - Tests: `PipelineRendererTest.java` lines 2437-2560

- [x] **$merge Stage - Full Implementation** (completed: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:599-712`
  - Implemented: Full Oracle MERGE statement generation
  - Supports: `MERGE INTO t USING (SELECT...) s ON (...) WHEN MATCHED THEN... WHEN NOT MATCHED THEN...`
  - Supports: whenMatched (REPLACE, MERGE with JSON_MERGEPATCH, KEEP_EXISTING, FAIL)
  - Supports: whenNotMatched (INSERT, DISCARD, FAIL)
  - Tests: `PipelineRendererTest.java` lines 2566+

- [x] **$lookup Pipeline Form (Correlated Subquery)** (completed: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/LookupStage.java`
  - Implemented: LATERAL join with JSON_ARRAYAGG for correlated subqueries
  - Supports: let variable bindings with proper substitution

- [x] **$graphLookup Non-Recursive Support** (completed: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:1026-1187`
  - Implemented: GRAPHLOOKUP001 (maxDepth=0) - simple single-level lookup via LATERAL join
  - Tests: `query-tests/tests/test-cases.json` - GRAPHLOOKUP001 passing

- [x] **$reduce Array Operation** (completed: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:575-645`
  - Implemented: Pattern detection for common reductions
  - Supports: Sum pattern (ADD -> SUM), Concat pattern (CONCAT -> LISTAGG)
  - Tests: `ArrayExpressionTest.java` shouldRenderReduceSumPattern, shouldRenderReduceWithConcatPattern

- [x] **$filter/$map Fallback for Expression Arrays** (completed: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:544-573`
  - Implemented: Proper fallback handling for non-field-path arrays
  - Field paths: Full JSON_TABLE with JSON_ARRAYAGG support
  - Expression arrays: Graceful fallback with descriptive comment
  - Tests: `ArrayExpressionTest.java` shouldRenderFilterOnExpressionArray, shouldRenderMapOnExpressionArray

- [x] **Variable Binding for $filter/$map/$reduce** (completed: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:544-700`
  - Implemented: $$item.field and $$this.field variable binding support
  - $filter: Extracts field from condition, creates JSON_TABLE with field column
  - $map: Extracts field from mapping expression, creates JSON_TABLE with field column
  - $reduce: Extracts $$this.field from ADD expressions for SUM pattern
  - Tests: `ArrayExpressionTest.java` shouldRenderFilterWithVariableFieldAccess, shouldRenderMapWithVariableFieldAccess, shouldRenderReduceSumPatternWithNestedFieldAccess

- [x] **$filter/$map Empty Array Handling** (completed: 2025-12-05)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:550-602`
  - Issue: Oracle JSON_ARRAYAGG returns NULL when no rows match, MongoDB returns empty array `[]`
  - Fix: Wrapped JSON_ARRAYAGG with COALESCE(..., JSON_ARRAY()) pattern
  - Applied to: $filter (lines 551-552, 565-566), $map (lines 587-589, 600-602)
  - Test: ARR015 ($filter on field path array) now shows "docs match" with empty arrays

- [x] **$facet with Pre-Facet $match + $group Stages** (completed: 2025-12-05)
  - File: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:1977-2158`
  - Issue: Customer query with $match + $group before $facet wasn't processing preceding stages
  - Fix: Added `renderFacetCountQuery`, `renderFacetPaginationQuery`, `renderPreFacetGroupQuery` methods
  - Supports: Pagination patterns like `{recordCount: [{$count}], data: [{$skip}, {$limit}]}`
  - Supports: Proper $match + $group processing before $facet sub-pipelines
  - Tests: FACET_PAGINATION001, FACET_PAGINATION002 passing
  - Data: `query-tests/scripts/generate-purchase-orders.js` for test data generation

- [x] **$facet with Post-Facet $project Transformations** (completed: 2025-12-05)
  - File: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:1674-1846`
  - Issue: $project stage after $facet that reshapes facet output wasn't being processed
  - Fix: Added `postFacetProjectStage` tracking in PipelineComponents and new rendering methods
  - Added: `sawFacetStage` flag in `analyzePipeline()` to detect post-facet stages
  - Added: `renderPostFacetProjectSelectClause()` for custom field names from $project
  - Added: `renderPostFacetFieldExpression()` for facet field renaming (e.g., `topLocations: "$results"`)
  - Added: `renderArrayElemAtFacetExtraction()` for scalar extraction (e.g., `$arrayElemAt: ["$summary.count", 0]`)
  - Supports: Complex patterns like `{locationCount: {$arrayElemAt: ["$summary.count", 0]}, topLocations: "$results"}`
  - Tests: FACET_PAGINATION003 passing

---

## Stub/Incomplete Implementations

- [ ] **$graphLookup Recursive Depth Support** (discovered: 2025-12-05)
  - File: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:1026-1187`
  - Issue: Oracle 23ai does not support CTEs inside LATERAL that reference outer columns (ORA-00904)
  - Issue: PRIOR keyword does not work with JSON dot notation (ORA-19200)
  - Current: Returns stub SQL with empty result set for recursive cases (maxDepth > 0)
  - Skipped Tests:
    - `query-tests/tests/test-cases.json` - GRAPHLOOKUP002 (recursive hierarchy traversal)
    - `PipelineRendererTest.java` - 7 tests with @Disabled annotation
  - Potential Solution: Requires JSON_VALUE usage (loses type information) or future Oracle features

---

## Improvements

- [ ] **PipelineRenderer Empty Result Handling** (discovered: 2025-12-04)
  - Files: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:537`, `PipelineRenderer.java:1054`
  - Current: Uses placeholder selection when nothing is rendered
  - Consider: Better error handling or warning when pipeline produces no output

---

## Future Features

- [ ] **Unsupported Operator Registry Enhancement**
  - Files: `core/src/main/java/com/oracle/mongodb/translator/parser/PipelineParser.java:62`, `ExpressionParser.java:129,215,427,616`
  - Consider: Adding detailed error messages with suggested alternatives for each unsupported operator

- [ ] **String Operations - $regexFind, $regexFindAll** (Medium Priority)
  - Oracle: REGEXP_SUBSTR with CONNECT BY for findAll
  - MongoDB: `{ $regexFind: { input: "$field", regex: "pattern" } }`

- [ ] **$let Expression** - Define and use variables in expression
  - Oracle: WITH clause or inline substitution
  - MongoDB: `{ $let: { vars: { x: 1 }, in: { $add: ["$$x", 1] } } }`

- [ ] **$literal Expression** - Return value without parsing
  - Oracle: Direct value embedding
  - MongoDB: `{ $literal: "$field" }` (returns the string "$field")

---

## Technical Debt

- [x] **Variable Binding Support for Complex Array Operations** (completed: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:544-700`
  - Note: $filter, $map, $reduce now support variable bindings ($$item.field, $$this.field)
  - See "Variable Binding for $filter/$map/$reduce" in Completed Implementations section

- [ ] **Type Preservation for JSON Field Paths** (discovered: 2025-12-05)
  - Issue: Oracle JSON_VALUE returns strings by default, MongoDB preserves types
  - Example: MongoDB `orderId: 1002` (number) vs Oracle `"ORDERID": "1002"` (string)
  - Files: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/FieldPathExpression.java`
  - Potential solutions:
    - Add `RETURNING NUMBER` clause for numeric fields (requires type inference)
    - Use JSON_QUERY for non-scalar values
    - Implement schema inference from sample documents
    - Add explicit type hints in translation options
  - Complexity: Medium-High (requires type propagation through AST)

---

## Notes

Items are categorized by priority:
- **Phase 1-5**: Systematic implementation of MongoDB compatibility gaps
- **High Priority**: Core functionality that limits translator capabilities
- **Medium Priority**: Features that improve completeness
- **Low Priority**: Nice-to-have improvements
- **Technical Debt**: Code quality improvements

When implementing items:
1. Write tests first (TDD approach)
2. Ensure all pre-commit hooks pass
3. Update this file to mark items as complete
