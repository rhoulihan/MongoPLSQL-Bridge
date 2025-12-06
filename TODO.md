# TODO - MongoDB to Oracle SQL Translator

This file tracks improvements, enhancements, and features discovered during development.
Last updated: 2025-12-05

## Completed Implementations

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

## Improvements

- [ ] **$facet with Aggregation Accumulators + Post-Facet $project** (discovered: 2025-12-05)
  - File: `query-tests/tests/test-cases.json` - FACET_PAGINATION003
  - Issue: Complex $facet patterns with $group accumulators (totalAmount, orderCount) and post-facet $project stage
  - Current: Basic pagination patterns work; complex accumulator + sort + reshape patterns need enhancement
  - Required: Handle $project stage after $facet that reshapes facet output (e.g., `$arrayElemAt`, field renames)
  - Complexity: Medium-High (requires proper handling of facet output as intermediate result)

- [ ] **PipelineRenderer Empty Result Handling** (discovered: 2025-12-04)
  - Files: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:537`, `PipelineRenderer.java:1054`
  - Current: Uses placeholder selection when nothing is rendered
  - Consider: Better error handling or warning when pipeline produces no output

## Future Features

- [ ] **Unsupported Operator Registry Enhancement**
  - Files: `core/src/main/java/com/oracle/mongodb/translator/parser/PipelineParser.java:62`, `ExpressionParser.java:129,215,427,616`
  - Consider: Adding detailed error messages with suggested alternatives for each unsupported operator

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

## Notes

Items are categorized by priority:
- **High Priority**: Core functionality that limits translator capabilities
- **Medium Priority**: Features that improve completeness
- **Low Priority**: Nice-to-have improvements
- **Technical Debt**: Code quality improvements

When implementing items:
1. Write tests first (TDD approach)
2. Ensure all pre-commit hooks pass
3. Update this file to mark items as complete
