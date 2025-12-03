# IMPL-031: Oracle Dot Notation Refactoring

## Overview
Refactor SQL generation from `JSON_VALUE(data, '$.field')` to Oracle's dot notation `alias.data.field` for cleaner, more readable SQL output and better type handling.

## Benefits
- Cleaner, more readable SQL
- Better automatic type inference (numbers stay as numbers)
- Shorter SQL statements
- More natural Oracle JSON syntax

## Current vs Target

| Operation | Current | Target |
|-----------|---------|--------|
| Simple field | `JSON_VALUE(data, '$.name')` | `base.data.name` |
| Nested field | `JSON_VALUE(data, '$.meta.source')` | `base.data.meta.source` |
| Array element | `JSON_VALUE(data, '$.items[0]')` | `base.data.items[0]` |
| With type cast | `JSON_VALUE(data, '$.amount' RETURNING NUMBER)` | Keep or use `CAST(base.data.amount AS NUMBER)` |

## Cases Where JSON_VALUE Must Be Retained
1. **Array size operations**: `JSON_VALUE(data, '$.items.size()')` - no dot notation equivalent
2. **Complex path filters**: `JSON_VALUE(data, '$.field?(@ > 5)')`
3. **JSON_TABLE operations**: Array flattening with `[*]`

## Implementation Phases

### Phase 1: Core FieldPathExpression (Priority: CRITICAL)
- [ ] **1.1** Modify `FieldPathExpression.render()` to generate dot notation
- [ ] **1.2** Handle the `returnType` case - decide CAST vs keep JSON_VALUE
- [ ] **1.3** Update `renderLookupFieldPath()` for $lookup results
- [ ] **1.4** Update `renderUnwindFieldPath()` for $unwind results
- [ ] **1.5** Update `FieldPathExpressionTest.java` (26 test cases)

### Phase 2: Expression Classes (Priority: HIGH)
- [ ] **2.1** `ArrayExpression.java` - array element access methods
- [ ] **2.2** `ComparisonExpression.java` - keep JSON_VALUE for size checks
- [ ] **2.3** `DateExpression.java` - field references in date operations
- [ ] **2.4** `ObjectExpression.java` - nested object access

### Phase 3: Stage Classes (Priority: HIGH)
- [ ] **3.1** `LookupStage.java` - JOIN ON conditions
- [ ] **3.2** `SetWindowFieldsStage.java` - PARTITION BY / ORDER BY
- [ ] **3.3** `GraphLookupStage.java` - recursive query paths
- [ ] **3.4** `PipelineRenderer.java` - post-window/post-group rendering

### Phase 4: Test Updates (Priority: CRITICAL)
- [ ] **4.1** Run integration tests after each phase
- [ ] **4.2** Update unit test assertions across 35+ test files
- [ ] **4.3** Verify all 142 integration tests pass

## Files to Modify

### Core Files
| File | Changes | Complexity |
|------|---------|------------|
| `FieldPathExpression.java` | Main render() method | HIGH |
| `PipelineRenderer.java` | 4+ render locations | HIGH |
| `ArrayExpression.java` | 5+ array operations | HIGH |
| `SetWindowFieldsStage.java` | 3 window function refs | MEDIUM |
| `LookupStage.java` | 2 JOIN ON conditions | MEDIUM |
| `GraphLookupStage.java` | 8+ recursive paths | HIGH |
| `ComparisonExpression.java` | 1 array size check | LOW |

### Test Files (35+)
- `FieldPathExpressionTest.java` - 26 tests
- `ArrayExpressionTest.java`
- `ComparisonExpressionTest.java`
- `LookupStageTest.java`
- `PipelineRendererTest.java`
- ... and 30+ more

## Design Decisions

### Decision 1: Type Casting Strategy
**Options:**
1. Use `CAST(base.data.field AS NUMBER)` - consistent dot notation
2. Keep `JSON_VALUE(data, '$.field' RETURNING NUMBER)` - proven to work
3. Hybrid - use dot notation when no casting needed, JSON_VALUE when needed

**Chosen:** Option 3 (Hybrid) - Use dot notation for simple field access, keep JSON_VALUE only when RETURNING clause is required.

### Decision 2: Array Size Operations
**Keep JSON_VALUE** for `.size()` operations as dot notation doesn't support method calls.

### Decision 3: Backward Compatibility
No backward compatibility flag needed - this is a clean refactor that produces equivalent SQL.

## Progress Tracking

| Phase | Status | Tests Passing |
|-------|--------|---------------|
| Phase 1 | Complete | 142/142 |
| Phase 2 | Complete | 142/142 |
| Phase 3 | Complete | 142/142 |
| Phase 4 | Complete | 142/142 |

## Completed Work (2025-12-02)

### FieldPathExpression.java
- [x] Modified `render()` to use dot notation: `base.data.fieldName`
- [x] Added `getDotNotationPath()` helper with field name quoting for Oracle identifiers
- [x] Added `quoteIfNeeded()` to quote fields starting with `_` (e.g., `"_id"`)
- [x] Updated `renderLookupFieldPath()` for $lookup results
- [x] Updated `renderUnwindFieldPath()` for $unwind results

### ConditionalExpression.java
- [x] Fixed `renderIfNull()` to cast FieldPathExpression to match literal type
- [x] JSON dot notation returns JSON type, NVL needs scalar - added `CAST(field AS NUMBER/VARCHAR2)`

### DateExpression.java
- [x] Fixed `render()` to use JSON_VALUE for FieldPathExpression arguments
- [x] GROUP BY with dot notation causes ORA-00979 - solved by using JSON_VALUE in date expressions
- [x] Comment: Dot notation doesn't work in GROUP BY contexts because Oracle treats `base.data` as a separate column reference

## Resolved Issues

### DATE005, DATE012 - GROUP BY with Dot Notation (FIXED)
- **Status**: PASS
- **Solution**: Modified DateExpression.render() to use JSON_VALUE for FieldPathExpression arguments instead of dot notation
- **Reason**: Dot notation like `base.data.field` in GROUP BY context causes Oracle to require `base.data` in GROUP BY clause, while JSON_VALUE works correctly in all contexts

## Notes
- Dot notation requires table alias: `alias.data.field` not just `data.field`
- String values retain quotes in output: `"Alice"` not `Alice`
- Automatic type coercion for arithmetic and string concatenation
- Use JSON_VALUE in expressions that may appear in GROUP BY (like date extractions)
