# TODO - MongoDB to Oracle SQL Translator

This file tracks improvements, enhancements, and features discovered during development.
Last updated: 2025-12-04

## Stub/Incomplete Implementations

### High Priority - Stage Implementations

- [ ] **$out Stage - Full Implementation** (discovered: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/OutStage.java:38-39`
  - Current: Stub implementation that only renders a comment
  - Needed: Full implementation using CREATE TABLE AS SELECT or DROP/INSERT pattern
  - Oracle translation: `INSERT INTO outputCollection (data) SELECT data FROM (aggregation query)`

- [ ] **$merge Stage - Full Implementation** (discovered: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/MergeStage.java:40-41`
  - Current: Stub implementation for AST structure only
  - Needed: Full MERGE statement generation with whenMatched/whenNotMatched handling
  - Oracle translation: `MERGE INTO targetCollection t USING (SELECT...) s ON (...) WHEN MATCHED THEN... WHEN NOT MATCHED THEN...`

- [ ] **$lookup Pipeline Form (Correlated Subquery)** (discovered: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/LookupStage.java:186-190`
  - Current: Throws UnsupportedOperatorException for let/pipeline form
  - Needed: Support for correlated subqueries using Oracle LATERAL or correlated subquery syntax
  - Use case: Complex joins with variable bindings

### Medium Priority - Expression Implementations

- [ ] **$graphLookup Recursive Depth Support** (discovered: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/stage/GraphLookupStage.java:385-387`
  - Current: Uses empty placeholder CTE for recursive cases (maxDepth > 0 or null)
  - Needed: Full recursive CTE implementation using Oracle's CONNECT BY or recursive WITH clause
  - Note: maxDepth=0 case works correctly

- [ ] **$reduce Array Operation** (discovered: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:564-567`
  - Current: Renders `/* $reduce not fully supported */ NULL`
  - Needed: Implement accumulator handling in SQL, possibly using recursive CTE or MODEL clause

- [ ] **$filter Array Operation - General Case** (discovered: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:544-552`
  - Current: Basic JSON_TABLE implementation for field path arrays only
  - Needed: Support for expression arrays, not just field path references

- [ ] **$map Array Operation - General Case** (discovered: 2025-12-04)
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:554-562`
  - Current: Basic JSON_TABLE implementation for field path arrays only
  - Needed: Support for applying mapping expressions to each element properly

## Improvements

- [ ] **PipelineRenderer Empty Result Handling** (discovered: 2025-12-04)
  - Files: `core/src/main/java/com/oracle/mongodb/translator/generator/PipelineRenderer.java:537`, `PipelineRenderer.java:1054`
  - Current: Uses placeholder selection when nothing is rendered
  - Consider: Better error handling or warning when pipeline produces no output

## Future Features

- [ ] **Unsupported Operator Registry Enhancement**
  - Files: `core/src/main/java/com/oracle/mongodb/translator/parser/PipelineParser.java:62`, `ExpressionParser.java:129,215,427,616`
  - Consider: Adding detailed error messages with suggested alternatives for each unsupported operator

## Technical Debt

- [ ] **Variable Binding Support for Complex Array Operations**
  - File: `core/src/main/java/com/oracle/mongodb/translator/ast/expression/ArrayExpression.java:540-541`
  - Note: $filter, $map, $reduce require variable bindings ($$this, $$value, etc.) which need proper scoping

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
