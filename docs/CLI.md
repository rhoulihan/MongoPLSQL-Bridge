# Command-Line Interface (CLI) Guide

The `mongo2sql` command-line tool translates MongoDB aggregation pipelines to Oracle SQL/JSON statements. This guide covers installation, usage, and examples.

## Quick Start

```bash
# Build the CLI
./gradlew :core:fatJar

# Translate a pipeline file
./mongo2sql pipeline.json

# With formatting options
./mongo2sql --collection orders --pretty --inline pipeline.json
```

## Installation

### Prerequisites

- **Java 17 or higher** - Required for running the CLI
- **Gradle** (optional) - For building from source

### Building from Source

```bash
# Clone the repository
git clone https://github.com/rhoulihan/MongoPLSQL-Bridge.git
cd MongoPLSQL-Bridge

# Build the fat JAR (includes all dependencies)
./gradlew :core:fatJar

# The JAR is created at: core/build/libs/core-1.0.0-SNAPSHOT-all.jar
```

### Running the CLI

The repository includes a wrapper script `mongo2sql` that handles JAR location and builds automatically if needed:

```bash
# Using the wrapper script (recommended)
./mongo2sql [options] <input-file>

# Direct JAR execution
java -jar core/build/libs/core-1.0.0-SNAPSHOT-all.jar [options] <input-file>
```

## Command-Line Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--collection <name>` | `-c` | Collection/table name (overrides file setting) | `collection` or from file |
| `--inline` | `-i` | Inline bind variables into SQL | Off (use bind variables) |
| `--pretty` | `-p` | Pretty-print the SQL output | Off |
| `--no-hints` | | Disable Oracle optimizer hints | Hints enabled |
| `--strict` | | Fail on unsupported operators | Off (warn only) |
| `--data-column <name>` | | JSON data column name | `data` |
| `--output <file>` | `-o` | Write output to file instead of stdout | stdout |
| `--version` | `-v` | Show version information | |
| `--help` | `-h` | Show help message | |

### Option Details

#### `--collection, -c`

Specifies the Oracle table/collection name. This overrides any collection name specified in the input file.

```bash
# Use "orders" as the table name
./mongo2sql --collection orders pipeline.json
```

#### `--inline, -i`

By default, the CLI generates SQL with bind variables (`:1`, `:2`, etc.) and lists their values in comments. With `--inline`, values are embedded directly in the SQL.

```bash
# Without --inline (default)
./mongo2sql pipeline.json
# Output: WHERE base.data.status = :1
# -- Bind variables:
# -- :1 = 'active'

# With --inline
./mongo2sql --inline pipeline.json
# Output: WHERE base.data.status = 'active'
```

#### `--pretty, -p`

Formats the SQL output with proper indentation and line breaks for readability.

```bash
./mongo2sql --pretty pipeline.json
```

#### `--no-hints`

Disables Oracle optimizer hints in the generated SQL. By default, the translator adds hints like `/*+ NO_XMLQUERY_REWRITE */` to improve query performance.

```bash
./mongo2sql --no-hints pipeline.json
```

#### `--strict`

In strict mode, the CLI fails with an error if the pipeline contains unsupported operators. By default, unsupported operators generate warnings but translation continues.

```bash
./mongo2sql --strict pipeline.json
```

#### `--data-column`

Specifies the name of the JSON column in your Oracle table. Most Oracle JSON Collection setups use `data` as the column name.

```bash
# If your JSON column is named "json_doc"
./mongo2sql --data-column json_doc pipeline.json
```

#### `--output, -o`

Writes the generated SQL to a file instead of stdout.

```bash
./mongo2sql --output result.sql pipeline.json
# Output: "Output written to: result.sql"
```

## Input File Formats

The CLI accepts three input file formats:

### Format 1: Raw Pipeline Array

A JSON array of pipeline stages. Use `--collection` to specify the table name.

```json
[
  {"$match": {"status": "active"}},
  {"$group": {"_id": "$category", "count": {"$sum": 1}}},
  {"$sort": {"count": -1}},
  {"$limit": 10}
]
```

```bash
./mongo2sql --collection orders pipeline.json
```

### Format 2: Single Pipeline with Metadata

A JSON object containing pipeline metadata and the stages.

```json
{
  "name": "Active Orders by Category",
  "description": "Count active orders grouped by category",
  "collection": "orders",
  "pipeline": [
    {"$match": {"status": "active"}},
    {"$group": {"_id": "$category", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
  ]
}
```

```bash
./mongo2sql pipeline.json
```

### Format 3: Multiple Pipelines

A JSON object containing an array of pipeline definitions. Each pipeline is translated and output with a comment header.

```json
{
  "pipelines": [
    {
      "id": "ORDERS001",
      "name": "Active Orders",
      "collection": "orders",
      "pipeline": [{"$match": {"status": "active"}}]
    },
    {
      "id": "PRODUCTS001",
      "name": "Low Stock Products",
      "collection": "products",
      "pipeline": [{"$match": {"stock": {"$lt": 10}}}]
    }
  ]
}
```

```bash
./mongo2sql pipelines.json
```

Output:
```sql
-- Pipeline: Active Orders

SELECT /*+ NO_XMLQUERY_REWRITE */ base.data
FROM orders base
WHERE base.data.status = :1

-- Bind variables:
-- :1 = 'active'

-- Pipeline: Low Stock Products

SELECT /*+ NO_XMLQUERY_REWRITE */ base.data
FROM products base
WHERE base.data.stock < :1

-- Bind variables:
-- :1 = 10
```

## Examples

### Basic Translation

```bash
# Translate a simple match pipeline
echo '[{"$match": {"status": "active"}}]' > /tmp/pipeline.json
./mongo2sql --collection orders /tmp/pipeline.json
```

### Production-Ready SQL

Generate formatted SQL with inlined values for direct execution:

```bash
./mongo2sql --pretty --inline --collection orders pipeline.json
```

### Save to File

```bash
./mongo2sql --pretty --output queries/orders-report.sql pipeline.json
```

### Batch Translation

Translate multiple pipelines from a single file:

```bash
./mongo2sql --pretty pipelines.json > all-queries.sql
```

### Integration with Other Tools

Pipe directly to SQL*Plus or other Oracle clients:

```bash
./mongo2sql --inline --collection orders pipeline.json | sqlplus user/pass@db
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Invalid arguments or missing input file |
| 2 | Error reading input file |
| 3 | Translation error |

## Output Format

### Standard Output

By default, the CLI outputs:
1. The generated SQL statement
2. Bind variable values (as comments, unless `--inline` is used)
3. Any warnings (as comments)

### Multiple Pipelines

When translating multiple pipelines, each is preceded by a comment header:

```sql
-- Pipeline: Pipeline Name
-- Optional description

SELECT ...

-- Pipeline: Next Pipeline
...
```

## Troubleshooting

### "Could not build JAR file"

Run the Gradle build manually:
```bash
./gradlew :core:fatJar
```

### "Unknown option: --xxx"

Check the option spelling. Use `--help` to see all available options.

### "Invalid input file format"

Ensure your JSON file is one of the three supported formats:
- A JSON array starting with `[`
- An object with a `pipeline` key
- An object with a `pipelines` key

### Translation Warnings

Warnings indicate potential issues but don't prevent translation. Use `--strict` to treat warnings as errors.

## See Also

- [README](../README.md) - Project overview and quick start
- [Test Catalog](test-catalog.html) - Interactive test catalog with examples
- [Java API](../README.md#java-api-usage) - Programmatic usage
