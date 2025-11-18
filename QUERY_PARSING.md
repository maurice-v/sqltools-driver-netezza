# Query Parsing and Execution Guide

## Overview
The Netezza driver now supports intelligent query parsing, allowing you to work with multiple SQL statements in a single file. Each semicolon-terminated statement is recognized as a separate executable query.

## How It Works

### Statement Separation
Queries are separated by semicolons (`;`). The driver intelligently handles:

- **String Literals**: Semicolons inside single or double quotes are ignored
  ```sql
  SELECT 'This ; is not a separator' AS text;
  ```

- **Line Comments**: Semicolons in line comments (`--`) are ignored
  ```sql
  -- This semicolon ; is ignored
  SELECT * FROM table1;
  ```

- **Block Comments**: Semicolons in block comments (`/* */`) are ignored
  ```sql
  /* This ; is also ignored */
  SELECT * FROM table2;
  ```

### Executing Individual Queries

#### Method 1: Run on Active Query
1. Place your cursor anywhere within a query
2. Use the keyboard shortcut or click "Run on active connection"
3. Only the query containing your cursor will execute

#### Method 2: Select and Run
1. Highlight/select one or more complete queries
2. Execute the selection
3. Only the selected queries will run

#### Method 3: Run All
1. Don't select anything
2. Execute the file
3. All queries in the file will run sequentially

## Examples

### Example 1: Simple Multiple Queries
```sql
-- Query 1
SELECT CURRENT_USER;

-- Query 2
SELECT CURRENT_CATALOG;

-- Query 3
SELECT VERSION();
```

Place your cursor on any line of Query 2, and only `SELECT CURRENT_CATALOG;` will execute.

### Example 2: Complex Queries with Comments
```sql
-- Get all tables in a schema
SELECT TABLENAME 
FROM _V_TABLE 
WHERE SCHEMA = 'ADMIN'
ORDER BY TABLENAME;  -- This semicolon ends the statement

/* Now get the column count for each table
   This is a multi-line comment */
SELECT 
    NAME AS table_name,
    COUNT(*) AS column_count
FROM _V_RELATION_COLUMN
WHERE SCHEMA = 'ADMIN'
GROUP BY NAME
ORDER BY column_count DESC;
```

### Example 3: Queries with String Literals
```sql
-- This query has a semicolon in the string
SELECT 
    'Query 1; with semicolon' AS description,
    COUNT(*) AS total
FROM _V_TABLE;

-- This is a separate query
SELECT 'Query 2' AS description;
```

## Best Practices

### 1. Always Terminate Statements
End each query with a semicolon for clear separation:
```sql
SELECT * FROM table1;  -- ✓ Good
SELECT * FROM table2   -- ✗ Will be treated as part of next query
SELECT * FROM table3;
```

### 2. Use Comments Liberally
Add comments to describe what each query does:
```sql
-- Get user information
SELECT CURRENT_USER, CURRENT_CATALOG;

-- Count tables by schema
SELECT SCHEMA, COUNT(*) FROM _V_TABLE GROUP BY SCHEMA;
```

### 3. One Query Per Logical Operation
Break complex operations into separate queries:
```sql
-- Step 1: Create temp table
CREATE TEMP TABLE temp_data AS 
SELECT * FROM source_table WHERE date = CURRENT_DATE;

-- Step 2: Analyze the data
SELECT COUNT(*), AVG(value) FROM temp_data;

-- Step 3: Clean up
DROP TABLE temp_data;
```

### 4. Test Queries Individually
When developing complex queries:
1. Write each query separately
2. Test each one individually by placing cursor on it
3. Fix any issues before running all queries together

## Keyboard Shortcuts

Default SQLTools shortcuts:
- **Execute Query**: `Ctrl+E Ctrl+E` (Windows/Linux) or `Cmd+E Cmd+E` (Mac)
- **Execute Current Query**: `Ctrl+E Ctrl+Q` (Windows/Linux) or `Cmd+E Cmd+Q` (Mac)
- **Run Selected Query**: `Ctrl+E Ctrl+R` (Windows/Linux) or `Cmd+E Cmd+R` (Mac)

## Troubleshooting

### Query Not Being Recognized
**Problem**: Cursor is on a query but wrong query executes

**Solution**: 
- Ensure each query ends with a semicolon
- Check for unclosed string literals or comments
- Verify there are no syntax errors

### Multiple Queries Execute When Only One Expected
**Problem**: Clicking run executes multiple queries

**Solution**:
- Ensure queries are properly separated by semicolons
- Check that semicolons in strings are properly quoted
- Make sure you're using "Run on active query" not "Run all"

### Semicolon in String Literal Causes Split
**Problem**: Query is split at semicolon inside a string

**Solution**: This should not happen with the current implementation. If it does:
- Ensure strings are properly quoted
- Report it as a bug

## Advanced Usage

### Dynamic SQL
```sql
-- Generate and display CREATE TABLE statement
SELECT 
    'CREATE TABLE ' || TABLENAME || ' (...);' AS ddl_statement
FROM _V_TABLE
WHERE SCHEMA = 'ADMIN'
LIMIT 5;

-- Execute the actual operation (separate query)
CREATE TABLE my_new_table (
    id INTEGER,
    name VARCHAR(100)
);
```

### Multi-Statement Transactions
```sql
-- Start transaction
BEGIN;

-- Statement 1
INSERT INTO table1 VALUES (1, 'value1');

-- Statement 2
INSERT INTO table2 VALUES (2, 'value2');

-- Commit
COMMIT;
```

Note: Execute all statements together for transactional consistency.

## See Also
- [SQLTools Documentation](https://vscode-sqltools.mteixeira.dev/)
- [Netezza SQL Reference](https://www.ibm.com/docs/en/netezza)
