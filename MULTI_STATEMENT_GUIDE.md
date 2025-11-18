# Multi-Statement Query Execution - Quick Guide

## What Changed?

Previously, the "Run on active query" button would only work at the top of the file. Now it works on **every query** in your file!

## How to Use

### Before (Old Behavior)
```
File: queries.sql
┌─────────────────────────────────────────┐
│ [Run on active query] ← Only here!      │
│                                         │
│ SELECT * FROM table1;                   │
│                                         │
│ SELECT * FROM table2;                   │
│                                         │
│ SELECT * FROM table3;                   │
└─────────────────────────────────────────┘
```
You had to manually select each query to run it individually.

### After (New Behavior)
```
File: queries.sql
┌─────────────────────────────────────────┐
│ -- Query 1                              │
│ SELECT * FROM table1; ← [Run this!]     │
│                                         │
│ -- Query 2                              │
│ SELECT * FROM table2; ← [Run this!]     │
│                                         │
│ -- Query 3                              │
│ SELECT * FROM table3; ← [Run this!]     │
└─────────────────────────────────────────┘
```
Place your cursor **anywhere** in a query and run it!

## Step-by-Step Example

### Step 1: Create a file with multiple queries
```sql
-- Get database info
SELECT CURRENT_CATALOG, CURRENT_USER;

-- Count tables
SELECT COUNT(*) FROM _V_TABLE;

-- List schemas
SELECT SCHEMA FROM _V_SCHEMA;
```

### Step 2: Execute a specific query
1. Click anywhere on line 2 (or 1 or 3 of the first query)
2. Press `Ctrl+E Ctrl+E` or click "Run on active connection"
3. ✅ Only the first query executes!

### Step 3: Execute another query
1. Click anywhere in the second query (lines 5-6)
2. Press `Ctrl+E Ctrl+E`
3. ✅ Only the second query executes!

## Key Points

✅ **Each semicolon (`;`) marks the end of a query**
```sql
SELECT * FROM table1;  ← Query 1 ends here
SELECT * FROM table2;  ← Query 2 ends here
```

✅ **Semicolons inside strings don't count**
```sql
SELECT 'Text with ; semicolon' FROM table1;  ← Still one query!
```

✅ **Comments don't affect parsing**
```sql
-- This semicolon ; in comment is ignored
SELECT * FROM table1;  ← This semicolon counts!
```

✅ **Works with complex queries**
```sql
-- Multi-line query with subquery
SELECT t1.*, t2.name
FROM (
    SELECT id, value
    FROM source_table
    WHERE date > '2024-01-01'
) t1
JOIN lookup_table t2 ON t1.id = t2.id;  ← One query despite multiple lines
```

## Common Scenarios

### Scenario 1: Testing Different Filters
```sql
-- Test with limit 10
SELECT * FROM large_table LIMIT 10;

-- Test with limit 100
SELECT * FROM large_table LIMIT 100;

-- Test with limit 1000
SELECT * FROM large_table LIMIT 1000;
```
Run each query individually to see results!

### Scenario 2: Development and Testing
```sql
-- Check table structure
SELECT ATTNAME, FORMAT_TYPE 
FROM _V_RELATION_COLUMN 
WHERE NAME = 'MY_TABLE';

-- Test query on actual data
SELECT * FROM MY_TABLE LIMIT 5;

-- Full query with filters
SELECT col1, col2, col3
FROM MY_TABLE
WHERE col1 > 100
ORDER BY col2;
```
Develop step-by-step, testing each query!

### Scenario 3: Data Analysis Workflow
```sql
-- Step 1: Explore the data
SELECT COUNT(*), MIN(date), MAX(date) FROM sales;

-- Step 2: Check for nulls
SELECT 
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customers,
    SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS null_amounts
FROM sales;

-- Step 3: Final analysis
SELECT 
    customer_id,
    SUM(amount) AS total_sales,
    COUNT(*) AS order_count
FROM sales
WHERE customer_id IS NOT NULL
GROUP BY customer_id
ORDER BY total_sales DESC
LIMIT 10;
```
Execute each step as you analyze!

## Troubleshooting

### ❌ Problem: Two queries run together
```sql
SELECT * FROM table1  -- Missing semicolon!
SELECT * FROM table2;
```
**Solution**: Add semicolon after first query
```sql
SELECT * FROM table1;  -- Added semicolon
SELECT * FROM table2;
```

### ❌ Problem: Wrong query executes
Make sure your cursor is in the correct query:
```sql
SELECT * FROM table1;
          ↑ Cursor here = runs query 1

SELECT * FROM table2;
     ↑ Cursor here = runs query 2
```

### ❌ Problem: Query with semicolon in string splits
This should NOT happen! If it does:
```sql
-- This should work correctly:
SELECT 'Value with ; inside' AS text;
```
If it doesn't work, please report as a bug.

## Benefits

1. **Faster Development**: Test queries individually without selecting
2. **Better Organization**: Keep related queries in one file
3. **Easy Comparison**: Compare different query variations
4. **Cleaner Workflow**: No need to create multiple files
5. **Natural Flow**: Works like most SQL clients

## Learn More

- See `QUERY_PARSING.md` for detailed documentation
- See `examples/multiple-queries.sql` for more examples
- SQLTools docs: https://vscode-sqltools.mteixeira.dev/
