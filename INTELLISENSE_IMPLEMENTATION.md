# Dynamic IntelliSense Implementation for SQLTools Netezza Driver

## Overview
This document describes the dynamic IntelliSense (auto-completion) implementation for the SQLTools Netezza driver. The implementation provides intelligent code completion for SQL keywords, functions, data types, schemas, tables, views, columns, and functions/procedures.

## Features Implemented

### 1. **Static Completions** (`getStaticCompletions`)
Provides completion suggestions for:
- **Keywords**: SQL keywords including Netezza-specific ones (DISTRIBUTE, ORGANIZE, ZONE, GROOM, GENERATE_STATISTICS, etc.) and standard SQL keywords
- **Functions**: Built-in SQL functions including:
  - Netezza-specific functions (REGEXP_EXTRACT, REGEXP_LIKE, REGEXP_REPLACE, TO_CHAR, TO_DATE, etc.)
  - Aggregate functions (COUNT, SUM, AVG, MIN, MAX, STDDEV, VARIANCE)
  - String functions (CONCAT, SUBSTR, LENGTH, TRIM, UPPER, LOWER, etc.)
  - Date/Time functions (CURRENT_DATE, EXTRACT, DATE_PART, DATE_TRUNC, etc.)
  - Numeric functions (ABS, CEIL, FLOOR, ROUND, SQRT, etc.)
  - Conditional functions (COALESCE, NULLIF, GREATEST, LEAST)
  - Window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.)
- **Data Types**: All Netezza data types (BYTEINT, SMALLINT, INTEGER, BIGINT, VARCHAR, DATE, TIMESTAMP, BOOLEAN, etc.)
- **Schemas**: Dynamically loaded list of all schemas in the database

### 2. **Dynamic Search** (`searchItems`)
Provides search capabilities for database objects:

#### Schema Search
```sql
SELECT SCHEMA AS label, SCHEMA AS schema
FROM _V_SCHEMA
WHERE SCHEMA LIKE '%{search}%'
ORDER BY SCHEMA
```

#### Table Search
```sql
SELECT TABLENAME AS label, 
       SCHEMA AS schema,
       TABLENAME AS table_name
FROM _V_TABLE
WHERE TABLENAME LIKE '%{search}%'
AND SCHEMA = '{schema}' -- optional filter
ORDER BY TABLENAME
```

#### View Search
```sql
SELECT VIEWNAME AS label,
       SCHEMA AS schema,
       VIEWNAME AS view_name
FROM _V_VIEW
WHERE VIEWNAME LIKE '%{search}%'
AND SCHEMA = '{schema}' -- optional filter
ORDER BY VIEWNAME
```

#### Column Search
```sql
SELECT ATTNAME AS label,
       SCHEMA AS schema,
       NAME AS table_name,
       FORMAT_TYPE AS data_type,
       ATTNOTNULL AS is_nullable
FROM _V_RELATION_COLUMN
WHERE ATTNAME LIKE '%{search}%'
AND SCHEMA = '{schema}' -- optional filter
AND NAME = '{table}' -- optional filter
ORDER BY ATTNUM
```

#### Function/Procedure Search
```sql
SELECT FUNCTION AS label,
       SCHEMA AS schema
FROM _V_FUNCTION
WHERE FUNCTION LIKE '%{search}%'
ORDER BY FUNCTION
```

## Implementation Details

### Cache Management
- **Completions Cache**: Static completions are cached on first load to improve performance
- **Cache Invalidation**: Cache is cleared when a new connection is opened to reflect database changes
- **Lazy Loading**: Completions are loaded only when first requested

### Helper Method
```typescript
private async executeCompletionQuery(query: string): Promise<any[]>
```
Executes queries for loading completion data with proper error handling.

### Connection Lifecycle
1. When `open()` is called, the completions cache is cleared
2. First call to `getStaticCompletions()` loads and caches:
   - All static keywords, functions, and data types
   - All schemas from the database
   - Common system functions
3. Cache is reused for subsequent requests until connection changes

## Usage in SQLTools

SQLTools will automatically call these methods to provide IntelliSense:

1. **On Connection**: `getStaticCompletions()` is called to load keywords and functions
2. **While Typing**: `searchItems()` is called to find matching database objects
3. **Context-Aware**: SQLTools uses the results to provide smart completions based on SQL context

## Benefits

- **Performance**: Caching strategy minimizes database queries
- **User Experience**: Real-time suggestions for all database objects
- **Accuracy**: Dynamic loading ensures completions reflect current database structure
- **Comprehensive**: Covers keywords, functions, schemas, tables, views, columns, and functions

## Netezza-Specific System Views Used

The implementation uses Netezza's system views for metadata:
- `_V_SCHEMA`: List of all schemas
- `_V_TABLE`: Table metadata
- `_V_VIEW`: View metadata
- `_V_RELATION_COLUMN`: Column metadata for tables and views
- `_V_FUNCTION`: User-defined functions and procedures

## Future Enhancements

Possible improvements for future versions:
1. Context-aware completions (e.g., suggest columns from tables in FROM clause)
2. Support for stored procedures with parameter hints
3. Table and column aliases support
4. JOIN condition suggestions
5. Snippet-based completions for common SQL patterns
