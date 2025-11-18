-- Example: Multiple SQL Statements in One File
-- Each statement is separated by a semicolon
-- You can now place your cursor on any statement and execute just that one

-- Query 1: Check current connection
SELECT CURRENT_CATALOG AS database, 
       CURRENT_USER AS username,
       VERSION() AS version;

-- Query 2: List all schemas
SELECT SCHEMA, 
       COUNT(*) AS object_count
FROM (
    SELECT SCHEMA FROM _V_TABLE
    UNION ALL
    SELECT SCHEMA FROM _V_VIEW
) AS objects
GROUP BY SCHEMA
ORDER BY SCHEMA;

-- Query 3: Get table statistics
SELECT 
    SCHEMA,
    COUNT(*) AS table_count,
    SUM(CASE WHEN TABLETYPE = 'TABLE' THEN 1 ELSE 0 END) AS base_tables,
    SUM(CASE WHEN TABLETYPE = 'TEMP' THEN 1 ELSE 0 END) AS temp_tables
FROM _V_TABLE
GROUP BY SCHEMA
ORDER BY table_count DESC
LIMIT 10;

-- Query 4: Simple test query
SELECT 1 AS test_number, 
       'Hello from Netezza' AS message;

-- Query 5: Date and time functions
SELECT 
    CURRENT_DATE AS today,
    CURRENT_TIME AS now_time,
    CURRENT_TIMESTAMP AS now_timestamp;

/* Block comment test
   This query is also separate */
SELECT 'Block comment handled' AS test;

-- Query with string containing semicolon
SELECT 'This ; is not a separator' AS text_with_semicolon;
