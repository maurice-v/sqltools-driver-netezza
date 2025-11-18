-- Test multiple SQL statements
-- Each statement should be executable separately

-- Query 1: Get columns from a table
SELECT ATTNAME 
FROM _V_RELATION_COLUMN 
WHERE NAME = 'PIPO' AND SCHEMA = 'ADMIN'
ORDER BY ATTNUM 
LIMIT 5;

-- Query 2: Get current database info
SELECT CURRENT_CATALOG, CURRENT_USER;

-- Query 3: Get table count
SELECT COUNT(*) AS table_count 
FROM _V_TABLE;

-- Query 4: List schemas
SELECT SCHEMA 
FROM _V_SCHEMA 
WHERE SCHEMA NOT IN ('SYSTEM', 'DEFINITION_SCHEMA')
ORDER BY SCHEMA;