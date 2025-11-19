-- Connection Pooling Test Queries
-- Use these queries to test the connection pooling implementation

-- Query 1: Simple SELECT to verify connection works
SELECT 1 AS test_value;

-- Query 2: Check current database
SELECT CURRENT_DATABASE;

-- Query 3: Quick metadata query
SELECT COUNT(*) FROM _V_TABLE WHERE SCHEMA = 'SYSTEM';

-- Query 4: Another simple query (tests connection reuse)
SELECT CURRENT_USER;

-- Query 5: Test concurrent execution
-- Open this file in multiple tabs and run queries simultaneously
SELECT CURRENT_TIMESTAMP;

/*
Expected Behavior:
1. First query should establish initial pool connection
2. Subsequent queries should reuse connections from pool
3. Check console logs for pool initialization messages:
   - "Connection pool opened"
   - "Pool configuration: min=X, max=Y, idleTimeout=Zms"
4. Monitor performance - should be faster than previous version
5. No connection errors between queries
6. Pool automatically manages connection lifecycle

To test concurrency:
1. Open this file in 2-3 editor tabs
2. Run queries simultaneously (Ctrl+Enter)
3. All should complete successfully without connection conflicts
*/
