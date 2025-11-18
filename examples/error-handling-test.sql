-- Error Handling Test File
-- This file demonstrates how errors are now displayed in the SQL console

-- Test 1: Syntax error
SELECT * FORM invalid_table;

-- Test 2: Non-existent table
SELECT * FROM this_table_does_not_exist;

-- Test 3: Invalid column
SELECT invalid_column FROM _v_dual;

-- Test 4: Division by zero
SELECT 1 / 0;

-- Test 5: Multiple queries with some failing
SELECT current_date;
SELECT * FROM non_existent_table;
SELECT current_timestamp;
