-- Debug Test File for Query Splitting
-- Place cursor on different queries and execute to test

-- Query 1
select current_date;

-- Query 2
SELECT current_timestamp;

-- Query 3
SELECT current_user;


--query 4
SELECT * 
FROM (
    SELECT current_user
) x
;

--query 5
SELECT current_date
union ALL
select current_date

;

--query 6
select * 
FROM 
    (SELECT current_date
    union ALL
    select current_date

)x
;


--query 7

-- Test 1: Success
SELECT current_date;

-- Test 2: Error (invalid syntax)  
SELECT * FORM invalid_table;

-- Test 3: Success (should still execute)
SELECT current_timestamp;