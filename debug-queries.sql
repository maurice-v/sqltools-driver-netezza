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
)