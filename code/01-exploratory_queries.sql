-- OPEN QUERY --

-- Instructing the use of the dataAnalistPaymentsV3 database, as indicated. Also, I am using the SHOW TABLES command to check what tables are there inside the DB, which only retrieves 1:
USE dataAnalistPaymentsV3;
SHOW TABLES;

-- Checking how the table looks like: 
SELECT *
FROM transactions
LIMIT 10;

-- Checking all the columns to find null values, for example, I’m starting with the Date column which looks alright. I checked the rest and none presented any NULL values:
SELECT COUNT(*)
FROM transactions as t
WHERE t.trans_datetime IS NULL;

-- Now, I’d like to check duplicated transactions through their ID. 
-- First, I tried with selecting and grouping the IDs column so in case there are 2 or more equal rows for any ID, I’d spot those duplicates using a HAVING COUNT > 1. 
-- However, due to the big size of the table, I had a timeout error: Error Code: 2013. Lost connection to MySQL server during query.
SELECT id_transaction, COUNT(*)
FROM transactions as t
GROUP BY id_transaction
HAVING COUNT(*) > 1;

-- Thus, I investigated other alternatives and found that just counting all the transactions and also another column with a COUNT DISTINCT instruction to perform a subtraction afterwards might be lighter to handle.
-- This makes total sense as it’s just 2 general counts and then just deducting one single number from another, avoiding the grouping and its filtering at the same time. No duplicates spotted:
SELECT COUNT(*) as all_rows,
  COUNT (DISTINCT id_transaction) as unique_ids,
  COUNT(*) - COUNT(DISTINCT id_transaction) AS duplicated
FROM dataAnalistPaymentsV3.transactions as t;

-- No strange dates:
SELECT *
FROM transactions as t
WHERE trans_datetime < '2000-01-01'
  OR trans_datetime > NOW();

-- END --




