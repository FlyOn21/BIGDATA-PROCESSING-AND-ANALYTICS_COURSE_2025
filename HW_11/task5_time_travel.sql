-- Task 5

-- Step 1: Check current record count in green_enriched table
SELECT 'Before Deletion' AS status, COUNT(*) AS record_count
FROM green_enriched;

-- Step 2: Store the current timestamp for Time Travel reference
SELECT CURRENT_TIMESTAMP() AS deletion_timestamp;

-- Step 3: Delete some records from green_enriched table
DELETE FROM green_enriched
WHERE pickup_borough = 'Manhattan'
  AND trip_distance > 20;

-- Step 4: Check record count after deletion
SELECT 'After Deletion' AS status, COUNT(*) AS record_count
FROM green_enriched;

-- Step 5: Show the difference
WITH before_count AS (
    SELECT COUNT(*) AS before_records
    FROM green_enriched AT(OFFSET => -300)
),
after_count AS (
    SELECT COUNT(*) AS after_records
    FROM green_enriched
)
SELECT
    before_records,
    after_records,
    (before_records - after_records) AS deleted_records
FROM before_count, after_count;

-- Step 6: Use Time Travel to check the old version of the table
SELECT COUNT(*) AS records_before_deletion
FROM green_enriched AT(TIMESTAMP => '2025-08-16 11:30:00'::TIMESTAMP);

SELECT 'Records 30 minutes ago' AS status, COUNT(*) AS record_count
FROM green_enriched AT(OFFSET => -3600);

-- Step 7: Preview the deleted records using Time Travel
SELECT
    'Deleted Records Preview' AS info,
    COUNT(*) AS records_to_restore
FROM (
    SELECT * FROM green_enriched AT(OFFSET => -3600)
    EXCEPT
    SELECT * FROM green_enriched
) deleted_records;

-- Step 8: Show sample of deleted records
SELECT
    VendorID,
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    trip_distance,
    total_amount
FROM (
    SELECT * FROM green_enriched AT(OFFSET => -3600)
    EXCEPT
    SELECT * FROM green_enriched
) deleted_records
LIMIT 10;

-- Step 9: Method 1 - Restore deleted records to the same table
INSERT INTO green_enriched
SELECT * FROM (
    SELECT * FROM green_enriched AT(OFFSET => -3600)
    EXCEPT
    SELECT * FROM green_enriched
);

-- Step 10: Method 2 - Create a new table with restored records
CREATE OR REPLACE TABLE green_enriched_restored AS
SELECT * FROM green_enriched AT(OFFSET => -3600);

-- Step 11: Verify restoration
SELECT 'After Restoration' AS status, COUNT(*) AS record_count
FROM green_enriched;

SELECT 'Restored Table' AS status, COUNT(*) AS record_count
FROM green_enriched_restored;

-- Step 13: Advanced Time Travel - Compare data between time points
WITH current_data AS (
    SELECT pickup_borough, COUNT(*) AS current_count
    FROM green_enriched
    GROUP BY pickup_borough
),
historical_data AS (
    SELECT pickup_borough, COUNT(*) AS historical_count
    FROM green_enriched AT(OFFSET => -300)
    GROUP BY pickup_borough
)
SELECT
    COALESCE(c.pickup_borough, h.pickup_borough) AS borough,
    COALESCE(c.current_count, 0) AS current_count,
    COALESCE(h.historical_count, 0) AS historical_count,
    COALESCE(h.historical_count, 0) - COALESCE(c.current_count, 0) AS records_lost
FROM current_data c
FULL OUTER JOIN historical_data h ON c.pickup_borough = h.pickup_borough
ORDER BY records_lost DESC;

-- Step 14: Check Time Travel retention period for the table
SHOW TABLES LIKE 'green_enriched';

-- Step 15: Time Travel best practices queries

SELECT
    COUNT(*) AS record_count
FROM green_enriched_restored;

-- Step 16: Monitor data changes over time
SELECT
    'Current' AS time_point,
    COUNT(*) AS record_count,
    AVG(trip_distance) AS avg_distance,
    SUM(total_amount) AS total_revenue
FROM green_enriched

UNION ALL

SELECT
    '5 minutes ago' AS time_point,
    COUNT(*) AS record_count,
    AVG(trip_distance) AS avg_distance,
    SUM(total_amount) AS total_revenue
FROM green_enriched AT(OFFSET => -300)

UNION ALL

SELECT
    '1 hour ago' AS time_point,
    COUNT(*) AS record_count,
    AVG(trip_distance) AS avg_distance,
    SUM(total_amount) AS total_revenue
FROM green_enriched AT(OFFSET => -3600);