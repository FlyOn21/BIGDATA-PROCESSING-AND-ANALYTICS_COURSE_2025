-- Task 6

-- Step 1: Check current state of yellow_enriched table
SELECT 'Initial State' AS status, COUNT(*) AS record_count
FROM yellow_enriched;

-- Step 2: Create a Stream on yellow_enriched table
CREATE OR REPLACE STREAM yellow_enriched_stream
ON TABLE yellow_enriched
COMMENT = 'Stream to track changes in yellow_enriched table';

-- Step 3: Verify Stream creation
SHOW STREAMS LIKE 'yellow_enriched_stream';

-- Step 4: Check initial Stream state (should be empty)
SELECT 'Initial Stream State' AS info, COUNT(*) AS change_count
FROM yellow_enriched_stream;

-- Step 5: Method 1 - Add new records manually using INSERT
INSERT INTO yellow_enriched (
    VendorID,
    tpep_pickup_datetime_raw,
    tpep_dropoff_datetime_raw,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    dropoff_borough
) VALUES
    (1, '2024-01-15 10:30:00', '2024-01-15 10:45:00', 2, 3.5, 1, 'N',
     142, 161, 1, 15.50, 0.50, 0.50, 2.00, 0.00, 0.30, 18.80, 2.50, 1.25,
     'Lincoln Square East', 'Manhattan', 'Midtown Center', 'Manhattan'),
    (2, '2024-01-15 11:00:00', '2024-01-15 11:20:00', 1, 5.2, 1, 'N',
     237, 142, 2, 22.00, 0.50, 0.50, 0.00, 0.00, 0.30, 23.30, 2.50, 1.25,
     'Upper East Side South', 'Manhattan', 'Lincoln Square East', 'Manhattan'),
    (1, '2024-01-15 12:15:00', '2024-01-15 12:35:00', 3, 2.1, 1, 'N',
     161, 234, 1, 12.00, 0.50, 0.50, 1.50, 0.00, 0.30, 14.80, 2.50, 1.25,
     'Midtown Center', 'Manhattan', 'Union Sq', 'Manhattan');

-- Step 6: Check Stream after INSERT operations
SELECT 'After INSERT Operations' AS info, COUNT(*) AS change_count
FROM yellow_enriched_stream;

-- Step 7: View the INSERT changes in detail
SELECT
    METADATA$ACTION AS change_type,
    METADATA$ISUPDATE AS is_update,
    METADATA$ROW_ID AS row_id,
    VendorID,
    tpep_pickup_datetime_raw,
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    total_amount,
    trip_distance
FROM yellow_enriched_stream
ORDER BY METADATA$ROW_ID;

-- Step 8: Perform UPDATE operations to demonstrate UPDATE tracking
UPDATE yellow_enriched
SET tip_amount = tip_amount + 1.00,
    total_amount = total_amount + 1.00
WHERE pickup_zone_name = 'Lincoln Square East'
  AND tpep_pickup_datetime_raw = '2024-01-15 10:30:00';

UPDATE yellow_enriched
SET passenger_count = 2,
    fare_amount = fare_amount + 2.50,
    total_amount = total_amount + 2.50
WHERE pickup_zone_name = 'Upper East Side South'
  AND tpep_pickup_datetime_raw = '2024-01-15 11:00:00';

-- Step 9: Check Stream after UPDATE operations
SELECT 'After UPDATE Operations' AS info, COUNT(*) AS change_count
FROM yellow_enriched_stream;

-- Step 10: View all changes (INSERT and UPDATE) in the Stream
SELECT
    METADATA$ACTION AS change_type,
    METADATA$ISUPDATE AS is_update,
    METADATA$ROW_ID AS row_id,
    VendorID,
    pickup_zone_name,
    pickup_borough,
    total_amount,
    tip_amount,
    passenger_count,
    CASE
        WHEN METADATA$ACTION = 'INSERT' THEN 'New Record'
        WHEN METADATA$ACTION = 'DELETE' AND METADATA$ISUPDATE = TRUE THEN 'Before Update'
        WHEN METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = TRUE THEN 'After Update'
        ELSE 'Other'
    END AS change_description
FROM yellow_enriched_stream
ORDER BY METADATA$ROW_ID, METADATA$ACTION;

-- Step 11: Create target table for storing change log
CREATE OR REPLACE TABLE yellow_changes_log (
    log_id NUMBER AUTOINCREMENT,
    change_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    change_type VARCHAR(10),
    is_update BOOLEAN,
    row_id VARCHAR(50),
    vendor_id NUMBER(38,0),
    pickup_datetime_raw VARCHAR(50),
    dropoff_datetime_raw VARCHAR(50),
    passenger_count NUMBER(38,2),
    trip_distance NUMBER(38,6),
    pickup_zone_name VARCHAR(100),
    pickup_borough VARCHAR(50),
    dropoff_zone_name VARCHAR(100),
    dropoff_borough VARCHAR(50),
    fare_amount NUMBER(38,2),
    tip_amount NUMBER(38,2),
    total_amount NUMBER(38,2),
    original_record VARIANT,
    processed_by VARCHAR(100) DEFAULT CURRENT_USER(),
    processing_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Step 12: Insert Stream data into change log table
INSERT INTO yellow_changes_log (
    change_type,
    is_update,
    row_id,
    vendor_id,
    pickup_datetime_raw,
    dropoff_datetime_raw,
    passenger_count,
    trip_distance,
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    dropoff_borough,
    fare_amount,
    tip_amount,
    total_amount,
    original_record
)
SELECT
    METADATA$ACTION,
    METADATA$ISUPDATE,
    METADATA$ROW_ID,
    VendorID,
    tpep_pickup_datetime_raw,
    tpep_dropoff_datetime_raw,
    passenger_count,
    trip_distance,
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    dropoff_borough,
    fare_amount,
    tip_amount,
    total_amount,
    OBJECT_CONSTRUCT(
        'VendorID', VendorID,
        'pickup_zone', pickup_zone_name,
        'dropoff_zone', dropoff_zone_name,
        'trip_distance', trip_distance,
        'total_amount', total_amount
    ) AS original_record
FROM yellow_enriched_stream;

-- Step 13: Verify data was inserted into change log
SELECT 'Change Log Records' AS info, COUNT(*) AS log_count
FROM yellow_changes_log;

-- Step 14: View the change log details
SELECT
    log_id,
    change_timestamp,
    change_type,
    is_update,
    vendor_id,
    pickup_zone_name,
    pickup_borough,
    total_amount,
    tip_amount,
    processed_by
FROM yellow_changes_log
ORDER BY log_id;

-- Step 15: Check Stream after consuming the data (should be empty)
SELECT 'Stream After Consumption' AS info, COUNT(*) AS remaining_changes
FROM yellow_enriched_stream;

-- Step 16: Method 2 - Add more records using a different approach
INSERT INTO yellow_enriched (
    VendorID, tpep_pickup_datetime_raw, tpep_dropoff_datetime_raw,
    passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
    PULocationID, DOLocationID, payment_type, fare_amount, extra,
    mta_tax, tip_amount, tolls_amount, improvement_surcharge,
    total_amount, congestion_surcharge, airport_fee,
    pickup_zone_name, pickup_borough, dropoff_zone_name, dropoff_borough
)
SELECT
    2 AS VendorID,
    '2024-01-15 ' || LPAD(FLOOR(RANDOM() * 24), 2, '0') || ':' ||
    LPAD(FLOOR(RANDOM() * 60), 2, '0') || ':00' AS pickup_time,
    '2024-01-15 ' || LPAD(FLOOR(RANDOM() * 24), 2, '0') || ':' ||
    LPAD(FLOOR(RANDOM() * 60), 2, '0') || ':00' AS dropoff_time,
    FLOOR(RANDOM() * 4) + 1 AS passenger_count,
    ROUND(RANDOM() * 10 + 1, 2) AS trip_distance,
    1 AS RatecodeID,
    'N' AS store_and_fwd_flag,
    FLOOR(RANDOM() * 250) + 1 AS PULocationID,
    FLOOR(RANDOM() * 250) + 1 AS DOLocationID,
    1 AS payment_type,
    ROUND(RANDOM() * 30 + 10, 2) AS fare_amount,
    0.50 AS extra,
    0.50 AS mta_tax,
    ROUND(RANDOM() * 5, 2) AS tip_amount,
    0.00 AS tolls_amount,
    0.30 AS improvement_surcharge,
    ROUND(RANDOM() * 40 + 15, 2) AS total_amount,
    2.50 AS congestion_surcharge,
    1.25 AS airport_fee,
    'Generated Zone ' || ROW_NUMBER() OVER (ORDER BY RANDOM()) AS pickup_zone,
    'Manhattan' AS pickup_borough,
    'Generated Dest ' || ROW_NUMBER() OVER (ORDER BY RANDOM()) AS dropoff_zone,
    'Manhattan' AS dropoff_borough
FROM TABLE(GENERATOR(ROWCOUNT => 5));

-- Step 17: Process the new changes
SELECT 'New Changes in Stream' AS info, COUNT(*) AS new_change_count
FROM yellow_enriched_stream;

-- Step 18: Create a procedure to automatically process Stream changes
CREATE OR REPLACE PROCEDURE process_yellow_stream_changes()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  changes_count INTEGER;
BEGIN
  SELECT COUNT(*) INTO changes_count FROM yellow_enriched_stream;

  IF (changes_count > 0) THEN
    INSERT INTO yellow_changes_log (
        change_type, is_update, row_id, vendor_id,
        pickup_datetime_raw, dropoff_datetime_raw,
        passenger_count, trip_distance, pickup_zone_name,
        pickup_borough, dropoff_zone_name, dropoff_borough,
        fare_amount, tip_amount, total_amount, original_record
    )
    SELECT
        METADATA$ACTION, METADATA$ISUPDATE, METADATA$ROW_ID, VendorID,
        tpep_pickup_datetime_raw, tpep_dropoff_datetime_raw,
        passenger_count, trip_distance, pickup_zone_name,
        pickup_borough, dropoff_zone_name, dropoff_borough,
        fare_amount, tip_amount, total_amount,
        OBJECT_CONSTRUCT('change_batch', 'auto_processed') AS original_record
    FROM yellow_enriched_stream;

    RETURN 'Processed ' || changes_count || ' changes successfully';
  ELSE
    RETURN 'No changes to process';
  END IF;
END;
$$;

-- Step 19: Execute the procedure to process changes
CALL process_yellow_stream_changes();

-- Step 20: Final verification and summary
SELECT
    'Summary Report' AS report_section,
    'Total changes logged: ' || COUNT(*) AS details
FROM yellow_changes_log

UNION ALL

SELECT
    'Change Types',
    change_type || ': ' || COUNT(*) AS details
FROM yellow_changes_log
GROUP BY change_type

UNION ALL

SELECT
    'Stream Status',
    'Remaining changes: ' || COUNT(*) AS details
FROM yellow_enriched_stream;

-- Step 21: Advanced Stream analysis
WITH change_analysis AS (
    SELECT
        change_type,
        is_update,
        COUNT(*) AS change_count,
        AVG(total_amount) AS avg_amount,
        MIN(change_timestamp) AS first_change,
        MAX(change_timestamp) AS last_change
    FROM yellow_changes_log
    GROUP BY change_type, is_update
)
SELECT
    change_type,
    CASE WHEN is_update THEN 'Update Operation' ELSE 'Direct Operation' END AS operation_type,
    change_count,
    ROUND(avg_amount, 2) AS avg_total_amount,
    first_change,
    last_change
FROM change_analysis
ORDER BY change_type, is_update;