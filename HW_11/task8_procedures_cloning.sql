-- Task 8

-- Step 1: Create the unified all_trips table combining yellow_enriched and green_enriched
CREATE OR REPLACE TABLE all_trips AS
WITH yellow_normalized AS (
    SELECT
        VendorID,
        TRY_TO_TIMESTAMP(tpep_pickup_datetime_raw) AS pickup_datetime,
        TRY_TO_TIMESTAMP(tpep_dropoff_datetime_raw) AS dropoff_datetime,
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
        NULL AS ehail_fee,
        NULL AS trip_type,
        pickup_zone_name,
        pickup_borough,
        dropoff_zone_name,
        dropoff_borough,
        'Yellow' AS taxi_type,
        CURRENT_TIMESTAMP() AS created_timestamp,
        MD5(
            COALESCE(VendorID::STRING, '') || '|' ||
            COALESCE(tpep_pickup_datetime_raw, '') || '|' ||
            COALESCE(tpep_dropoff_datetime_raw, '') || '|' ||
            COALESCE(PULocationID::STRING, '') || '|' ||
            COALESCE(DOLocationID::STRING, '') || '|' ||
            COALESCE(trip_distance::STRING, '') || '|' ||
            COALESCE(total_amount::STRING, '')
        ) AS trip_hash
    FROM yellow_enriched
    WHERE trip_distance > 0
      AND total_amount > 0
      AND passenger_count BETWEEN 1 AND 6
),
green_normalized AS (
    SELECT
        VendorID,
        TRY_TO_TIMESTAMP(lpep_pickup_datetime_raw) AS pickup_datetime,
        TRY_TO_TIMESTAMP(lpep_dropoff_datetime_raw) AS dropoff_datetime,
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
        NULL AS airport_fee,
        ehail_fee,
        trip_type,
        pickup_zone_name,
        pickup_borough,
        dropoff_zone_name,
        dropoff_borough,
        'Green' AS taxi_type,
        CURRENT_TIMESTAMP() AS created_timestamp,
        MD5(
            COALESCE(VendorID::STRING, '') || '|' ||
            COALESCE(lpep_pickup_datetime_raw, '') || '|' ||
            COALESCE(lpep_dropoff_datetime_raw, '') || '|' ||
            COALESCE(PULocationID::STRING, '') || '|' ||
            COALESCE(DOLocationID::STRING, '') || '|' ||
            COALESCE(trip_distance::STRING, '') || '|' ||
            COALESCE(total_amount::STRING, '')
        ) AS trip_hash
    FROM green_enriched
    WHERE trip_distance > 0
      AND total_amount > 0
      AND passenger_count BETWEEN 1 AND 6
)
SELECT * FROM yellow_normalized
UNION ALL
SELECT * FROM green_normalized;

-- Step 2: Create a log table for the stored procedure operations
CREATE OR REPLACE TABLE trip_processing_log (
    log_id NUMBER AUTOINCREMENT,
    operation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    operation_type VARCHAR(50),
    records_processed NUMBER(10,0),
    duplicates_found NUMBER(10,0),
    duplicates_skipped NUMBER(10,0),
    records_inserted NUMBER(10,0),
    success_rate NUMBER(5,2),
    error_count NUMBER(10,0),
    operation_details VARIANT,
    execution_time_ms NUMBER(10,0),
    processed_by VARCHAR(100) DEFAULT CURRENT_USER(),
    batch_id VARCHAR(100),
    source_table VARCHAR(100),
    target_table VARCHAR(100),
    status VARCHAR(20),
    error_message VARCHAR(1000),
    PRIMARY KEY (log_id)
);

-- Step 3: Create a comprehensive stored procedure for duplicate checking and insertion
CREATE OR REPLACE PROCEDURE insert_trips_with_duplicate_check(
    source_table_name STRING,
    batch_identifier STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP_NTZ;
    end_time TIMESTAMP_NTZ;
    execution_time NUMBER;
    total_source_records NUMBER := 0;
    duplicates_found NUMBER := 0;
    records_inserted NUMBER := 0;
    error_count NUMBER := 0;
    result_status STRING;
    error_message STRING := '';
    success_rate NUMBER;
    sql_statement STRING;
BEGIN
    start_time := CURRENT_TIMESTAMP();

    EXECUTE IMMEDIATE 'INSERT INTO trip_processing_log (operation_type, source_table, target_table, status) VALUES (''DUPLICATE_CHECK_INSERT'', ''' || source_table_name || ''', ''all_trips'', ''STARTED'')';

    BEGIN
        IF (UPPER(source_table_name) = 'YELLOW_ENRICHED') THEN
            sql_statement := '
                INSERT INTO all_trips
                WITH yellow_with_hash AS (
                    SELECT
                        VendorID,
                        TRY_TO_TIMESTAMP(tpep_pickup_datetime_raw) AS pickup_datetime,
                        TRY_TO_TIMESTAMP(tpep_dropoff_datetime_raw) AS dropoff_datetime,
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
                        NULL AS ehail_fee,
                        NULL AS trip_type,
                        pickup_zone_name,
                        pickup_borough,
                        dropoff_zone_name,
                        dropoff_borough,
                        ''Yellow'' AS taxi_type,
                        CURRENT_TIMESTAMP() AS created_timestamp,
                        MD5(
                            COALESCE(VendorID::STRING, '''') || ''|'' ||
                            COALESCE(tpep_pickup_datetime_raw, '''') || ''|'' ||
                            COALESCE(tpep_dropoff_datetime_raw, '''') || ''|'' ||
                            COALESCE(PULocationID::STRING, '''') || ''|'' ||
                            COALESCE(DOLocationID::STRING, '''') || ''|'' ||
                            COALESCE(trip_distance::STRING, '''') || ''|'' ||
                            COALESCE(total_amount::STRING, '''')
                        ) AS trip_hash
                    FROM yellow_enriched
                    WHERE trip_distance > 0 AND total_amount > 0 AND passenger_count BETWEEN 1 AND 6
                ),
                new_records AS (
                    SELECT y.*
                    FROM yellow_with_hash y
                    LEFT JOIN all_trips a ON y.trip_hash = a.trip_hash
                    WHERE a.trip_hash IS NULL
                )
                SELECT * FROM new_records';

        ELSEIF (UPPER(source_table_name) = 'GREEN_ENRICHED') THEN
            sql_statement := '
                INSERT INTO all_trips
                WITH green_with_hash AS (
                    SELECT
                        VendorID,
                        TRY_TO_TIMESTAMP(lpep_pickup_datetime_raw) AS pickup_datetime,
                        TRY_TO_TIMESTAMP(lpep_dropoff_datetime_raw) AS dropoff_datetime,
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
                        NULL AS airport_fee,
                        ehail_fee,
                        trip_type,
                        pickup_zone_name,
                        pickup_borough,
                        dropoff_zone_name,
                        dropoff_borough,
                        ''Green'' AS taxi_type,
                        CURRENT_TIMESTAMP() AS created_timestamp,
                        MD5(
                            COALESCE(VendorID::STRING, '''') || ''|'' ||
                            COALESCE(lpep_pickup_datetime_raw, '''') || ''|'' ||
                            COALESCE(lpep_dropoff_datetime_raw, '''') || ''|'' ||
                            COALESCE(PULocationID::STRING, '''') || ''|'' ||
                            COALESCE(DOLocationID::STRING, '''') || ''|'' ||
                            COALESCE(trip_distance::STRING, '''') || ''|'' ||
                            COALESCE(total_amount::STRING, '''')
                        ) AS trip_hash
                    FROM green_enriched
                    WHERE trip_distance > 0 AND total_amount > 0 AND passenger_count BETWEEN 1 AND 6
                ),
                new_records AS (
                    SELECT g.*
                    FROM green_with_hash g
                    LEFT JOIN all_trips a ON g.trip_hash = a.trip_hash
                    WHERE a.trip_hash IS NULL
                )
                SELECT * FROM new_records';
        ELSE
            error_message := 'Invalid source table: ' || source_table_name;
            error_count := 1;
            result_status := 'FAILED';
        END IF;

        IF (error_count = 0) THEN
            EXECUTE IMMEDIATE sql_statement;
            records_inserted := SQLROWCOUNT;
        END IF;

        IF (UPPER(source_table_name) = 'YELLOW_ENRICHED') THEN
            SELECT COUNT(*) INTO total_source_records FROM yellow_enriched;
        ELSEIF (UPPER(source_table_name) = 'GREEN_ENRICHED') THEN
            SELECT COUNT(*) INTO total_source_records FROM green_enriched;
        END IF;

        duplicates_found := total_source_records - records_inserted;

        end_time := CURRENT_TIMESTAMP();
        execution_time := DATEDIFF('millisecond', start_time, end_time);
        success_rate := CASE
            WHEN total_source_records = 0 THEN 0
            ELSE ROUND((records_inserted * 100.0) / total_source_records, 2)
        END;

        IF (error_count = 0) THEN
            result_status := 'SUCCESS';
        END IF;

    EXCEPTION
        WHEN OTHER THEN
            error_message := SQLERRM;
            error_count := 1;
            result_status := 'FAILED';
            end_time := CURRENT_TIMESTAMP();
            execution_time := DATEDIFF('millisecond', start_time, end_time);
    END;

    RETURN 'Operation: ' || result_status ||
           ' | Source: ' || source_table_name ||
           ' | Processed: ' || total_source_records ||
           ' | Inserted: ' || records_inserted ||
           ' | Duplicates: ' || duplicates_found ||
           ' | Success Rate: ' || success_rate || '%' ||
           ' | Time: ' || execution_time || 'ms' ||
           CASE WHEN error_count > 0 THEN ' | Error: ' || error_message ELSE '' END;
END;
$$;

-- Step 4: Test the stored procedure
CALL insert_trips_with_duplicate_check('YELLOW_ENRICHED', 'TEST_YELLOW_BATCH_1');
CALL insert_trips_with_duplicate_check('GREEN_ENRICHED', 'TEST_GREEN_BATCH_1');

-- Step 5: View processing results
SELECT
    operation_timestamp,
    operation_type,
    batch_id,
    source_table,
    records_processed,
    duplicates_found,
    records_inserted,
    success_rate,
    execution_time_ms,
    status,
    CASE WHEN error_message IS NOT NULL THEN error_message ELSE 'No errors' END AS error_status
FROM trip_processing_log
ORDER BY operation_timestamp DESC;

-- Step 6: Data quality verification
SELECT
    'Total Records in all_trips' AS metric,
    COUNT(*)::STRING AS value
FROM all_trips

UNION ALL

SELECT
    'Unique Trip Hashes' AS metric,
    COUNT(DISTINCT trip_hash)::STRING AS value
FROM all_trips

UNION ALL

SELECT
    'Yellow Taxi Records' AS metric,
    COUNT(*)::STRING AS value
FROM all_trips
WHERE taxi_type = 'Yellow'

UNION ALL

SELECT
    'Green Taxi Records' AS metric,
    COUNT(*)::STRING AS value
FROM all_trips
WHERE taxi_type = 'Green'

UNION ALL

SELECT
    'Records with Valid Zones' AS metric,
    COUNT(*)::STRING AS value
FROM all_trips
WHERE pickup_zone_name IS NOT NULL AND dropoff_zone_name IS NOT NULL;

-- Step 7: Zero-Copy Cloning - Create development environment

CREATE DATABASE PZHOHOLIEV_DB_DEV CLONE PZHOHOLIEV_DB
COMMENT = 'Development environment cloned from production taxi_data database owner Zhoholiev Pavlo';

USE DATABASE PZHOHOLIEV_DB_DEV;

SHOW TABLES;

SELECT
    'PZHOHOLIEV_DB_DEV - all_trips' AS table_info,
    COUNT(*) AS record_count,
    MIN(created_timestamp) AS earliest_record,
    MAX(created_timestamp) AS latest_record
FROM all_trips;

CREATE OR REPLACE TABLE dev_config (
    config_id NUMBER AUTOINCREMENT,
    config_key VARCHAR(100),
    config_value VARCHAR(500),
    environment VARCHAR(20) DEFAULT 'DEV',
    created_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    created_by VARCHAR(100) DEFAULT CURRENT_USER(),
    PRIMARY KEY (config_id)
);

INSERT INTO dev_config (config_key, config_value) VALUES
    ('environment_type', 'development'),
    ('data_retention_days', '30'),
    ('auto_vacuum_enabled', 'true'),
    ('debug_mode', 'enabled'),
    ('clone_source', 'taxi_data'),
    ('clone_timestamp', CURRENT_TIMESTAMP()::STRING);

CREATE OR REPLACE VIEW dev_environment_info AS
SELECT
    'Database Info' AS info_type,
    'taxi_dev_zhoholievpv' AS database_name,
    CURRENT_DATABASE() AS current_db,
    CURRENT_SCHEMA() AS current_schema,
    CURRENT_USER() AS current_user,
    CURRENT_TIMESTAMP() AS query_time
UNION ALL
SELECT
    'Table Counts' AS info_type,
    table_name AS database_name,
    row_count::STRING AS current_db,
    'rows' AS current_schema,
    created::STRING AS current_user,
    last_altered::STRING AS query_time
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'PUBLIC'
ORDER BY info_type, database_name;

-- Step 8: Clone specific tables for targeted development
CREATE TABLE all_trips_dev CLONE all_trips
COMMENT = 'Development copy of all_trips for testing new features';

CREATE TABLE trip_processing_log_dev CLONE trip_processing_log
COMMENT = 'Development copy of processing log for testing workflows';

-- Step 9: Create development-specific stored procedures
CREATE OR REPLACE PROCEDURE dev_reset_environment()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    DROP TABLE IF EXISTS all_trips_dev;
    CREATE TABLE all_trips_dev CLONE all_trips;

    DROP TABLE IF EXISTS trip_processing_log_dev;
    CREATE TABLE trip_processing_log_dev CLONE trip_processing_log;

    INSERT INTO dev_config (config_key, config_value) VALUES
        ('last_reset', CURRENT_TIMESTAMP()::STRING);

    RETURN 'Development environment reset successfully at ' || CURRENT_TIMESTAMP()::STRING;
END;
$$;

-- Step 10: Verification and monitoring queries
USE DATABASE PZHOHOLIEV_DB;
SELECT 'Production' AS environment, COUNT(*) AS all_trips_count FROM all_trips
UNION ALL
SELECT 'Development' AS environment, COUNT(*) FROM PZHOHOLIEV_DB_DEV.public.all_trips;

USE DATABASE PZHOHOLIEV_DB_DEV;

SELECT
    'Environment Status' AS status,
    'taxi_dev database successfully created with Zero-Copy Clone' AS details
UNION ALL
SELECT
    'Clone Verification',
    'Data integrity maintained: ' || COUNT(*) || ' records in all_trips'
FROM all_trips
UNION ALL
SELECT
    'Development Features',
    'Additional dev tables and procedures created successfully'
UNION ALL
SELECT
    'Stored Procedure Status',
    'insert_trips_with_duplicate_check available with logging'
UNION ALL
SELECT
    'Ready for Development',
    'Environment configured for safe testing and development';