-- Task 7

-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
-- GRANT CREATE TASK ON SCHEMA public TO ROLE SYSADMIN;

-- Step 1: Create the zone_hourly_stats table for aggregated statistics
CREATE OR REPLACE TABLE zone_hourly_stats (
    stats_id NUMBER AUTOINCREMENT,
    calculation_hour TIMESTAMP_NTZ,
    pickup_zone_name VARCHAR(100),
    pickup_borough VARCHAR(50),
    taxi_type VARCHAR(10),

    total_trips NUMBER(10,0),
    avg_trip_distance NUMBER(10,4),
    min_trip_distance NUMBER(10,4),
    max_trip_distance NUMBER(10,4),

    avg_total_amount NUMBER(10,2),
    min_total_amount NUMBER(10,2),
    max_total_amount NUMBER(10,2),
    total_revenue NUMBER(12,2),
    avg_tip_amount NUMBER(10,2),

    avg_passenger_count NUMBER(5,2),
    total_passengers NUMBER(10,0),

    avg_trip_duration_minutes NUMBER(8,2),
    peak_hour_indicator BOOLEAN,

    calculation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    calculated_by VARCHAR(100) DEFAULT CURRENT_USER(),
    data_quality_score NUMBER(3,2),

    PRIMARY KEY (stats_id)
);

-- Step 2: Create a procedure for stream processing (to be used by Task)
CREATE OR REPLACE PROCEDURE process_stream_changes_batch()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  changes_count INTEGER;
  error_message STRING;
BEGIN
  SELECT COUNT(*) INTO changes_count FROM yellow_enriched_stream;

  IF (changes_count > 0) THEN
    BEGIN
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
              'batch_id', 'task_' || CURRENT_TIMESTAMP()::STRING,
              'processing_mode', 'automated_task',
              'stream_offset', METADATA$ROW_ID,
              'change_action', METADATA$ACTION
          ) AS original_record
      FROM yellow_enriched_stream;

      RETURN 'SUCCESS: Processed ' || changes_count || ' changes at ' || CURRENT_TIMESTAMP()::STRING;

    EXCEPTION
      WHEN OTHER THEN
        error_message := SQLERRM;
        RETURN 'ERROR: Failed to process stream changes - ' || error_message;
    END;
  ELSE
    RETURN 'INFO: No changes to process at ' || CURRENT_TIMESTAMP()::STRING;
  END IF;
END;
$$;

-- Step 3: Create a procedure for hourly statistics calculation
CREATE OR REPLACE PROCEDURE calculate_hourly_zone_stats()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  current_hour TIMESTAMP_NTZ;
  stats_count INTEGER;
  error_message STRING;
BEGIN
  current_hour := DATE_TRUNC('HOUR', CURRENT_TIMESTAMP());

  BEGIN
    DELETE FROM zone_hourly_stats WHERE calculation_hour = current_hour;
    INSERT INTO zone_hourly_stats (
        calculation_hour,
        pickup_zone_name,
        pickup_borough,
        taxi_type,
        total_trips,
        avg_trip_distance,
        min_trip_distance,
        max_trip_distance,
        avg_total_amount,
        min_total_amount,
        max_total_amount,
        total_revenue,
        avg_tip_amount,
        avg_passenger_count,
        total_passengers,
        avg_trip_duration_minutes,
        peak_hour_indicator,
        data_quality_score
    )
    WITH hourly_data AS (
        SELECT
            pickup_zone_name,
            pickup_borough,
            'Yellow' AS taxi_type,
            pickup_datetime,
            trip_distance,
            total_amount,
            tip_amount,
            passenger_count,
            DATEDIFF('minute', pickup_datetime, dropoff_datetime) AS trip_duration_minutes
        FROM taxi_unified_transformed
        WHERE DATE_TRUNC('HOUR', pickup_datetime) = current_hour
          AND pickup_zone_name IS NOT NULL
          AND pickup_borough IS NOT NULL
    ),
    zone_stats AS (
        SELECT
            pickup_zone_name,
            pickup_borough,
            taxi_type,
            COUNT(*) AS total_trips,
            AVG(trip_distance) AS avg_trip_distance,
            MIN(trip_distance) AS min_trip_distance,
            MAX(trip_distance) AS max_trip_distance,
            AVG(total_amount) AS avg_total_amount,
            MIN(total_amount) AS min_total_amount,
            MAX(total_amount) AS max_total_amount,
            SUM(total_amount) AS total_revenue,
            AVG(tip_amount) AS avg_tip_amount,
            AVG(passenger_count) AS avg_passenger_count,
            SUM(passenger_count) AS total_passengers,
            AVG(trip_duration_minutes) AS avg_trip_duration_minutes,
            CASE
                WHEN EXTRACT(HOUR FROM current_hour) BETWEEN 6 AND 9
                  OR EXTRACT(HOUR FROM current_hour) BETWEEN 17 AND 20
                THEN TRUE
                ELSE FALSE
            END AS peak_hour_indicator,
            ROUND(
                (COUNT(CASE WHEN trip_distance > 0 THEN 1 END) * 1.0 / COUNT(*)) * 100, 2
            ) AS data_quality_score
        FROM hourly_data
        GROUP BY pickup_zone_name, pickup_borough, taxi_type
        HAVING COUNT(*) >= 5
    )
    SELECT
        current_hour,
        pickup_zone_name,
        pickup_borough,
        taxi_type,
        total_trips,
        avg_trip_distance,
        min_trip_distance,
        max_trip_distance,
        avg_total_amount,
        min_total_amount,
        max_total_amount,
        total_revenue,
        avg_tip_amount,
        avg_passenger_count,
        total_passengers,
        avg_trip_duration_minutes,
        peak_hour_indicator,
        data_quality_score
    FROM zone_stats;

    SELECT COUNT(*) INTO stats_count
    FROM zone_hourly_stats
    WHERE calculation_hour = current_hour;

    RETURN 'SUCCESS: Calculated stats for ' || stats_count || ' zones at hour ' || current_hour::STRING;

  EXCEPTION
    WHEN OTHER THEN
      error_message := SQLERRM;
      RETURN 'ERROR: Failed to calculate hourly stats - ' || error_message;
  END;
END;
$$;

-- Step 4: Create Task 1 - Stream Processing (runs every hour)
CREATE OR REPLACE TASK stream_processing_task
  WAREHOUSE = ZHOHOLIEVPV_WH
  SCHEDULE = '60 MINUTE'
  COMMENT = 'Automated task to process yellow_enriched_stream changes hourly'
AS
  CALL process_stream_changes_batch();

-- Step 5: Create Task 2 - Hourly Statistics (runs every hour, offset by 10 minutes)
CREATE OR REPLACE TASK hourly_stats_task
  WAREHOUSE = ZHOHOLIEVPV_WH
  SCHEDULE = '60 MINUTE'
  COMMENT = 'Automated task to calculate hourly zone statistics'
AS
  CALL calculate_hourly_zone_stats();

-- Step 6: Create a child task that depends on the stream processing task
CREATE OR REPLACE TASK data_quality_check_task
  WAREHOUSE = ZHOHOLIEVPV_WH
  AFTER stream_processing_task
AS
  INSERT INTO yellow_changes_log (
      change_type,
      is_update,
      vendor_id,
      pickup_zone_name,
      pickup_borough,
      total_amount,
      original_record
  )
  SELECT
      'QUALITY_CHECK',
      FALSE,
      -1,
      'SYSTEM_CHECK',
      'MONITORING',
      0.00,
      OBJECT_CONSTRUCT(
          'check_type', 'data_quality',
          'total_records', (SELECT COUNT(*) FROM yellow_enriched),
          'stream_processed', 'TRUE',
          'check_timestamp', CURRENT_TIMESTAMP()
      );

-- Step 7
SELECT
    scheduled_time,
    query_start_time,
    completed_time,
    state,
    return_value,
    error_code,
    error_message,
    run_id
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE scheduled_time >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY scheduled_time DESC;

ALTER TASK stream_processing_task RESUME;
ALTER TASK hourly_stats_task RESUME;
ALTER TASK data_quality_check_task RESUME;

ALTER TASK stream_processing_task SUSPEND;
ALTER TASK hourly_stats_task SUSPEND;
ALTER TASK data_quality_check_task SUSPEND;

EXECUTE TASK stream_processing_task;
EXECUTE TASK hourly_stats_task;

DESC TASK stream_processing_task;
DESC TASK hourly_stats_task;
DESC TASK data_quality_check_task;