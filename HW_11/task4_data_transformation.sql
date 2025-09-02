-- TASK4
CREATE OR REPLACE TABLE yellow_transformed AS
SELECT
    *,
    TRY_TO_TIMESTAMP(tpep_dropoff_datetime_raw) AS tpep_dropoff_datetime,
    TRY_TO_TIMESTAMP(tpep_pickup_datetime_raw) AS tpep_pickup_datetime,
    CASE
        WHEN trip_distance <= 2 THEN 'Short'
        WHEN trip_distance <= 10 THEN 'Medium'
        ELSE 'Long'
    END AS trip_category,

    HOUR(tpep_pickup_datetime) AS pickup_hour,
    'Yellow' AS taxi_type

FROM yellow_enriched
WHERE
    trip_distance > 0
    AND total_amount > 0
    AND passenger_count BETWEEN 1 AND 6;




CREATE OR REPLACE TABLE green_transformed AS
SELECT
    *,
    TRY_TO_TIMESTAMP(lpep_dropoff_datetime_raw) AS lpep_dropoff_datetime,
    TRY_TO_TIMESTAMP(lpep_pickup_datetime_raw) AS lpep_pickup_datetime,
    CASE
        WHEN trip_distance <= 2 THEN 'Short'
        WHEN trip_distance <= 10 THEN 'Medium'
        ELSE 'Long'
    END AS trip_category,

    HOUR(lpep_pickup_datetime) AS pickup_hour,

    'Green' AS taxi_type

FROM green_enriched
WHERE
    trip_distance > 0
    AND total_amount > 0
    AND passenger_count BETWEEN 1 AND 6;




CREATE OR REPLACE TABLE taxi_unified_transformed AS
SELECT
    VendorID,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
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
    dropoff_borough,
    trip_category,
    pickup_hour,
    taxi_type
FROM yellow_transformed

UNION ALL

SELECT
    VendorID,
    lpep_pickup_datetime AS pickup_datetime,
    lpep_dropoff_datetime AS dropoff_datetime,
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
    ehail_fee AS improvement_surcharge,
    total_amount,
    congestion_surcharge,
    NULL AS airport_fee,
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    dropoff_borough,
    trip_category,
    pickup_hour,
    taxi_type
FROM green_transformed;




CREATE OR REPLACE TABLE taxi_zone_aggregates AS
SELECT
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    dropoff_borough,
    taxi_type,
    trip_category,
    pickup_hour,

    COUNT(*) AS total_trips,
    COUNT(DISTINCT DATE(pickup_datetime)) AS active_days,

    AVG(trip_distance) AS avg_trip_distance,
    MIN(trip_distance) AS min_trip_distance,
    MAX(trip_distance) AS max_trip_distance,

    AVG(total_amount) AS avg_total_amount,
    SUM(total_amount) AS total_revenue,
    AVG(tip_amount) AS avg_tip_amount,

    AVG(passenger_count) AS avg_passenger_count,

    AVG(DATEDIFF('minute', pickup_datetime, dropoff_datetime)) AS avg_trip_duration_minutes



FROM taxi_unified_transformed
WHERE pickup_zone_name IS NOT NULL
    AND dropoff_zone_name IS NOT NULL
GROUP BY
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    dropoff_borough,
    taxi_type,
    trip_category,
    pickup_hour;



CREATE OR REPLACE TABLE taxi_summary_stats AS
SELECT
    'Overall' AS summary_level,
    taxi_type,
    COUNT(*) AS total_trips,
    COUNT(DISTINCT pickup_zone_name) AS unique_pickup_zones,
    COUNT(DISTINCT dropoff_zone_name) AS unique_dropoff_zones,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_fare,
    SUM(total_amount) AS total_revenue
FROM taxi_unified_transformed
GROUP BY taxi_type

UNION ALL

SELECT
    'By Category' AS summary_level,
    CONCAT(taxi_type, ' - ', trip_category) AS taxi_type,
    COUNT(*) AS total_trips,
    COUNT(DISTINCT pickup_zone_name) AS unique_pickup_zones,
    COUNT(DISTINCT dropoff_zone_name) AS unique_dropoff_zones,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_fare,
    SUM(total_amount) AS total_revenue
FROM taxi_unified_transformed
GROUP BY taxi_type, trip_category

UNION ALL

SELECT
    'By Borough' AS summary_level,
    CONCAT(taxi_type, ' - ', pickup_borough) AS taxi_type,
    COUNT(*) AS total_trips,
    COUNT(DISTINCT pickup_zone_name) AS unique_pickup_zones,
    COUNT(DISTINCT dropoff_zone_name) AS unique_dropoff_zones,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_fare,
    SUM(total_amount) AS total_revenue
FROM taxi_unified_transformed
WHERE pickup_borough IS NOT NULL
GROUP BY taxi_type, pickup_borough
ORDER BY summary_level, total_trips DESC;



SELECT 'Transformation Results' AS check_type,
       COUNT(*)::VARCHAR AS record_count
FROM taxi_unified_transformed

UNION ALL

SELECT 'Trip Category Distribution' AS check_type,
       trip_category AS record_count
FROM (
    SELECT trip_category, COUNT(*) AS cnt
    FROM taxi_unified_transformed
    GROUP BY trip_category
    ORDER BY cnt DESC
    LIMIT 1
) sub

UNION ALL

SELECT 'Peak Hour' AS check_type,
       pickup_hour::VARCHAR AS record_count
FROM (
    SELECT pickup_hour, COUNT(*) AS cnt
    FROM taxi_unified_transformed
    GROUP BY pickup_hour
    ORDER BY cnt DESC
    LIMIT 1
) sub;



SELECT
    'Data Quality Check' AS metric,
    CONCAT('Original Records: ',
           (SELECT COUNT(*) FROM yellow_enriched) + (SELECT COUNT(*) FROM green_enriched),
           ' | Filtered Records: ',
           COUNT(*),
           ' | Reduction: ',
           ROUND(100 * (1 - COUNT(*)::FLOAT / ((SELECT COUNT(*) FROM yellow_enriched) + (SELECT COUNT(*) FROM green_enriched))), 2),
           '%') AS details
FROM taxi_unified_transformed;