-- TASK3

CREATE OR REPLACE TABLE taxi_zone_lookup (
    LocationID NUMBER(38,0),
    Borough VARCHAR(50),
    Zone VARCHAR(100),
    service_zone VARCHAR(50)
);

COPY INTO taxi_zone_lookup
FROM @s3_csv_stage_zhohlievpv
FILE_FORMAT = csv_format
ON_ERROR = 'CONTINUE';

CREATE OR REPLACE TABLE yellow_enriched AS
SELECT
    y.*,
    pu_zone.Zone AS pickup_zone_name,
    pu_zone.Borough AS pickup_borough,
    do_zone.Zone AS dropoff_zone_name,
    do_zone.Borough AS dropoff_borough
FROM YELLOW_TAXI_COMPATIBLE y
LEFT JOIN taxi_zone_lookup pu_zone
    ON y.PULocationID = pu_zone.LocationID
LEFT JOIN taxi_zone_lookup do_zone
    ON y.DOLocationID = do_zone.LocationID;

CREATE OR REPLACE TABLE green_enriched AS
SELECT
    g.*,
    pu_zone.Zone AS pickup_zone_name,
    pu_zone.Borough AS pickup_borough,
    do_zone.Zone AS dropoff_zone_name,
    do_zone.Borough AS dropoff_borough
FROM green_taxi_compatible g
LEFT JOIN taxi_zone_lookup pu_zone
    ON g.PULocationID = pu_zone.LocationID
LEFT JOIN taxi_zone_lookup do_zone
    ON g.DOLocationID = do_zone.LocationID;

SELECT
    PULocationID,
    pickup_zone_name,
    pickup_borough,
    DOLocationID,
    dropoff_zone_name,
    dropoff_borough,
    COUNT(*) AS trip_count
FROM yellow_enriched
WHERE pickup_zone_name IS NOT NULL
    AND dropoff_zone_name IS NOT NULL
GROUP BY 1,2,3,4,5,6
ORDER BY trip_count DESC
LIMIT 10;

SELECT
    'Yellow Taxi - Pickup' AS data_type,
    COUNT(*) AS total_trips,
    SUM(CASE WHEN pickup_zone_name IS NULL THEN 1 ELSE 0 END) AS missing_pickup_zones,
    SUM(CASE WHEN dropoff_zone_name IS NULL THEN 1 ELSE 0 END) AS missing_dropoff_zones
FROM yellow_enriched

UNION ALL

SELECT
    'Green Taxi - Pickup' AS data_type,
    COUNT(*) AS total_trips,
    SUM(CASE WHEN pickup_zone_name IS NULL THEN 1 ELSE 0 END) AS missing_pickup_zones,
    SUM(CASE WHEN dropoff_zone_name IS NULL THEN 1 ELSE 0 END) AS missing_dropoff_zones
FROM green_enriched;