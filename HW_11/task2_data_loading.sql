-- Task 2

CREATE OR REPLACE FILE FORMAT parquet_format
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY'
  BINARY_AS_TEXT = FALSE
  REPLACE_INVALID_CHARACTERS = FALSE
  NULL_IF = ('NULL', 'null', 'N/A', 'n/a', '');

CREATE OR REPLACE STAGE s3_parquet_stage_zhohlievpv_green
  STORAGE_INTEGRATION = s3_int_zhohlievpv
  URL = 's3://robot-dreams-source-data/home-work-1/nyc_taxi/green/'
  FILE_FORMAT = parquet_format;

LIST @s3_parquet_stage_zhohlievpv_green;

CREATE OR REPLACE STAGE s3_parquet_stage_zhohlievpv_yellow
  STORAGE_INTEGRATION = s3_int_zhohlievpv
  URL = 's3://robot-dreams-source-data/home-work-1/nyc_taxi/yellow/'
  FILE_FORMAT = parquet_format;

LIST @s3_parquet_stage_zhohlievpv_yellow;

CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    TRIM_SPACE = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY='"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ;

CREATE OR REPLACE STAGE s3_csv_stage_zhohlievpv
  STORAGE_INTEGRATION = s3_int_zhohlievpv
  URL = 's3://robot-dreams-source-data/home-work-1/nyc_taxi/taxi_zone_lookup.csv'
  FILE_FORMAT = csv_format;

LIST @s3_csv_stage_zhohlievpv;

DROP TABLE IF EXISTS YELLOW_TAXI_COMPATIBLE;

CREATE OR REPLACE TABLE YELLOW_TAXI_COMPATIBLE (
    VendorID NUMBER(38,0),
    tpep_pickup_datetime_raw VARCHAR(50),
    tpep_dropoff_datetime_raw VARCHAR(50),
    passenger_count NUMBER(38,2),
    trip_distance NUMBER(38,6),
    RatecodeID NUMBER(38,2),
    store_and_fwd_flag VARCHAR(10),
    PULocationID NUMBER(38,0),
    DOLocationID NUMBER(38,0),
    payment_type NUMBER(38,0),
    fare_amount NUMBER(38,2),
    extra NUMBER(38,2),
    mta_tax NUMBER(38,2),
    tip_amount NUMBER(38,2),
    tolls_amount NUMBER(38,2),
    improvement_surcharge NUMBER(38,2),
    total_amount NUMBER(38,2),
    congestion_surcharge NUMBER(38,2),
    airport_fee NUMBER(38,2)
);

COPY INTO YELLOW_TAXI_COMPATIBLE
FROM (
    SELECT
        $1:VendorID::NUMBER(38,0) as VendorID,
        $1:tpep_pickup_datetime::VARCHAR(50) as tpep_pickup_datetime_raw,
        $1:tpep_dropoff_datetime::VARCHAR(50) as tpep_dropoff_datetime_raw,
        $1:passenger_count::NUMBER(38,2) as passenger_count,
        $1:trip_distance::NUMBER(38,6) as trip_distance,
        $1:RatecodeID::NUMBER(38,2) as RatecodeID,
        $1:store_and_fwd_flag::VARCHAR(10) as store_and_fwd_flag,
        $1:PULocationID::NUMBER(38,0) as PULocationID,
        $1:DOLocationID::NUMBER(38,0) as DOLocationID,
        $1:payment_type::NUMBER(38,0) as payment_type,
        $1:fare_amount::NUMBER(38,2) as fare_amount,
        $1:extra::NUMBER(38,2) as extra,
        $1:mta_tax::NUMBER(38,2) as mta_tax,
        $1:tip_amount::NUMBER(38,2) as tip_amount,
        $1:tolls_amount::NUMBER(38,2) as tolls_amount,
        $1:improvement_surcharge::NUMBER(38,2) as improvement_surcharge,
        $1:total_amount::NUMBER(38,2) as total_amount,
        $1:congestion_surcharge::NUMBER(38,2) as congestion_surcharge,
        $1:airport_fee::NUMBER(38,2) as airport_fee
    FROM @s3_parquet_stage_zhohlievpv_yellow
)
FILE_FORMAT = parquet_format
ON_ERROR = 'CONTINUE';

SELECT
    tpep_pickup_datetime_raw,
    tpep_dropoff_datetime_raw,
    TRY_TO_TIMESTAMP(tpep_pickup_datetime_raw) as converted_pickup,
    TRY_TO_TIMESTAMP(tpep_dropoff_datetime_raw) as converted_dropoff
FROM YELLOW_TAXI_COMPATIBLE
LIMIT 10;

CREATE OR REPLACE TABLE green_taxi_compatible (
    VendorID NUMBER(38,0),
    lpep_pickup_datetime_raw VARCHAR(50),
    lpep_dropoff_datetime_raw VARCHAR(50),
    store_and_fwd_flag VARCHAR(10),
    RatecodeID NUMBER(38,2),
    PULocationID NUMBER(38,0),
    DOLocationID NUMBER(38,0),
    passenger_count NUMBER(38,2),
    trip_distance NUMBER(38,6),
    fare_amount NUMBER(38,2),
    extra NUMBER(38,2),
    mta_tax NUMBER(38,2),
    tip_amount NUMBER(38,2),
    tolls_amount NUMBER(38,2),
    ehail_fee NUMBER(38,2),
    improvement_surcharge NUMBER(38,2),
    total_amount NUMBER(38,2),
    payment_type NUMBER(38,0),
    trip_type NUMBER(38,2),
    congestion_surcharge NUMBER(38,2)
);

COPY INTO green_taxi_compatible
FROM (
    SELECT
        $1:VendorID::NUMBER(38,0) as VendorID,
        $1:lpep_pickup_datetime::VARCHAR(50) as lpep_pickup_datetime_raw,
        $1:lpep_dropoff_datetime::VARCHAR(50) as lpep_dropoff_datetime_raw,
        $1:store_and_fwd_flag::VARCHAR(10) as store_and_fwd_flag,
        $1:RatecodeID::NUMBER(38,2) as RatecodeID,
        $1:PULocationID::NUMBER(38,0) as PULocationID,
        $1:DOLocationID::NUMBER(38,0) as DOLocationID,
        $1:passenger_count::NUMBER(38,2) as passenger_count,
        $1:trip_distance::NUMBER(38,6) as trip_distance,
        $1:fare_amount::NUMBER(38,2) as fare_amount,
        $1:extra::NUMBER(38,2) as extra,
        $1:mta_tax::NUMBER(38,2) as mta_tax,
        $1:tip_amount::NUMBER(38,2) as tip_amount,
        $1:tolls_amount::NUMBER(38,2) as tolls_amount,
        $1:ehail_fee::NUMBER(38,2) as ehail_fee,
        $1:improvement_surcharge::NUMBER(38,2) as improvement_surcharge,
        $1:total_amount::NUMBER(38,2) as total_amount,
        $1:payment_type::NUMBER(38,0) as payment_type,
        $1:trip_type::NUMBER(38,2) as trip_type,
        $1:congestion_surcharge::NUMBER(38,2) as congestion_surcharge
    FROM @s3_parquet_stage_zhohlievpv_green
)
FILE_FORMAT = parquet_format
ON_ERROR = 'CONTINUE';

SELECT
    lpep_pickup_datetime_raw,
    lpep_dropoff_datetime_raw,
    TRY_TO_TIMESTAMP(lpep_pickup_datetime_raw) as converted_pickup,
    TRY_TO_TIMESTAMP(lpep_dropoff_datetime_raw) as converted_dropoff
FROM green_taxi_compatible
LIMIT 10;