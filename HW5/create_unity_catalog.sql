-- use in Databricks SQL
CREATE CATALOG IF NOT EXISTS `pavlo_zhoholiev_nyc_catalog`;
USE CATALOG `pavlo_zhoholiev_nyc_catalog`;

CREATE SCHEMA IF NOT EXISTS trips_schema
COMMENT 'Schema NYC taxi';

USE SCHEMA trips_schema;

CREATE TABLE IF NOT EXISTS trips (
    vendor_id STRING,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance FLOAT,
    pickup_longitude FLOAT,
    pickup_latitude FLOAT,
    rate_code_id INT,
    store_and_fwd_flag STRING,
    dropoff_longitude FLOAT,
    dropoff_latitude FLOAT,
    payment_type STRING,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT
)

-- Check catalogs is created
SHOW CATALOGS;

-- Check schemas in catalog
USE CATALOG `pavlo_zhoholiev_nyc_catalog`;
SHOW SCHEMAS;

-- Check tables in schema
USE SCHEMA trips_schema;
SHOW TABLES;

-- ОтрGet table information
DESCRIBE EXTENDED raw_trips;