{{ config(
    materialized='view',
    tags=['stage', 'yellow_taxi']
    ) }}

SELECT
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
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
    'yellow' as taxi_type,
    EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime))/60 as trip_duration_minutes,
    EXTRACT(HOUR FROM pickup_datetime) as pickup_hour,
    EXTRACT(DOW FROM pickup_datetime) as pickup_day_of_week,
    pickup_datetime::date as pickup_date
FROM {{ var('yellow_stage_table') }}
WHERE pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
  AND pickup_datetime < dropoff_datetime