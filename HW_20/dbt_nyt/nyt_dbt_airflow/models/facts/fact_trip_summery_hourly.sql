{{ config(
    materialized='table',
    schema='facts',
    tags=['fact', 'trip_summary_hourly'],
    pre_hooks=[
       "{{ limit_pg_resources() }}",
      "DROP TABLE IF EXISTS {{ this }} CASCADE"
    ],
    indexes=[
        {
            'columns': ['created_at'],
            'type': 'btree'
        }
    ]
) }}

SELECT
    pickup_date,
    pickup_hour,
    taxi_type,
    pickup_borough,
    dropoff_borough,
    COUNT(*) as trip_count,
    SUM(passenger_count) as total_passengers,
    AVG(trip_distance) as avg_trip_distance,
    AVG(trip_duration_minutes) as avg_trip_duration,
    SUM(fare_amount) as total_fare_amount,
    SUM(tip_amount) as total_tip_amount,
    SUM(total_amount) as total_amount_sum,
    AVG(total_amount) as avg_total_amount,
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('dim_trips') }}
GROUP BY 1,2,3,4,5