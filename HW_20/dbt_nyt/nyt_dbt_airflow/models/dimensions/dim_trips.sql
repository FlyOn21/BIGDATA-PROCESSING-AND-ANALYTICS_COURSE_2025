{{ config(
    materialized='table',
    schema='dimensions',
    tags=['dimension', 'trip'],
    pre_hooks=[
       "{{ limit_pg_resources() }}",
      "DROP TABLE IF EXISTS {{ this }} CASCADE"
    ],
    indexes=[
        {'columns': ['trip_id'], 'type': 'btree'}
    ]
) }}

WITH unified_trips AS (
    SELECT * FROM {{ ref('stg_yellow_taxi') }}
    UNION ALL
    SELECT * FROM {{ ref('stg_green_taxi') }}
),

enriched_trips AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'pickup_datetime',
            'dropoff_datetime',
            'pu_location_id',
            'do_location_id',
            'taxi_type',
            'trip_distance'
        ]) }} AS trip_id,
        t.*,
        pu_zone.borough AS pickup_borough,
        pu_zone.zone AS pickup_zone,
        pu_zone.service_zone AS pickup_service_zone,
        do_zone.borough AS dropoff_borough,
        do_zone.zone AS dropoff_zone,
        do_zone.service_zone AS dropoff_service_zone,
        CURRENT_TIMESTAMP AS created_at
    FROM unified_trips t
    LEFT JOIN {{ ref('dim_locations') }} pu_zone
        ON t.pu_location_id = pu_zone.location_id
    LEFT JOIN {{ ref('dim_locations') }} do_zone
        ON t.do_location_id = do_zone.location_id
    WHERE t.trip_duration_minutes > 0
      AND t.trip_distance > 0
      AND t.passenger_count > 0
),

deduplicated_trips AS (
    SELECT *
    FROM (
        SELECT
            e.*,
            ROW_NUMBER() OVER (
                PARTITION BY e.trip_id
                ORDER BY
                    e.trip_distance DESC,
                    e.total_amount DESC,
                    e.created_at DESC
            ) AS row_num
        FROM enriched_trips e
    ) ranked
    WHERE row_num = 1
)

SELECT *
FROM deduplicated_trips