{{ config(
    materialized='table',
    schema='dimensions',
    tags=['dimension', 'location'],
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
    location_id,
    borough,
    zone,
    service_zone,
    CURRENT_TIMESTAMP as created_at
FROM {{ ref('stg_taxi_zones') }}