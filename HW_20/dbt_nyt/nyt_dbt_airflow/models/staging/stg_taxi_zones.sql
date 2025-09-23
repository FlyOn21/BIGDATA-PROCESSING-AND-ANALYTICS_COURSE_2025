{{ config(
    materialized='view',
    tags=['stage', 'zone_lookup']
) }}

SELECT
    zones."LocationID" as location_id,
    zones."Borough" as borough,
    zones."Zone" as zone,
    zones.service_zone
FROM {{ var('zone_lookup_stage_table') }} as zones