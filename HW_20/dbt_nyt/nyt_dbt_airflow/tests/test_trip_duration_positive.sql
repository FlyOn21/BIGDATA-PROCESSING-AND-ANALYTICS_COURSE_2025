SELECT *
FROM {{ ref('dim_trips') }}
WHERE trip_duration_minutes <= 0