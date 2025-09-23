SELECT *
FROM {{ ref('dim_trips') }}
WHERE passenger_count < 1 OR passenger_count > 6