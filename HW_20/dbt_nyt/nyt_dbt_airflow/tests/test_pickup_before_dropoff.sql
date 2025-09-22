SELECT *
FROM {{ ref('dim_trips') }}
WHERE pickup_datetime >= dropoff_datetime