{{ config(
    materialized='table',
    tags=['stage', 'male_coach']
) }}


with src as (
  select
    {{ select_all_snake(source('public_raw','male_coaches_10000'), alias='s', type_map=var('male_coaches_type_map', {})) }}
  from {{ source('public_raw','male_coaches_10000') }} as s
)
select
  'M'::text as gender, *
from src
