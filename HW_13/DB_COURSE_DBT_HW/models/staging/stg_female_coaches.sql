{{ config(
    materialized='table',
    tags=['stage', 'female_coach']
) }}

with src as (
  select
    {{ select_all_snake(source('public_raw','female_coaches_10000'), alias='s', type_map=var('female_coaches_type_map', {})) }}
  from {{ source('public_raw','female_coaches_10000') }} as s
)
select
  'F'::text as gender, *
from src
