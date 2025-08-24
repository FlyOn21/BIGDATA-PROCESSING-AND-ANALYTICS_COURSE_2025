{{ config(
    materialized='table',
    tags=['stage', 'female_team']
) }}

with src as (
  select
    {{ select_all_snake(source('public_raw','female_teams_10000'), alias='s', type_map=var('female_teams_type_map', {})) }}
  from {{ source('public_raw','female_teams_10000') }} as s
)
select
  *
from src
