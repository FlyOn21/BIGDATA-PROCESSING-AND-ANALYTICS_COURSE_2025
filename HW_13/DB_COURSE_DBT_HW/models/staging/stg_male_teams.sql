{{ config(
    materialized='table',
    tags=['stage', 'male_team']
) }}

with src as (
  select
    {{ select_all_snake(source('public_raw','male_teams_10000'), alias='s', type_map=var('male_teams_type_map', {})) }}
  from {{ source('public_raw','male_teams_10000') }} as s
)
select
  *
from src
