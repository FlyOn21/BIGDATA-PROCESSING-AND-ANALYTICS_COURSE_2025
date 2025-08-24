{{ config(
    materialized='table',
    tags=['stage', 'male_player']
) }}

with src as (
  select
    {{ select_all_snake(source('public_raw','male_players_10000'), alias='s', type_map=var('male_players_type_map', {})) }}
  from {{ source('public_raw','male_players_10000') }} as s
)
select
  'M'::text as gender, *,
  {{ skey(["player_id","quote_literal('M')", "player_url", "coalesce(club_team_id::text,'')", "coalesce(league_id::text,'')"]) }} as id
from src
