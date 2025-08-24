{{ config(
    materialized='table',
    tags=['stage', 'male_player']
) }}

with src as (
  select
    player_id::int as player_id,
    player_url::text as player_url,
    nullif(btrim(short_name), '')::text  as short_name,
    nullif(btrim(long_name), '')::text  as long_name,
    club_team_id::int as club_id,
    league_id::int,
    nullif(value_eur,null)::numeric as value_eur,
    nullif(wage_eur,null)::numeric as wage_eur,
    nullif(overall,null)::int as overall,
    nullif(potential,null)::int as potential,
    nullif(age,null)::int as age,
    nullif(player_positions,'')::text as position
  from {{ source('public_raw','male_players_10000') }}
)
select
  {{ skey(["player_id","quote_literal('M')", "player_url", "coalesce(club_id::text,'')", "coalesce(league_id::text,'')"]) }} as id,
  'M'::text as gender,
  *
from src
where player_url is not null
