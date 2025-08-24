{{ config(
  tags=['mart', 'player_profiles', 'male', 'player_profiles_mart_male']
  ) }}


with p as (
  select
    gender,
    player_id,
    player_url,
    short_name,
    long_name,
    club_id,
    league_id,
    value_eur,
    wage_eur,
    overall,
    potential,
    age,
    position
  from {{ ref('int_players_male')  }}
)
select
  gender,
  player_id,
  player_url,
  short_name,
  long_name,
  club_id,
  league_id,
  value_eur,
  wage_eur,
  overall,
  potential,
  age,
  {{ age_group_filter('p.age') }} as age_group,
  {{ classify_role('p.position') }} as role
from p
