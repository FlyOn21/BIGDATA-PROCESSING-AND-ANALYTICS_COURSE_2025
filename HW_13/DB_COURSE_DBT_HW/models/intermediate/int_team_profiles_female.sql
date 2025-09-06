{{ config(
  materialized='table',
  tags=['inter', 'teams_female', 'female', 'teams_female_inter']
  ) }}


-- Aggregate team profiles (female) from int_players_female
with players as (
  select * from {{ ref('int_players_female') }}
),
agg as (
  select
    club_id,
    min(league_id)                              as league_id,
    count(*)                                    as players_count,
    avg(overall)::numeric(10,2)                 as avg_overall,
    coalesce(sum(value_eur), 0)::numeric        as total_team_value_eur
  from players
  where club_id is not null
  group by club_id
)
select * from agg
