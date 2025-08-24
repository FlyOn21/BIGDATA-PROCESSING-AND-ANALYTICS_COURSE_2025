{{ config(
materialized='table',
tags=['inter','female', 'leagues', 'female_league_inter']) }}

with players as (
  select * from {{ ref('int_players_female') }}
),
teams as (
  select * from {{ ref('int_team_profiles_female') }}
),
league_player_metrics as (
  select
    p.league_id,
    avg(p.overall)::numeric(10,2)                                   as avg_player_overall,
    (sum(case when p.age <= 23 and p.potential > 85 then 1 else 0 end) > 0) as has_young_talents
  from players p
  where p.league_id is not null
  group by p.league_id
),
league_team_metrics as (
  select
    t.league_id,
    avg(t.avg_overall)::numeric(10,2)                                as avg_team_overall,
    sum(t.players_count)                                             as total_players,
    count(*)                                                         as total_teams
  from teams t
  where t.league_id is not null
  group by t.league_id
)
select
  coalesce(lp.league_id, lt.league_id)             as league_id,
  lp.avg_player_overall,
  lt.avg_team_overall,
  lt.total_players,
  lt.total_teams,
  lp.has_young_talents::boolean                    as has_young_talents
from league_player_metrics lp
full join league_team_metrics lt
  on lp.league_id = lt.league_id
