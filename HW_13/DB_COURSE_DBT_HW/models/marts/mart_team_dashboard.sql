{{ config(
  tags=['mart', 'team_dashboard'])
  }}


with teams_m as (
  select 'M'::text as gender, t.*
  from {{ ref('int_team_profiles_male') }} t
),
teams_f as (
  select 'F'::text as gender, t.*
  from {{ ref('int_team_profiles_female') }} t
),
teams as (
  select * from teams_m
  union all
  select * from teams_f
),
players as (
  select * from {{ ref('int_players_male') }}
  union all
  select * from {{ ref('int_players_female') }}
),
ranked as (
  select
    p.gender,
    p.club_id,
    p.player_id,
    p.short_name,
    p.overall,
    p.potential,
    p.age,
    row_number() over (partition by p.gender, p.club_id order by p.overall desc, p.potential desc) as rk
  from players p
  where p.club_id is not null
),
key_players as (
  select
    r.gender,
    r.club_id,
    string_agg(r.short_name || ' (' || r.overall || ')', ', ' order by r.overall desc) as key_players_top3
  from ranked r
  where r.rk <= 3
  group by r.gender, r.club_id
),
overperf as (
  select
    p.gender,
    p.club_id,
    string_agg(p.short_name || ' (' || p.overall || ')', ', ' order by p.overall desc) as overperformers,
    count(*) as overperformers_count
  from players p
  join teams t
    on t.gender = p.gender and t.club_id = p.club_id
  where (p.overall >= coalesce(p.potential, p.overall) - 2)
     or (p.overall >= coalesce(t.avg_overall, 0) + 5)
  group by p.gender, p.club_id
)
select
  t.gender,
  t.club_id,
  t.league_id,
  t.players_count,
  t.avg_overall,
  t.total_team_value_eur,
  kp.key_players_top3,
  op.overperformers,
  coalesce(op.overperformers_count, 0) as overperformers_count,
  (coalesce(op.overperformers_count, 0) > 0) as has_overperformers
from teams t
left join key_players kp
  on kp.gender = t.gender and kp.club_id = t.club_id
left join overperf op
  on op.gender = t.gender and op.club_id = t.club_id
