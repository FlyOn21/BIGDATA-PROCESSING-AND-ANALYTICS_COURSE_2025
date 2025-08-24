with p as (
  select * from {{ ref('int_players_male') }}
),
agg as (
  select
    club_id,
    avg(overall)::numeric(5,2) as avg_overall,
    sum(coalesce(value_eur,0))  as total_value_eur,
    count(*) as players_count
  from p
  where club_id is not null
  group by club_id
)
select
  t.club_id,
  t.club_name,
  t.league_id,
  t.coach_id,
  c.long_name as coach_name,
  c.coach_age,
  c.nationality_name as coach_nationality,
  a.players_count,
  a.avg_overall,
  a.total_value_eur
from {{ ref('stg_male_teams') }} t
left join agg a using (club_id)
left join {{ ref('int_coaches_male') }} c
  on c.coach_id = t.coach_id;
