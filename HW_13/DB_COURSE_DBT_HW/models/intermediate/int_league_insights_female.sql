with tp as (
  select * from {{ ref('int_team_profiles_female') }}
),
pl as (
  select * from {{ ref('int_players_female') }}
),
league_agg as (
  select
    tp.league_id,
    avg(tp.avg_overall)::numeric(5,2) as league_avg_overall
  from tp
  group by tp.league_id
),
young_talents as (
  select
    league_id,
    bool_or(age <= 23 and potential > 85) as has_young_talents
  from pl
  group by league_id
)
select
  l.league_id,
  l.league_avg_overall,
  coalesce(y.has_young_talents,false) as has_young_talents
from league_agg l
left join young_talents y using (league_id);
