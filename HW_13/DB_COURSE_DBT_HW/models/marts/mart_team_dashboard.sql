with tp_m as (select *, 'M'::text gender from {{ ref('int_team_profiles_male') }}),
     tp_f as (select *, 'F'::text gender from {{ ref('int_team_profiles_female') }}),
     tp as (
       select * from tp_m
       union all
       select * from tp_f
     ),
     pl as (
       select * from {{ ref('mart_player_profiles_male') }}
       union all
       select * from {{ ref('mart_player_profiles_female') }}
     ),
     key_players as (
       select
         club_id,
         gender,
         array_agg(player_id order by overall desc)[:3] as top3_players
       from pl
       group by 1,2
     ),
     outperformers as (
       select
         p.club_id,
         p.gender,
         array_agg(player_id order by (p.overall - t.avg_overall) desc)[:3] as top3_outperform
       from pl p
       join tp t using (club_id, gender)
       where p.overall > t.avg_overall + 5
       group by 1,2
     )
select
  t.gender,
  t.club_id,
  t.club_name,
  t.league_id,
  t.coach_id,
  t.coach_name,
  t.coach_age,
  t.coach_nationality,
  t.players_count,
  t.avg_overall,
  t.total_value_eur,
  k.top3_players,
  o.top3_outperform
from tp t
left join key_players k using (club_id, gender)
left join outperformers o using (club_id, gender);
