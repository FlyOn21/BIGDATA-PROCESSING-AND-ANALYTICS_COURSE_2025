with all_players as (
  select player_id from {{ ref('int_players_male') }}
  union all
  select player_id from {{ ref('int_players_female') }}
),
dups as (
  select player_id, count(*) as c
  from all_players
  group by 1
  having count(*) > 1
)
select * from dups;
