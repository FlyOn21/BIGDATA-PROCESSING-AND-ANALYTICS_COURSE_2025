with pl as (
  select * from {{ ref('mart_player_profiles_male') }}
  union all
  select * from {{ ref('mart_player_profiles_female') }}
)
select *
from pl
where wage_eur is not null
  and value_eur is not null
  and wage_eur > value_eur
