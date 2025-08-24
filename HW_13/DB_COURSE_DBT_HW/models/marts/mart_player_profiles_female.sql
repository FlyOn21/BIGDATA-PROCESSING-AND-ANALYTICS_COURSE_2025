with p as (select * from {{ ref('int_players_female') }})
select
  player_id,
  gender,
  player_name,
  club_id,
  league_id,
  overall,
  potential,
  age,
  case
    when age < 21 then 'U21'
    when age between 21 and 28 then 'Prime'
    else 'Veteran'
  end as age_group,
  {{ classify_role('position') }} as role_class,
  value_eur,
  wage_eur
from p;
