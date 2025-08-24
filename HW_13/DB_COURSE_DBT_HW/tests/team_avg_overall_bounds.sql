with tp as (
  select * from {{ ref('int_team_profiles_male') }}
  union all
  select * from {{ ref('int_team_profiles_female') }}
)
select *
from tp
where avg_overall < 40 or avg_overall > 100;
