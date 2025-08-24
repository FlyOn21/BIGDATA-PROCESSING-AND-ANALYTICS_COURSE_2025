select
  player_id, gender, player_name, club_id, league_id,
  value_eur, wage_eur, overall, potential, age, position
from {{ ref('stg_female_players') }};
