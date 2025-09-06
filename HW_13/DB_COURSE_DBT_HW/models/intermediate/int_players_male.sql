{{ config(
  materialized='table',
  tags=['inter', 'players_male', 'male', 'players_male_inter']
  ) }}


with src as (
  select
    s.gender::text                                      as gender,
    nullif(btrim(s.id::text), '')::text             as player_id,
    s.player_id::int                                    as player_id_stg,
    nullif(btrim(s.player_url), '')::text               as player_url,
    nullif(btrim(s.short_name), '')::text               as short_name,
    nullif(btrim(s.long_name), '')::text                as long_name,
    nullif(s.club_team_id, null)::int                   as club_id,
    nullif(s.league_id, null)::int                      as league_id,
    nullif(s.value_eur, null)::numeric                  as value_eur,
    nullif(s.wage_eur, null)::numeric                   as wage_eur,
    nullif(s.overall, null)::int                        as overall,
    nullif(s.potential, null)::int                      as potential,
    nullif(s.age, null)::int                            as age,
    nullif(btrim(s.player_positions), '')::text         as position
  from {{ ref('stg_male_players') }} as s
)
select * from src
where player_id is not null
