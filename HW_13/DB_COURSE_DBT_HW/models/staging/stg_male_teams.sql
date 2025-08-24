{{ config(
    materialized='table',
    tags=['stage', 'male_team']
) }}

with src as (select
  team_id::int as club_id,
  team_url::text as club_url,
  trim(team_name)::text as club_name,
  league_id::int as league_id,
  league_name::text as league_name,
  nationality_id::int as club_nationality_id,
  nationality_name::text as club_nationality,
  nullif(coach_id,null)::int as coach_id
  from {{ source('public_raw','male_teams_10000') }})
select *,
       {{ skey(["club_id", "club_url"]) }} as id
from src
where club_id is not null
