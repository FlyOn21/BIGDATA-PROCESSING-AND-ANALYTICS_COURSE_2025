{{ config(
    materialized='table',
    tags=['stage', 'male_coach']
) }}

with src as (
  select
    coach_id::int                               as coach_id,
    nullif(btrim(short_name), '')::text         as short_name,
    nullif(btrim(long_name), '')::text          as long_name,
    nullif(btrim(dob::text, '"'), '')           as dob_txt,

    nationality_id::text                        as nationality_id_txt,
    nullif(btrim(nationality_name), '')::text   as nationality_name,
    nullif(coach_url, '')::text                 as coach_url,
    nullif(face_url, '')::text                  as face_url
  from {{ source('public_raw','male_coaches_10000') }}
),
typed as (
  select
    coach_id, short_name, long_name,
    case
      when dob_txt is null then date '1000-01-01'
      when dob_txt = '1000-01-01T00:00:00' then date '1000-01-01'
      when dob_txt ~ '^\d{4}-\d{2}-\d{2}$'
        then dob_txt::date
      when dob_txt ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$'
        then to_timestamp(replace(dob_txt, 'T', ' '), 'YYYY-MM-DD HH24:MI:SS')::date
      else date '1000-01-01'
    end                                           as dob,

    case
      when nationality_id_txt is null or btrim(nationality_id_txt) = '' then 0
      when nationality_id_txt ~ '^\d+$' then nationality_id_txt::int
      else 0
    end                                           as nationality_id,

    nationality_name, coach_url, face_url
  from src
)
select
  'M'::text as gender,
  {{ skey(["coach_id", "coach_url"]) }} as id ,
  *
from typed
where coach_id is not null