{{ config(
    materialized='incremental',
    unique_key='coach_id',
    tags=['inter', 'female_coach', 'female_coach_inter']
) }}

with base as (
  select
    coach_id,
    gender,
    short_name,
    long_name,
    nationality_id,
    nationality_name,
    dob,
    coach_url,
    face_url,
    id as stage_id
  from {{ ref('stg_female_coaches') }}
)

select
  coach_id,
  gender,
  short_name,
  long_name,
  nationality_id,
  nationality_name,
  dob,
  stage_id,
  case when dob is not null
       then date_part('year', age(current_date, dob))::int
  end as coach_age,
  coach_url,
  face_url
from base

{% if is_incremental() %}
where coach_id not in (select coach_id from {{ this }})
{% endif %}

