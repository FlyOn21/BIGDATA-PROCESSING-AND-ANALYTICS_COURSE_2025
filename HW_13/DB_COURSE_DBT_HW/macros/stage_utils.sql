{% macro to_snake(name) -%}
  {{ return(name | replace(' ', '_') | replace('-', '_') | replace('/', '_') | replace('.', '_') | lower) }}
{%- endmacro %}

{% macro cleanup_expr(alias, col_name, target_type='auto', null_tokens=None) -%}
  {% set alias_q = adapter.quote(alias) %}
  {% set col_q = adapter.quote(col_name) %}
  {% set val = alias_q ~ '.' ~ col_q ~ '::text' %}
  {% set trimmed = 'btrim(' ~ val ~ ')' %}

  {% set nulllist = null_tokens if null_tokens is not none else ['', 'na', 'n/a', 'null', 'none', '-', '—'] %}
  {% set null_checks = [] %}
  {% do null_checks.append(trimmed ~ " = ''") %}

  {% for tok in nulllist if tok %}
    {% do null_checks.append('lower(' ~ trimmed ~ ") = '" ~ tok|lower ~ "'") %}
  {% endfor %}

  {% if target_type in ['int','integer','bigint','smallint'] %}
    CASE
      WHEN {{ ' OR '.join(null_checks) }} THEN NULL
      WHEN {{ trimmed }} ~ '^[-]?\d+$' THEN ({{ trimmed }})::{{ target_type }}
      ELSE NULL
    END
  {% elif target_type in ['numeric','decimal','float','double','real','double precision'] %}
    CASE
      WHEN {{ ' OR '.join(null_checks) }} THEN NULL
      WHEN {{ trimmed }} ~ '^[-]?\d+(\.\d+)?$' THEN ({{ trimmed }})::numeric
      ELSE NULL
    END
  {% elif target_type in ['boolean','bool'] %}
    CASE
      WHEN {{ ' OR '.join(null_checks) }} THEN NULL
      WHEN lower({{ trimmed }}) in ('true','t','1','yes','y') THEN TRUE
      WHEN lower({{ trimmed }}) in ('false','f','0','no','n') THEN FALSE
      ELSE NULL
    END
  {% elif target_type in ['date'] %}
    CASE
      WHEN {{ ' OR '.join(null_checks) }} THEN NULL
      WHEN {{ trimmed }} ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date({{ trimmed }}, 'YYYY-MM-DD')
      WHEN {{ trimmed }} ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date({{ trimmed }}, 'MM/DD/YYYY')
      ELSE NULL
    END
  {% elif target_type in ['timestamp','timestamptz','timestamp with time zone','timestamp without time zone'] %}
    CASE
      WHEN {{ ' OR '.join(null_checks) }} THEN NULL
      WHEN {{ trimmed }} ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$' THEN
        to_timestamp(regexp_replace({{ trimmed }}, 'T', ' ', 'g'), 'YYYY-MM-DD HH24:MI:SS')
      ELSE NULL
    END
  {% elif target_type in ['text','string','varchar','character varying'] %}
    NULLIF({{ trimmed }}, '')
  {% else %}

    NULLIF({{ trimmed }}, '')
  {% endif %}
{%- endmacro %}

{% macro select_all_snake(relation, alias='src', type_map=None, null_tokens=None) -%}
  {% set cols = adapter.get_columns_in_relation(relation) %}
  {% set tm = type_map if type_map is not none else {} %}
  {%- for c in cols %}
    {%- set snake = to_snake(c.name) -%}
    {{ cleanup_expr(alias, c.name, tm.get(snake, 'auto'), null_tokens) }} as {{ adapter.quote(snake) }}{% if not loop.last %},{% endif %}
  {%- endfor %}
{%- endmacro %}

{% macro star_except(relation, alias='src', except=[]) -%}
  {% set cols = adapter.get_columns_in_relation(relation) %}
  {%- for c in cols if (c.name | lower | replace(' ', '_') | replace('-', '_') | replace('/', '_') | replace('.', '_')) not in except -%}
    {{ adapter.quote(alias) }}.{{ adapter.quote(c.name) }}{% if not loop.last %},{% endif %}
  {%- endfor -%}
{%- endmacro %}
