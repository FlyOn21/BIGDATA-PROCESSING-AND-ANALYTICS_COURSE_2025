{% macro skey(cols) -%}
  md5(concat_ws('||', {{ cols | join(', ') }}))
{%- endmacro %}
