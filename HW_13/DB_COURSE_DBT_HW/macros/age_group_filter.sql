{% macro age_group_filter(age) -%}
case
    when {{ age }} <= 21 then 'U21'
    when {{ age }} <= 28 then 'Prime'
    else 'Veteran'
end
{%- endmacro %}
