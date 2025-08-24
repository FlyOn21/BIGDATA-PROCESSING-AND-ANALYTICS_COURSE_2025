{% macro classify_role(pos) -%}
case
  when {{ pos }} in ('RB','RWB','LB','LWB','WB') then 'Wingback'
  when {{ pos }} in ('ST','CF') then 'Poacher'
  when {{ pos }} in ('CM','CDM','DM','DLP') then 'Regista'
  when {{ pos }} in ('LW','RW','LM','RM') then 'Winger'
  when {{ pos }} = 'CB' then 'Center Back'
  when {{ pos }} = 'GK' then 'Goalkeeper'
  else 'Other'
end
{%- endmacro %}
