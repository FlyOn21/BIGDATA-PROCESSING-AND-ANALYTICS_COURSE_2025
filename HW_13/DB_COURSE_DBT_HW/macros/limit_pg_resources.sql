{% macro limit_pg_resources(
    work_mem_mb=64,
    parallel_workers_per_gather=0,
    statement_timeout='10min',
    temp_buffers_mb=16,
    jit=false
) -%}
  set local work_mem = '{{ work_mem_mb }}MB';
  set local max_parallel_workers_per_gather = {{ parallel_workers_per_gather }};
  set local statement_timeout = '{{ statement_timeout }}';
  set local temp_buffers = '{{ temp_buffers_mb }}MB';
  set local jit = {{ 'on' if jit else 'off' }};
{%- endmacro %}