{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/utils/normalize.sql. #}
{%- macro normalize_string_or_list(raw, field_name) -%}
  {%- if raw is none -%}
    {%- set result = [] -%}
  {%- elif raw is mapping -%}
    {{ exceptions.raise_compiler_error(field_name ~ " must be a list of strings or a string") }}
  {%- elif raw is string -%}
    {%- set result = [raw] -%}
  {%- elif raw is sequence -%}
    {%- set result = raw | list -%}
  {%- else -%}
    {{ exceptions.raise_compiler_error(field_name ~ " must be a list of strings or a string") }}
  {%- endif -%}
  {%- for item in result -%}
    {%- if item is not string -%}
      {% do exceptions.raise_compiler_error(field_name ~ " list values must be non-empty strings") %}
    {%- endif -%}
  {%- endfor -%}
  {{ return(result) }}
{%- endmacro -%}
