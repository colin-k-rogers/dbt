{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/utils/date.sql. #}
{% macro motherduck__date(year, month, day) -%}
    make_date({{ year }}, {{ month }}, {{ day }})
{%- endmacro %}
