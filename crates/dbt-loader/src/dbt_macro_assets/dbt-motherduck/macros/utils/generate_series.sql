{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/utils/generate_series.sql. #}
{% macro motherduck__generate_series(upper_bound) %}
    select
        generate_series as generated_number
    from generate_series(1, {{ upper_bound }})
{% endmacro %}
