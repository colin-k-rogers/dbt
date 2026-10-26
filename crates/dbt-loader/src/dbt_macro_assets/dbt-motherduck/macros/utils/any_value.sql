{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/utils/any_value.sql. #}
{% macro motherduck__any_value(expression) -%}

    arbitrary({{ expression }})

{%- endmacro %}
