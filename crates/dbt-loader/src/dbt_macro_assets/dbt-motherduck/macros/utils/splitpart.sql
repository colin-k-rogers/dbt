{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/utils/splitpart.sql. #}
{% macro motherduck__split_part(string_text, delimiter_text, part_number) %}
    string_split({{ string_text }}, {{ delimiter_text }})[ {{ part_number }} ]
{% endmacro %}
