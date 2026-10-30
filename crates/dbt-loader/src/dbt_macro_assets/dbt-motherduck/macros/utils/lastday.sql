{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/utils/lastday.sql. #}
{% macro motherduck__last_day(date, datepart) -%}

    {%- if datepart == 'quarter' -%}
    -- duckdb dateadd does not support quarter interval.
    cast(
        {{dbt.dateadd('day', '-1',
        dbt.dateadd('month', '3', dbt.date_trunc(datepart, date))
        )}}
        as date)
    {%- else -%}
    {{dbt.default_last_day(date, datepart)}}
    {%- endif -%}

{%- endmacro %}
