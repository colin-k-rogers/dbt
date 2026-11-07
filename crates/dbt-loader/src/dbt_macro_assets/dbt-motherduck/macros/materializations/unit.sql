{# Adapted from dbt-adapters 1.24.5, dbt/include/global_project/macros/materializations/tests/unit.sql. #}
{%- materialization unit, adapter='motherduck' -%}

  {% set relations = [] %}
  {% set sql_header = config.get('sql_header') %}

  {% set expected_rows = config.get('expected_rows') %}
  {% set expected_sql = config.get('expected_sql') %}

  {# The endpoint rejects temporary tables; DESCRIBE also preserves nested column types. #}
  {% if sql_header %}
    {# Run once before DESCRIBE, inside the same transaction as the test query. #}
    {% call statement('sql_header', auto_begin=True) %}
      {{ sql_header }}
    {% endcall %}
  {% endif %}
  {% set description = run_query('DESCRIBE (' ~ get_empty_subquery_sql(sql) ~ ')') %}
  {% set columns_in_relation = [] %}
  {% for row in description %}
    {% do columns_in_relation.append(api.Column(row[0], row[1])) %}
  {% endfor %}
  {% set tested_expected_column_names = expected_rows[0].keys() if (expected_rows | length) > 0 else columns_in_relation | map(attribute='name') | list %}
  {%- set column_name_to_data_types = {} -%}
  {%- set column_name_to_quoted = {} -%}
  {%- for column in columns_in_relation -%}
  {%-   do column_name_to_data_types.update({column.name|lower: column.data_type}) -%}
  {%-   do column_name_to_quoted.update({column.name|lower: column.quoted}) -%}
  {%- endfor -%}

  {%- set expected_column_names_quoted = [] -%}
  {%- for column_name in tested_expected_column_names -%}
  {%-   do expected_column_names_quoted.append(column_name_to_quoted[column_name|lower]) -%}
  {%- endfor -%}

  {% if not expected_sql %}
  {%   set expected_sql = get_expected_sql(expected_rows, column_name_to_data_types, column_name_to_quoted) %}
  {% endif %}
  {% set unit_test_sql = get_unit_test_sql(sql, expected_sql, expected_column_names_quoted) %}

  {% call statement('main', fetch_result=True) -%}

    {{ unit_test_sql }}

  {%- endcall %}


  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
