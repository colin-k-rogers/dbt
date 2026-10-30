{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/columns.sql. #}
{% macro motherduck__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}

  {% if add_columns %}
    {% for column in add_columns %}
      {% set sql -%}
         alter {{ relation.type }} {{ relation }} add column
           {{ api.Relation.create(identifier=column.name) }} {{ column.data_type }}
      {%- endset -%}
      {% do adapter.execute(sql, auto_begin=True) %}
    {% endfor %}
  {% endif %}

  {% if remove_columns %}
    {% for column in remove_columns %}
      {% set sql -%}
        alter {{ relation.type }} {{ relation }} drop column
          {{ api.Relation.create(identifier=column.name) }}
      {%- endset -%}
      {% do adapter.execute(sql, auto_begin=True) %}
    {% endfor %}
  {% endif %}

{% endmacro %}
