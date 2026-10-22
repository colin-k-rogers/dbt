{% macro motherduck__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {% if language != 'sql' %}
    {% do exceptions.raise_compiler_error(
      "MotherDuck endpoint connections do not support Python models or other local client workflows. Use a SQL model."
    ) %}
  {% endif %}
  {{ return(duckdb__create_table_as(temporary, relation, compiled_code, language)) }}
{%- endmacro %}
