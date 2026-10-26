{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/persist_docs.sql. #}

{#
  The logic in this file is adapted from dbt-postgres, since DuckDB matches
  the Postgres relation/column commenting model as of 0.10.1
#}

{% macro motherduck__persist_docs(relation, model, for_relation, for_columns) -%}
  {# one single batch lets the connection manager pipeline all comments in the model transaction #}
  {% set comment_sql = [] %}
  {% if for_relation and config.persist_relation_docs() and model.description %}
    {% do comment_sql.append(alter_relation_comment(relation, model.description)) %}
  {% endif %}

  {% if for_columns and config.persist_column_docs() and model.columns %}
    {% do comment_sql.append(alter_column_comment(relation, model.columns)) %}
  {% endif %}

  {% set sql = comment_sql | join('\n') | trim %}
  {% if sql %}
    {% do run_query(sql) %}
  {% endif %}
{%- endmacro %}

{% macro motherduck__alter_relation_comment(relation, comment) %}
  {% set escaped_comment = duckdb_escape_comment(comment) %}
  comment on {{ relation.type }} {{ relation }} is {{ escaped_comment }};
{% endmacro %}

{% macro motherduck__alter_column_comment(relation, column_dict) %}
  {% set existing_columns = adapter.get_columns_in_relation(relation) | map(attribute="name") | list %}
  {% set column_dict = validate_doc_columns(relation, column_dict, existing_columns) %}
  {% for column_name in column_dict %}
    {% set comment = column_dict[column_name]['description'] %}
    {% set escaped_comment = duckdb_escape_comment(comment) %}
    comment on column {{ relation }}.{{ adapter.quote(column_name) if column_dict[column_name]['quote'] else column_name }} is {{ escaped_comment }};
  {% endfor %}
{% endmacro %}

{#
  By using dollar-quoting like this, users can embed anything they want into their comments
  (including nested dollar-quoting), as long as they do not use this exact dollar-quoting
  label. It would be nice to just pick a new one but eventually you do have to give up.
#}
{% macro duckdb_escape_comment(comment) -%}
  {% if comment is not string %}
    {% do exceptions.raise_compiler_error('cannot escape a non-string: ' ~ comment) %}
  {% endif %}
  {%- set magic = '$dbt_comment_literal_block$' -%}
  {%- if magic in comment -%}
    {%- do exceptions.raise_compiler_error('The string ' ~ magic ~ ' is not allowed in comments.') -%}
  {%- endif -%}
  {{ magic }}{{ comment }}{{ magic }}
{%- endmacro %}
