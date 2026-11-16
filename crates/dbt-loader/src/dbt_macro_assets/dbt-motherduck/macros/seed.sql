{% macro motherduck__get_binding_char() %}
  {{ return('%s') }}
{% endmacro %}

{# shortcut: flat 500 can exceed the endpoint's 65535-parameter limit past 131 columns, size the batch from the column count inside load_csv_rows if that shows up #}
{% macro motherduck__get_batch_size() %}
  {{ return(500) }}
{% endmacro %}

{% macro motherduck__load_csv_rows(model, agate_table) %}
  {{ return(default__load_csv_rows(model, agate_table)) }}
{% endmacro %}
