{% macro motherduck__get_binding_char() %}
  {{ return('%s') }}
{% endmacro %}

{# shortcut: flat 500 can exceed the endpoint's 65535-parameter limit past 131 columns, size the batch from the column count inside load_csv_rows if that shows up #}
{% macro motherduck__get_batch_size() %}
  {{ return(500) }}
{% endmacro %}

{% macro motherduck__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
  {% if full_refresh %}
    {{ adapter.drop_relation(old_relation) }}
    {% set sql = create_csv_table(model, agate_table) %}
  {% elif (agate_table.rows | length) == 0 %}
    {{ adapter.truncate_relation(old_relation) }}
    {% set sql = "truncate table " ~ old_relation.render() %}
  {% else %}
    {# Truncate runs in motherduck__load_csv_rows, in the same transaction as the inserts. #}
    {% set sql = "" %}
  {% endif %}
  {{ return(sql) }}
{% endmacro %}

{% macro motherduck__load_csv_rows(model, agate_table) %}
  {% set batch_size = get_batch_size() %}
  {% set cols_sql = get_seed_column_quoted_csv(model, agate_table.column_names) %}
  {% set ns = namespace(bindings=[]) %}

  {% set dml %}
    truncate table {{ this.render() }};
    {% for chunk in agate_table.rows | batch(batch_size) %}
      {% for row in chunk %}
        {% do ns.bindings.extend(row) %}
      {% endfor %}
      insert into {{ this.render() }} ({{ cols_sql }}) values
      {% for row in chunk -%}
        ({%- for column in agate_table.column_names -%}
          {{ get_binding_char() }}
          {%- if not loop.last %},{%- endif %}
        {%- endfor -%})
        {%- if not loop.last %},{%- endif %}
      {%- endfor %};
    {% endfor %}
  {% endset %}

  {% set sql = motherduck_dml_explicit_transaction(dml) %}
  {% do adapter.add_query(sql, bindings=ns.bindings, abridge_sql_log=True) %}
  {{ return(sql) }}
{% endmacro %}
