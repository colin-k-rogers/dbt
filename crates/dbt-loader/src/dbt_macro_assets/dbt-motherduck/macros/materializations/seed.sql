{% materialization seed, adapter='motherduck' %}

  {%- set identifier = model['alias'] -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set grant_config = config.get('grants') -%}
  {%- set agate_table = load_agate_table() -%}

  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {% set rows_affected = (agate_table.rows | length) %}
  {# Non-empty loads put inside-transaction hooks in the truncate+insert batch. #}
  {% if rows_affected == 0 %}
    {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% endif %}

  {% set create_table_sql = "" %}
  {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a view".format(old_relation.render())) }}
  {% elif exists_as_table %}
    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% else %}
    {% set create_table_sql = create_csv_table(model, agate_table) %}
  {% endif %}

  {% set code = 'CREATE' if full_refresh_mode else 'INSERT' %}
  {% set sql = "" %}
  {% if rows_affected > 0 %}
    {% set sql = load_csv_rows(model, agate_table) %}
  {% endif %}

  {% call noop_statement('main', code ~ ' ' ~ rows_affected, code, rows_affected) %}
    {{ get_csv_sql(create_table_sql, sql) }};
  {% endcall %}

  {% set target_relation = this.incorporate(type='table') %}

  {% set should_revoke = should_revoke(old_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if full_refresh_mode or not exists_as_table %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {% if rows_affected == 0 %}
    {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% endif %}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
