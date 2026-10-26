{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/materializations/incremental.sql.
   MotherDuck uses regular tables in a staging schema, with post-commit cleanup
   and a post-model hook for failures. #}
{% materialization incremental, adapter='motherduck' %}
  {% set existing_relation = load_cached_relation(this) %}
  {% set target_relation = this.incorporate(type='table') %}
  {% set intermediate_relation = make_intermediate_relation(target_relation) %}
  {% set backup_type = existing_relation.type if existing_relation else 'table' %}
  {% set backup_relation = make_backup_relation(target_relation, backup_type) %}
  {% set full_refresh_mode = should_full_refresh() or (existing_relation and existing_relation.is_view) %}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
  {% set partitioned_by = none %}
  {% set sorted_by = none %}
  {% if existing_relation is none or full_refresh_mode %}
    {% set partitioned_by = motherduck__get_partitioned_by(target_relation, false) %}
    {% set sorted_by = motherduck__get_sorted_by(target_relation, false) %}
  {% endif %}
  {% set strategy = config.get('incremental_strategy') or 'default' %}
  {% set strategy_macro = adapter.get_incremental_strategy_macro(context, strategy) %}
  {% set grant_config = config.get('grants') %}
  {% set to_drop = [] %}

  {{ drop_relation_if_exists(load_cached_relation(intermediate_relation)) }}
  {{ drop_relation_if_exists(load_cached_relation(backup_relation)) }}
  {% set batch_id = '' %}
  {% set batch_ctx = model.get('batch') %}
  {% if batch_ctx and batch_ctx.get('event_time_start') %}
    {% set batch_id = batch_ctx.get('event_time_start') | string | replace('-', '') | replace(':', '') | replace(' ', '_') | replace('+', '_') %}
  {% endif %}
  {# shortcut: temp schema is always dbt_temp, honor meta.temp_schema_name when the adapter path does #}
  {% set temp_relation = target_relation.incorporate(path=adapter.get_temp_relation_path(target_relation, batch_id=batch_id)) %}
  {% do adapter.commit() %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(false, target_relation, sql, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(false, intermediate_relation, sql, partitioned_by=partitioned_by, sorted_by=sorted_by) %}
  {% else %}
    {% do run_query(get_create_table_as_sql(false, temp_relation, sql)) %}
    {% do to_drop.append(temp_relation) %}
    {% do adapter.expand_target_column_types(from_relation=temp_relation, to_relation=target_relation) %}
    {% set dest_columns = process_schema_changes(on_schema_change, temp_relation, existing_relation) %}
    {% if not dest_columns %}
      {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
    {% endif %}
    {% set build_sql = strategy_macro({
        'target_relation': target_relation,
        'temp_relation': temp_relation,
        'unique_key': config.get('unique_key'),
        'dest_columns': dest_columns,
        'incremental_predicates': config.get('predicates') or config.get('incremental_predicates')
    }) %}
  {% endif %}

  {% call statement('main') %}
    {{ build_sql }}
  {% endcall %}
  {% if existing_relation and full_refresh_mode %}
    {% do drop_indexes_on_relation(target_relation) %}
    {% do adapter.rename_relation(target_relation, backup_relation) %}
    {% do adapter.rename_relation(intermediate_relation, target_relation) %}
    {% do to_drop.append(backup_relation) %}
  {% endif %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke(existing_relation, full_refresh_mode)) %}
  {% if existing_relation is none or existing_relation.is_view or full_refresh_mode %}
    {% do create_indexes(target_relation) %}
  {% endif %}
  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {% for relation in to_drop %}
    {% do adapter.drop_relation(relation) %}
  {% endfor %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
