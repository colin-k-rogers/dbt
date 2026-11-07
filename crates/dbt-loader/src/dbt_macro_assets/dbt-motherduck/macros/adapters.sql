{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/adapters.sql. #}

{% macro create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none, sorted_by=none) -%}
  {{ return(adapter.dispatch('create_table_as', 'dbt')(temporary, relation, compiled_code, language, partitioned_by=partitioned_by, sorted_by=sorted_by)) }}
{%- endmacro %}

{% macro motherduck__create_table_as(temporary, relation, compiled_code, language='sql', partitioned_by=none, sorted_by=none) -%}
  {%- if language == 'sql' -%}
    {% set contract_config = config.get('contract') %}
    {% if contract_config.enforced %}
      {{ get_assert_columns_equivalent(compiled_code) }}
    {% endif %}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    create {% if temporary: -%}temporary{%- endif %} table
      {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  {% if contract_config.enforced and not temporary %}
    {#-- DuckDB doesnt support constraints on temp tables --#}
    {{ get_table_columns_and_constraints() }} ;
  {% elif partitioned_by or sorted_by %}
    as (
      select * from (
        {{ compiled_code }}
      ) as model_subq
      limit 0
    );
  {% else %}
    as (
      {{ compiled_code }}
    );
  {% endif %}
  {% if partitioned_by %}
    {{ motherduck__alter_table_set_partitioned_by(relation, partitioned_by) }}
  {% endif %}
  {% if sorted_by %}
    {{ motherduck__alter_table_set_sorted_by(relation, sorted_by) }}
  {% endif %}
  {% if contract_config.enforced and not temporary %}
    insert into {{ relation }} {{ get_column_names() }} (
      {{ get_select_subquery(compiled_code) }}
    );
  {% elif partitioned_by or sorted_by %}
    insert into {{ relation }}
      select * from (
        {{ compiled_code }}
      ) as model_subq;
  {% endif %}
  {%- elif language == 'python' -%}
    {{ exceptions.raise_compiler_error('Python models are not supported by dbt-motherduck') }}
  {%- else -%}
      {% do exceptions.raise_compiler_error("motherduck__create_table_as macro didn't get supported language, it got %s" % language) %}
  {%- endif -%}
{% endmacro %}

{% macro motherduck__create_view_as(relation, sql) -%}
  {% set contract_config = config.get('contract') %}
  {% if contract_config.enforced %}
    {{ get_assert_columns_equivalent(sql) }}
  {%- endif %}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}

{% macro motherduck__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {% if adapter.is_ducklake(relation) %}
      drop {{ relation.type }} if exists {{ relation }}
    {% else %}
      drop {{ relation.type }} if exists {{ relation }} cascade
    {% endif %}
  {%- endcall %}
{% endmacro %}

{% macro motherduck__rename_sql(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  alter {{ to_relation.type }} {{ from_relation }} rename to {{ target_name }}
{%- endmacro %}

{% macro motherduck__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    {{ motherduck__rename_sql(from_relation, to_relation) }}
  {%- endcall %}
{% endmacro %}

{# One begin/commit for the backup+promote pair. A single rename is not a swap. #}
{% macro motherduck__relation_swap_sql(existing_relation, intermediate_relation, target_relation, backup_relation) -%}
  {% if existing_relation is not none %}
    {{ motherduck__rename_sql(existing_relation, backup_relation) }};
  {% endif %}
  {{ motherduck__rename_sql(intermediate_relation, target_relation) }};
{%- endmacro %}

{% macro motherduck__in_transaction_hook_sql(hooks) %}
  {% if hooks is defined and hooks %}
    {% for hook in hooks | selectattr('transaction', 'equalto', true) %}
      {% set rendered = render(hook.get('sql')) | trim %}
      {% if (rendered | length) > 0 %}
        {{ rendered }};
      {% endif %}
    {% endfor %}
  {% endif %}
{% endmacro %}

{# Same shape as snowflake_dml_explicit_transaction. Inside-transaction hooks
   live in this string: adapter.commit() does not keep a transaction open. #}
{% macro motherduck_dml_explicit_transaction(dml) %}
  {% set wrapped %}
    begin;
    {{ motherduck__in_transaction_hook_sql(pre_hooks) }}
    {{ dml }};
    {{ motherduck__in_transaction_hook_sql(post_hooks) }}
    commit;
  {% endset %}
  {{ return(wrapped) }}
{% endmacro %}

{% macro get_column_names() %}
    {%- set user_provided_columns = model['columns'] -%}
    (
    {% for i in user_provided_columns %}
      {% set col = user_provided_columns[i] %}
      {{ adapter.quote(col['name']) if col.get('quote') else col['name'] }} {{ "," if not loop.last }}
    {% endfor %}
  )
{% endmacro %}

{% macro motherduck__list_schemas(database) -%}
  {% set sql %}
    select schema_name
    from duckdb_schemas()
    {% if database is not none %}
    where lower(database_name) = '{{ database | lower | replace("'", "''") }}'
    {% endif %}
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro motherduck__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        select count(*)
        from duckdb_schemas()
        where lower(schema_name) = '{{ schema | lower | replace("'", "''") }}'
        and lower(database_name) = '{{ information_schema.database | lower | replace("'", "''") }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}

{% macro motherduck__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    {% set sql %}
        select type from duckdb_databases()
        where lower(database_name)='{{ relation.database | lower | replace("'", "''") }}'
        and type != 'duckdb'
    {% endset %}
    {% set results = run_query(sql) %}
    {% if results|length == 0 %}
        create schema if not exists {{ relation.without_identifier() }}
    {% else %}
        {% set attached_type = results.columns[0].values()[0] %}
        {% if attached_type == 'sqlite' and relation.schema != 'main' %}
            {{ exceptions.raise_compiler_error(
                "Schema must be 'main' when writing to sqlite "
                ~ "instead got " ~ relation.schema
            )}}
        {% elif attached_type not in ['sqlite', 'mysql'] %}
            create schema if not exists {{ relation.without_identifier() }}
        {% endif %}
    {% endif %}
  {%- endcall -%}
{% endmacro %}

{% macro motherduck__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {%- endcall -%}
{% endmacro %}

{% macro motherduck__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      database: '{{ schema_relation.database | replace("'", "''") }}',
      name: table_name,
      schema: schema_name,
      type: 'table'
    from duckdb_tables()
    where lower(schema_name) = '{{ schema_relation.schema | lower | replace("'", "''") }}'
      and lower(database_name) = '{{ schema_relation.database | lower | replace("'", "''") }}'

    union all

    select
      database: '{{ schema_relation.database | replace("'", "''") }}',
      name: view_name,
      schema: schema_name,
      type: 'view'
    from duckdb_views()
    where lower(schema_name) = '{{ schema_relation.schema | lower | replace("'", "''") }}'
      and lower(database_name) = '{{ schema_relation.database | lower | replace("'", "''") }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro motherduck__get_columns_in_relation(relation) -%}
  {# DESCRIBE preserves exact type strings without a global catalog scan. #}
  {% call statement('get_columns_in_relation', fetch_result=True, auto_begin=False) %}
      select
          column_name,
          column_type as data_type,
          cast(null as bigint) as character_maximum_length,
          cast(null as bigint) as numeric_precision,
          cast(null as bigint) as numeric_scale
      from (describe {% if relation.external %}select * from {% endif %}{{ relation.render() }})
  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro motherduck__get_partitioned_by(relation, temporary) -%}
  {%- if temporary -%}
    {{ return(none) }}
  {%- endif -%}

  {#- Root-level partition_by is the BigQuery config. It is not meta.partition_by. -#}
  {%- if config.get('partition_by', none) is not none -%}
    {% do exceptions.raise_compiler_error("MotherDuck does not read root-level partition_by. Set meta.partition_by for a DuckLake relation.") %}
  {%- endif -%}

  {%- set raw = config.get('meta', {}).get('partitioned_by', none) -%}
  {%- if raw is none or raw == '' -%}
    {%- set raw = config.get('meta', {}).get('partition_by', none) -%}
  {%- endif -%}
  {%- if raw is none or raw == '' -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set parts = normalize_string_or_list(raw, "partitioned_by/partition_by") -%}
  {{ return(motherduck__ducklake_column_list(relation, parts, "partitioned_by/partition_by")) }}
{%- endmacro %}

{% macro motherduck__alter_table_set_partitioned_by(relation, partitioned_by) -%}
  alter table {{ relation }} set partitioned by ({{ partitioned_by }});
{%- endmacro %}

{% macro motherduck__get_sorted_by(relation, temporary) -%}
  {%- if temporary -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set raw = config.get('meta', {}).get('sorted_by', none) -%}
  {%- if raw is none -%}
    {%- set raw = config.get('meta', {}).get('sort_by', none) -%}
  {%- endif -%}
  {%- if raw is none -%}
    {{ return(none) }}
  {%- endif -%}

  {%- set parts = normalize_string_or_list(raw, "sorted_by/sort_by") -%}
  {{ return(motherduck__ducklake_column_list(relation, parts, "sorted_by/sort_by", true)) }}
{%- endmacro %}

{% macro motherduck__ducklake_column_list(relation, parts, label, quote_identifiers=false) -%}
  {%- if parts | length == 0 -%}
    {% do exceptions.raise_compiler_error(label ~ " must contain at least one column") %}
  {%- endif -%}

  {# Skip staging relations: only the final target identifier is laid out. #}
  {%- if this is defined and config.get('materialized') in ['table', 'incremental']
      and relation.identifier and this.identifier and relation.identifier != this.identifier -%}
    {{ return(none) }}
  {%- endif -%}

  {%- if not adapter.is_ducklake(relation) -%}
    {% do exceptions.warn(label ~ " is only supported for DuckLake relations; ignoring for " ~ relation) %}
    {{ return(none) }}
  {%- endif -%}

  {%- if quote_identifiers -%}
    {%- set quoted = [] -%}
    {%- for col in parts -%}
      {%- do quoted.append(adapter.quote(col | replace('"', '""'))) -%}
    {%- endfor -%}
    {{ return(quoted | join(', ')) }}
  {%- endif -%}
  {{ return(parts | join(', ')) }}
{%- endmacro %}

{% macro motherduck__alter_table_set_sorted_by(relation, sorted_by) -%}
  alter table {{ relation }} set sorted by ({{ sorted_by }});
{%- endmacro %}

{% macro motherduck__get_incremental_default_sql(arg_dict) %}
  {% do return(get_incremental_delete_insert_sql(arg_dict)) %}
{% endmacro %}

{% macro motherduck__temp_schema() %}
  {%- set name = config.get('meta', {}).get('temp_schema_name', none) -%}
  {{ return(name if name is string and name else 'dbt_temp') }}
{% endmacro %}

{% macro motherduck__dispatch_temporary_relation(target_relation, batch_id='') %}
  {% set temp_schema = motherduck__temp_schema() %}
  {% set relation = target_relation.incorporate(path=adapter.get_temp_relation_path(target_relation, batch_id=batch_id, temp_schema=temp_schema)) %}
  {{ return({'relation': relation, 'temporary': false}) }}
{% endmacro %}

{% macro motherduck__drop_staging_relation(database, schema, identifier) %}
  {% if database and schema and identifier %}
    {% set relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') %}
    {% do adapter.drop_relation(relation) %}
  {% endif %}
{% endmacro %}

{% macro motherduck__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro motherduck__snapshot_get_time() -%}
  {{ current_timestamp() }}::timestamp
{%- endmacro %}

{% macro motherduck__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "'::timestamp" -%}
    {{ return(result) }}
{%- endmacro %}

{% macro motherduck__apply_grants(relation, grant_config, should_revoke=True) %}
    {% if grant_config %}
      {{ exceptions.warn('Grants for relations are not supported by MotherDuck') }}
    {% endif %}
{% endmacro %}

{% macro motherduck__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set comma_separated_columns = ", ".join(index_config.columns) -%}
  {%- set index_name = index_config.render(relation) -%}

  create {% if index_config.unique -%}
    unique
  {%- endif %} index
  "{{ index_name }}"
  on {{ relation }}
  ({{ comma_separated_columns }});
{%- endmacro %}

{% macro drop_indexes_on_relation(relation) -%}
  {#-- auto_begin=False on these reads so this macro can run safely after
       adapter.commit() in a materialization cleanup without opening a new
       transaction that would later get rolled back at connection release. #}
  {% call statement('get_indexes_on_relation', fetch_result=True, auto_begin=False) %}
    SELECT index_name
    FROM duckdb_indexes()
    WHERE database_name = '{{ relation.database | replace("'", "''") }}'
      AND schema_name = '{{ relation.schema | replace("'", "''") }}'
      AND table_name = '{{ relation.identifier | replace("'", "''") }}'
  {% endcall %}

  {% set results = load_result('get_indexes_on_relation').table %}
  {% for row in results %}
    {% set index_name = row[0] %}
    {% call statement('drop_index_' + loop.index|string, auto_begin=false) %}
      DROP INDEX "{{ relation.database }}"."{{ relation.schema }}"."{{ index_name }}"
    {% endcall %}
  {% endfor %}
{%- endmacro %}
