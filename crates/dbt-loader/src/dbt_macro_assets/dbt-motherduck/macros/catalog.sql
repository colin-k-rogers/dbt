{# Adapted from dbt-duckdb commit acdab5dfc85715572bbf43fcf2b318a5ed1bfa16,
   dbt/include/duckdb/macros/catalog.sql. #}

{% macro motherduck__get_catalog(information_schema, schemas) -%}
  {% set query %}
    with tables as (
      {{ motherduck__get_catalog_tables_sql(information_schema) }}
      {{ motherduck__get_catalog_schemas_where_clause_sql(schemas) }}
    ),
    columns as (
      {{ motherduck__get_catalog_columns_sql(information_schema) }}
      {{ motherduck__get_catalog_schemas_where_clause_sql(schemas) }}
    )
    {{ motherduck__get_catalog_results_sql() }}
  {% endset %}

  {{ return(run_query(query)) }}
{%- endmacro %}


{% macro motherduck__get_catalog_relations(information_schema, relations) -%}
  {% set query %}
    with tables as (
      {{ motherduck__get_catalog_tables_sql(information_schema) }}
      {{ motherduck__get_catalog_relations_where_clause_sql(relations) }}
    ),
    columns as (
      {{ motherduck__get_catalog_columns_sql(information_schema) }}
      {{ motherduck__get_catalog_relations_where_clause_sql(relations) }}
    )
    {{ motherduck__get_catalog_results_sql() }}
  {% endset %}

  {{ return(run_query(query)) }}
{%- endmacro %}


{% macro motherduck__get_catalog_tables_sql(information_schema) -%}
    select *
    from (
        select
            table_database: t.database_name,
            table_schema: t.schema_name,
            table_name: t.table_name,
            table_type: 'BASE TABLE',
            table_comment: t.comment
        from duckdb_tables() t
        where upper(t.database_name) = upper('{{ information_schema.database | replace("'", "''") }}')

        union all

        select
            table_database: v.database_name,
            table_schema: v.schema_name,
            table_name: v.view_name,
            table_type: 'VIEW',
            table_comment: v.comment
        from duckdb_views() v
        where upper(v.database_name) = upper('{{ information_schema.database | replace("'", "''") }}')
    ) catalog_tables
{%- endmacro %}


{% macro motherduck__get_catalog_columns_sql(information_schema) -%}
    {# duckdb_columns() exposes column comments, which DESCRIBE omits. #}
    select *
    from (
        select
            table_database: c.database_name,
            table_schema: c.schema_name,
            table_name: c.table_name,
            column_name: c.column_name,
            column_index: c.column_index,
            column_type: c.data_type,
            column_comment: c.comment
        from duckdb_columns() c
        where upper(c.database_name) = upper('{{ information_schema.database | replace("'", "''") }}')
    ) catalog_columns
{%- endmacro %}


{% macro motherduck__get_catalog_results_sql() -%}
    select
        tables.table_database,
        tables.table_schema,
        tables.table_name,
        tables.table_type,
        tables.table_comment,
        columns.column_name,
        columns.column_index,
        columns.column_type,
        columns.column_comment,
        null as table_owner
    from tables
    join columns using (table_database, table_schema, table_name)
    order by table_schema, table_name, column_index
{%- endmacro %}


{% macro motherduck__get_catalog_schemas_where_clause_sql(schemas) -%}
    {% if schemas | length == 0 %}
        where 1 = 0
    {% else %}
        where (
            {%- for schema in schemas -%}
                upper(table_schema) = upper('{{ schema | replace("'", "''") }}'){% if not loop.last %} or {% endif %}
            {%- endfor -%}
        )
    {% endif %}
{%- endmacro %}


{% macro motherduck__get_catalog_relations_where_clause_sql(relations) -%}
    {% if relations | length == 0 %}
        where 1 = 0
    {% else %}
        where (
            {%- for relation in relations -%}
                {% if not relation.schema %}
                    {% do exceptions.raise_compiler_error(
                        '`get_catalog_relations` requires a list of relations, each with a schema'
                    ) %}
                {% endif %}

                (
                    upper(table_schema) = upper('{{ relation.schema | replace("'", "''") }}')
                    {%- if relation.identifier %}
                    and upper(table_name) = upper('{{ relation.identifier | replace("'", "''") }}')
                    {%- endif %}
                ){% if not loop.last %} or {% endif %}
            {%- endfor -%}
        )
    {% endif %}
{%- endmacro %}
