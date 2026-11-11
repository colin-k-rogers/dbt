use arrow_array::{RecordBatch, StringArray};
use dbt_adapter_core::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_adapter_engine::record_batch::RecordBatchExt;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::relations::base::BaseRelation;
use minijinja::State;

use crate::AdapterResult;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::column::Column;
use crate::query_ctx::{node_id_from_state, query_ctx_from_state};

fn execute_sql(
    adapter: &AdapterImpl,
    state: &State,
    description: &str,
    sql: &str,
) -> AdapterResult<RecordBatch> {
    let mut conn = adapter.borrow_tlocal_connection(Some(state), node_id_from_state(state))?;
    let ctx = query_ctx_from_state(state)?.with_desc(description);
    adapter.engine().execute(
        Some(state),
        conn.as_mut(),
        &ctx,
        sql,
        CancellationToken::never_cancels(),
    )
}

pub(crate) fn relation_columns(
    adapter: &AdapterImpl,
    state: &State,
    relation: &dyn BaseRelation,
) -> AdapterResult<Vec<Column>> {
    let sql = describe_query_sql(&relation.render_self_as_str());
    let batch = execute_sql(adapter, state, "get_columns_in_relation", &sql)?;
    columns_from_describe(&batch)
}

pub(crate) fn list_relations_sql(quoting: ResolvedQuoting, catalog: &str, schema: &str) -> String {
    let schema_filter = name_eq("schema_name", schema, quoting.schema);
    let catalog_filter = database_and(catalog);
    format!(
        "SELECT database_name AS table_catalog, schema_name AS table_schema, \
         table_name, 'BASE TABLE' AS table_type FROM duckdb_tables() \
         WHERE {schema_filter}{catalog_filter} UNION ALL \
         SELECT database_name AS table_catalog, schema_name AS table_schema, \
         view_name AS table_name, 'VIEW' AS table_type FROM duckdb_views() \
         WHERE {schema_filter}{catalog_filter}",
    )
}

/// `schema` and `identifier` are already folded for unquoted names by the caller.
pub(crate) fn get_relation_sql(
    quoting: ResolvedQuoting,
    database: &str,
    schema: &str,
    identifier: &str,
) -> String {
    let schema_filter = name_eq("schema_name", schema, quoting.schema);
    let identifier_filter = name_eq("table_name", identifier, quoting.identifier);
    let view_identifier_filter = name_eq("view_name", identifier, quoting.identifier);
    let database_filter = database_and(database);
    format!(
        "SELECT 'BASE TABLE' AS type FROM duckdb_tables() \
         WHERE {schema_filter} AND {identifier_filter}{database_filter} UNION ALL \
         SELECT 'VIEW' AS type FROM duckdb_views() \
         WHERE {schema_filter} AND {view_identifier_filter}{database_filter}",
    )
}

pub(crate) fn describe_query_sql(sql: &str) -> String {
    format!("DESCRIBE {}", sql.trim().trim_end_matches(';'))
}

pub(crate) fn columns_from_describe(describe_result: &RecordBatch) -> AdapterResult<Vec<Column>> {
    let column_names = describe_result.column_values::<StringArray>("column_name")?;
    let data_types = describe_result.column_values::<StringArray>("column_type")?;

    Ok((0..describe_result.num_rows())
        .map(|i| {
            Column::new(
                AdapterType::MotherDuck,
                column_names.value(i).to_string(),
                data_types.value(i).to_string(),
                None,
                None,
                None,
            )
        })
        .collect())
}

fn name_eq(column: &str, value: &str, quoted: bool) -> String {
    let value = dbt_adapter_sql::ident::escape_string_literal(value, AdapterType::MotherDuck);
    if quoted {
        format!("{column} = '{value}'")
    } else {
        format!("lower({column}) = lower('{value}')")
    }
}

fn database_eq(database: &str) -> String {
    if database.is_empty() {
        String::new()
    } else {
        let database =
            dbt_adapter_sql::ident::escape_string_literal(database, AdapterType::MotherDuck);
        format!("lower(database_name) = lower('{database}')")
    }
}

fn database_and(database: &str) -> String {
    let predicate = database_eq(database);
    if predicate.is_empty() {
        String::new()
    } else {
        format!(" AND {predicate}")
    }
}
