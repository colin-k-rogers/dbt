use std::sync::Arc;

use arrow_array::RecordBatch;
use dbt_adapter_core::AdapterType;
use dbt_common::cancellation::CancellationToken;
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

pub(crate) fn list_schemas_sql(database: &str) -> String {
    let database = dbt_adapter_sql::ident::escape_string_literal(database, AdapterType::MotherDuck);
    let filter = if database.is_empty() {
        String::new()
    } else {
        format!(" WHERE lower(database_name) = lower('{database}')")
    };
    format!("SELECT schema_name FROM duckdb_schemas(){filter}")
}

pub(crate) fn list_schemas(
    adapter: &AdapterImpl,
    state: &State,
    database: &str,
) -> AdapterResult<Vec<String>> {
    let batch = execute_sql(adapter, state, "list_schemas", &list_schemas_sql(database))?;
    adapter.list_schemas_inner(Arc::new(batch))
}

pub(crate) fn relation_columns(
    adapter: &AdapterImpl,
    state: &State,
    relation: &dyn BaseRelation,
) -> AdapterResult<Vec<Column>> {
    let sql = format!("DESCRIBE {}", relation.render_self_as_str());
    let batch = execute_sql(adapter, state, "get_columns_in_relation", &sql)?;
    super::duckdb::columns_from_duckdb_describe(AdapterType::MotherDuck, &batch)
}

/// `schema` and `identifier` are already folded for unquoted names by the caller.
pub(crate) fn get_relation_sql(
    quoting: ResolvedQuoting,
    database: &str,
    schema: &str,
    identifier: &str,
) -> String {
    let literal =
        |value: &str| dbt_adapter_sql::ident::escape_string_literal(value, AdapterType::MotherDuck);
    let database = literal(database);
    let schema = literal(schema);
    let identifier = literal(identifier);
    let schema_filter = if quoting.schema {
        format!("schema_name = '{schema}'")
    } else {
        format!("lower(schema_name) = lower('{schema}')")
    };
    let identifier_filter = if quoting.identifier {
        format!("table_name = '{identifier}'")
    } else {
        format!("lower(table_name) = lower('{identifier}')")
    };
    let view_identifier_filter = if quoting.identifier {
        format!("view_name = '{identifier}'")
    } else {
        format!("lower(view_name) = lower('{identifier}')")
    };
    let database_filter = if database.is_empty() {
        String::new()
    } else {
        format!(" AND lower(database_name) = lower('{database}')")
    };

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
