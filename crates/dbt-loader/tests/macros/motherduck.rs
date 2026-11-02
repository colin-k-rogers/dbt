use std::collections::BTreeMap;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, executed_sql};

fn harness() -> MacroTestHarness {
    MacroTestHarness::for_adapter(AdapterType::MotherDuck)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("MotherDuck macro harness should build")
}

#[test]
fn dispatch_uses_motherduck_macros_without_duckdb() {
    let harness = harness();
    let batch_size = harness
        .render(
            "{{ adapter.dispatch('get_batch_size', 'dbt')() }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("MotherDuck seed batch size should resolve");
    assert_eq!(batch_size.trim(), "500");

    let timestamp = harness
        .render(
            "{{ adapter.dispatch('current_timestamp', 'dbt')() }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect("MotherDuck current_timestamp should resolve");
    assert_eq!(timestamp.trim(), "now()");

    let err = harness
        .render(
            "{{ external_location(this, {}) }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect_err("DuckDB external_location must not be inherited");
    let message = err.to_string();
    assert!(
        message.contains("external_location"),
        "expected an unknown external_location, got {message}"
    );
}

#[test]
fn create_table_as_rejects_python_models() {
    let err = harness()
        .render(
            "{{ motherduck__create_table_as(false, none, 'pass', 'python') }}",
            BTreeMap::<String, Value>::new(),
        )
        .expect_err("Python models must be rejected");
    assert!(err.to_string().contains("Python models are not supported"));
}

#[test]
fn sql_create_table_as_renders_a_regular_table() {
    let harness = harness();
    let ctx = harness
        .materialization_context("orders", "select 1 as id")
        .relation_type(RelationType::Table)
        .build();
    let rendered = harness
        .render(
            "{{ motherduck__create_table_as(false, this, compiled_code, 'sql') }}",
            ctx,
        )
        .expect("SQL create_table_as should render");
    let sql = rendered.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(sql.contains("create table"), "{rendered}");
    assert!(!sql.contains("temporary"), "{rendered}");
    assert!(sql.contains("select 1 as id"), "{rendered}");
}

#[test]
fn list_relations_uses_duckdb_functions_and_escapes_quotes() {
    let harness = harness();
    let relation = harness.relation(
        "team's data",
        "Sales'Ops",
        "orders",
        Some(RelationType::Table),
    );
    let mut ctx = harness
        .materialization_context("orders", "")
        .relation_type(RelationType::Table)
        .build();
    ctx.insert(
        "schema_relation".to_string(),
        RelationObject::new(relation).into_value(),
    );
    harness
        .render(
            "{{ motherduck__list_relations_without_caching(schema_relation) }}",
            ctx,
        )
        .expect("list_relations should render");
    let sql = executed_sql(harness.mock()).join("\n");
    assert!(sql.contains("duckdb_tables()"), "{sql}");
    assert!(sql.contains("duckdb_views()"), "{sql}");
    assert!(sql.contains("team''s data"), "{sql}");
    assert!(sql.contains("sales''ops"), "{sql}");
    assert!(!sql.contains("information_schema"), "{sql}");
}

#[test]
fn get_columns_uses_describe() {
    let harness = harness();
    let ctx = harness
        .materialization_context("orders", "")
        .relation_type(RelationType::Table)
        .build();
    harness
        .render("{{ motherduck__get_columns_in_relation(this) }}", ctx)
        .expect("get_columns_in_relation should render");
    let sql = executed_sql(harness.mock()).join("\n").to_lowercase();
    assert!(sql.contains("describe"), "{sql}");
    assert!(!sql.contains("information_schema"), "{sql}");
}

#[test]
fn staging_relation_is_persistent() {
    let harness = harness();
    harness.mock().on("get_temp_relation_path", |_| {
        Ok(Value::from_serialize(BTreeMap::from([
            ("database", Value::from("TEST_DB")),
            ("schema", Value::from("dbt_temp")),
            ("identifier", Value::from("orders__dbt_tmp")),
        ])))
    });
    let ctx = harness
        .materialization_context("orders", "")
        .relation_type(RelationType::Table)
        .build();
    let rendered = harness
        .render(
            "{{ motherduck__dispatch_temporary_relation(this).temporary }}",
            ctx,
        )
        .expect("dispatch_temporary_relation should render");
    assert_eq!(rendered.trim(), "False");
}
