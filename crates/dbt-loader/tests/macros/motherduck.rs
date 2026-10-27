use std::collections::BTreeMap;
use std::sync::Arc;

use dbt_adapter::relation::RelationObject;
use dbt_adapter_core::AdapterType;
use dbt_jinja_utils::mock_object::MockJinjaObject;
use dbt_schemas::dbt_types::RelationType;
use minijinja::Value;

use crate::macro_test_harness::{MacroTestHarness, default_mock_config, executed_sql};

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

fn catalog_relation() -> Value {
    let relation = Arc::new(MockJinjaObject::new());
    relation.set_attr("duckdb_write_strategy", Value::from("create_as_select"));
    Value::from_dyn_object(relation)
}

fn model(language: &str) -> Value {
    Value::from_serialize(BTreeMap::from([
        ("name", Value::from("orders")),
        ("alias", Value::from("orders")),
        ("unique_id", Value::from("model.test_project.orders")),
        ("columns", Value::from(BTreeMap::<String, Value>::new())),
        ("language", Value::from(language)),
        ("compiled_code", Value::from("select 1 as id")),
    ]))
}

fn incremental_config() -> Arc<MockJinjaObject> {
    let config = default_mock_config();
    config.on("get", |args| {
        let key = args.first().and_then(Value::as_str);
        let default = args.get(1).cloned().unwrap_or(Value::UNDEFINED);
        Ok(match key {
            Some("contract") => {
                Value::from_serialize(BTreeMap::from([("enforced", Value::from(false))]))
            }
            Some("full_refresh") => Value::from(false),
            Some("on_schema_change") => Value::from("ignore"),
            Some("incremental_strategy") => Value::from("merge"),
            _ => default,
        })
    });
    config
}

fn incremental_harness(adapter_type: AdapterType, strategy_sql: &'static str) -> MacroTestHarness {
    let harness = MacroTestHarness::for_adapter(adapter_type)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");

    let existing = harness.relation(
        "TEST_DB",
        "TEST_SCHEMA",
        "orders",
        Some(RelationType::Table),
    );
    harness.mock().on("get_relation", move |_| {
        Ok(RelationObject::new(Arc::clone(&existing)).into_value())
    });
    harness.mock().on("has_feature", move |args| {
        Ok(Value::from(
            adapter_type == AdapterType::MotherDuck
                && args.first().and_then(Value::as_str) == Some("motherduck"),
        ))
    });
    harness
        .mock()
        .on("generate_unique_temporary_table_suffix", |_| {
            Ok(Value::from("__dbt_tmp"))
        });
    harness.mock().on("get_temp_relation_path", |_| {
        Ok(Value::from_serialize(BTreeMap::from([
            ("database", Value::from("TEST_DB")),
            ("schema", Value::from("dbt_temp")),
            ("identifier", Value::from("orders__dbt_tmp")),
        ])))
    });
    harness
        .mock()
        .on("build_catalog_relation", |_| Ok(catalog_relation()));
    harness.mock().on("get_columns_in_relation", |_| {
        Ok(Value::from_serialize(vec![BTreeMap::from([
            ("name", Value::from("id")),
            ("quoted", Value::from("\"id\"")),
            ("dtype", Value::from("integer")),
        ])]))
    });
    harness
        .mock()
        .on("get_incremental_strategy_macro", move |_| {
            Ok(Value::from_function(
                move |_args: &[Value]| -> Result<Value, minijinja::Error> {
                    Ok(Value::from(strategy_sql))
                },
            ))
        });
    harness.mock().on("commit", |_| Ok(Value::UNDEFINED));
    harness.mock().on("drop_relation", |_| Ok(Value::UNDEFINED));
    harness
        .mock()
        .on("expand_target_column_types", |_| Ok(Value::UNDEFINED));

    harness
}

fn render_second_incremental(adapter_type: AdapterType, strategy_sql: &'static str) -> Vec<String> {
    let harness = incremental_harness(adapter_type, strategy_sql);
    let ctx = harness
        .materialization_context("orders", "select 1 as id")
        .relation_type(RelationType::Table)
        .config(Value::from_dyn_object(incremental_config()))
        .with("model", model("sql"))
        .with("dbt_version", Value::from("2.0.0"))
        .build();

    let materialization = if adapter_type == AdapterType::MotherDuck {
        "materialization_incremental_motherduck"
    } else {
        "materialization_incremental_duckdb"
    };
    harness
        .render(&format!("{{{{ {materialization}() }}}}"), ctx)
        .unwrap_or_else(|error| panic!("incremental materialization should render: {error:?}"));
    executed_sql(harness.mock())
}

#[test]
fn non_empty_seed_uses_batched_parameterized_insert() {
    let harness = MacroTestHarness::for_adapter(AdapterType::MotherDuck)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");
    harness.mock().on("convert_datetimes_to_strs", |args| {
        Ok(args.first().cloned().unwrap_or(Value::UNDEFINED))
    });
    harness.mock().on("quote_seed_column", |args| {
        Ok(args.first().cloned().unwrap_or(Value::UNDEFINED))
    });
    harness.mock().on("add_query", |_| Ok(Value::UNDEFINED));

    let agate_table = Value::from_serialize(BTreeMap::from([
        ("column_names", Value::from_serialize(["id", "status"])),
        (
            "rows",
            Value::from_serialize([
                [Value::from(1), Value::from("placed")],
                [Value::from(2), Value::from("shipped")],
            ]),
        ),
    ]));
    let ctx = harness
        .materialization_context("orders", "")
        .relation_type(RelationType::Table)
        .with("model", model("sql"))
        .with("agate_table", agate_table)
        .build();

    let rendered = harness
        .render("{{ load_csv_rows(model, agate_table) }}", ctx)
        .expect("seed macro should render");

    let calls = harness.mock().observed_calls();
    let add_query = calls
        .to("add_query")
        .next()
        .expect("non-empty seed should execute an INSERT");
    let sql = add_query.args[0].as_str().expect("first argument is SQL");
    assert_eq!(sql.matches("%s").count(), 4, "{sql}");
    assert!(sql.to_lowercase().contains("insert into"), "{sql}");
    assert!(!sql.to_lowercase().contains("copy"), "{sql}");
    assert!(rendered.contains("%s"), "{rendered}");
    calls.assert_not_called("get_seed_file_path");
}

#[test]
fn endpoint_incremental_insert_creates_staging_relation_first() {
    let sqls = render_second_incremental(
        AdapterType::MotherDuck,
        "INSERT INTO TEST_DB.TEST_SCHEMA.orders SELECT * FROM TEST_DB.dbt_temp.orders__dbt_tmp",
    );
    let create = sqls
        .iter()
        .position(|sql| {
            sql.split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_lowercase()
                .contains("create table")
        })
        .unwrap_or_else(|| panic!("persistent staging CREATE: {sqls:?}"));
    let insert = sqls
        .iter()
        .position(|sql| sql.to_lowercase().contains("insert into test_db"))
        .expect("dependent INSERT");
    assert!(
        create < insert,
        "staging relation must exist first: {sqls:?}"
    );
    assert!(
        !sqls[insert].to_lowercase().contains("create table"),
        "{sqls:?}"
    );
}

#[test]
fn endpoint_incremental_merge_creates_staging_relation_first() {
    let sqls = render_second_incremental(
        AdapterType::MotherDuck,
        "MERGE INTO TEST_DB.TEST_SCHEMA.orders USING TEST_DB.dbt_temp.orders__dbt_tmp ON false",
    );
    let create = sqls
        .iter()
        .position(|sql| {
            sql.split_whitespace()
                .collect::<Vec<_>>()
                .join(" ")
                .to_lowercase()
                .contains("create table")
        })
        .unwrap_or_else(|| panic!("persistent staging CREATE: {sqls:?}"));
    let merge = sqls
        .iter()
        .position(|sql| sql.to_lowercase().contains("merge into"))
        .expect("dependent MERGE");
    assert!(
        create < merge,
        "staging relation must exist first: {sqls:?}"
    );
    assert!(
        !sqls[merge].to_lowercase().contains("create table"),
        "{sqls:?}"
    );
}

#[test]
fn embedded_duckdb_keeps_temp_create_and_dml_in_one_batch() {
    let sqls = render_second_incremental(
        AdapterType::DuckDB,
        "MERGE INTO TEST_DB.TEST_SCHEMA.orders USING orders__dbt_tmp ON false",
    );
    let batch = sqls
        .iter()
        .find(|sql| {
            let sql = sql.to_lowercase();
            sql.contains("create temporary table") && sql.contains("merge into")
        })
        .unwrap_or_else(|| panic!("DuckDB should execute one temp-table batch: {sqls:?}"));
    assert!(batch.contains(';'), "{batch}");
}

#[test]
fn dispatched_create_table_as_rejects_python_models() {
    let harness = MacroTestHarness::for_adapter(AdapterType::MotherDuck)
        .load_all_macros()
        .with_stub_functions()
        .build()
        .expect("harness should build");
    let ctx = harness
        .materialization_context("orders", "def model(dbt, session): pass")
        .relation_type(RelationType::Table)
        .with("model", model("python"))
        .build();

    let error = harness
        .render(
            "{{ create_table_as(false, this, compiled_code, 'python') }}",
            ctx,
        )
        .expect_err("MotherDuck endpoint must reject local Python execution");
    assert!(
        error
            .to_string()
            .contains("Python models are not supported"),
        "{error:?}"
    );
}
