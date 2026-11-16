use std::collections::HashMap;

use dbt_adapter_core::AdapterType;
use dbt_adbc::Connection;
use dbt_auth::AdapterConfig;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_schemas::schemas::dbt_catalogs::{CatalogType, DbtCatalogsView};

use crate::errors::adbc_error_to_adapter_error;

fn config_error(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Configuration, message)
}

fn settings_sql(config: &AdapterConfig) -> AdapterResult<Vec<String>> {
    let Some(value) = config.get("settings") else {
        return Ok(Vec::new());
    };
    let dbt_yaml::Value::Mapping(settings, _) = value else {
        return Err(config_error("settings must be a mapping"));
    };

    settings
        .iter()
        .map(|(name, value)| {
            let Some(name) = name.as_str() else {
                return Err(config_error("setting names must be strings"));
            };
            if name.trim().is_empty() || name.contains('\0') {
                return Err(config_error(
                    "setting names must be non-empty strings without NUL characters",
                ));
            }
            let value = match value {
                dbt_yaml::Value::String(value, _) => value.clone(),
                dbt_yaml::Value::Bool(value, _) => value.to_string(),
                dbt_yaml::Value::Number(value, _) => value.to_string(),
                _ => {
                    return Err(config_error(
                        "setting values must be strings, booleans, or numbers",
                    ));
                }
            };
            let name = dbt_adapter_sql::ident::quote_identifier(name, AdapterType::MotherDuck);
            let value =
                dbt_adapter_sql::ident::escape_string_literal(&value, AdapterType::MotherDuck);
            Ok(format!("SET {name} = '{value}'"))
        })
        .collect()
}

#[derive(Debug)]
struct InitStatement {
    sql: String,
    catalog: Option<String>,
}

fn catalog_attach_sql(view: &DbtCatalogsView<'_>) -> AdapterResult<Vec<InitStatement>> {
    let mut seen_aliases = HashMap::new();
    let mut statements = Vec::new();

    for catalog in &view.catalogs {
        let (database, alias) = match catalog.catalog_type {
            CatalogType::MotherDuck => crate::catalog_relation::motherduck_attach_names(catalog)?,
            CatalogType::DuckLake => {
                crate::catalog_relation::motherduck_ducklake_attach_names(catalog)?
            }
            _ => continue,
        };
        let collision_key = alias.to_lowercase();
        if let Some(prior) = seen_aliases.insert(collision_key, catalog.name) {
            return Err(config_error(format!(
                "Catalog '{}' MotherDuck attach alias '{alias}' collides case-insensitively with catalog '{prior}'",
                catalog.name
            )));
        }

        let database =
            dbt_adapter_sql::ident::escape_string_literal(&database, AdapterType::MotherDuck);
        let alias = dbt_adapter_sql::ident::quote_identifier(&alias, AdapterType::MotherDuck);
        statements.push(InitStatement {
            sql: format!("ATTACH IF NOT EXISTS 'md:{database}' AS {alias}"),
            catalog: Some(catalog.name.to_string()),
        });
    }

    Ok(statements)
}

fn init_sql_with_catalogs(
    config: &AdapterConfig,
    catalogs: Option<&DbtCatalogsView<'_>>,
) -> AdapterResult<Vec<InitStatement>> {
    let mut statements = catalogs.map_or_else(|| Ok(Vec::new()), catalog_attach_sql)?;
    statements.extend(settings_sql(config)?.into_iter().map(|sql| InitStatement {
        sql,
        catalog: None,
    }));
    Ok(statements)
}

fn init_sql(config: &AdapterConfig) -> AdapterResult<Vec<InitStatement>> {
    if !crate::load_catalogs::fetch_use_catalogs_v2() {
        return init_sql_with_catalogs(config, None);
    }
    let Some(catalogs) = crate::load_catalogs::fetch_catalogs() else {
        return init_sql_with_catalogs(config, None);
    };
    let view = catalogs
        .view()
        .map_err(|error| config_error(error.to_string()))?;
    init_sql_with_catalogs(config, Some(&view))
}

fn init_statement_error(
    index: usize,
    catalog: Option<&str>,
    source: Option<&AdapterError>,
) -> AdapterError {
    let Some(catalog) = catalog else {
        return AdapterError::new(
            AdapterErrorKind::Driver,
            format!("MotherDuck session initialization statement {index} failed"),
        );
    };
    let detail = source.map(AdapterError::message).unwrap_or("failed");
    AdapterError::new(
        AdapterErrorKind::Driver,
        format!(
            "MotherDuck session initialization statement {index} failed for catalog '{catalog}': {detail}"
        ),
    )
}

pub fn apply_init_sql(conn: &mut dyn Connection, config: &AdapterConfig) -> AdapterResult<()> {
    for (idx, statement) in init_sql(config)?.iter().enumerate() {
        let mut stmt = conn.new_statement().map_err(adbc_error_to_adapter_error)?;
        let fail = |err| {
            init_statement_error(
                idx + 1,
                statement.catalog.as_deref(),
                Some(&adbc_error_to_adapter_error(err)),
            )
        };
        stmt.set_sql_query(&statement.sql).map_err(fail)?;
        stmt.execute_update().map_err(fail)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(yaml: &str) -> AdapterConfig {
        let value: dbt_yaml::Value = dbt_yaml::from_str(yaml).unwrap();
        let dbt_yaml::Value::Mapping(mapping, _) = value else {
            panic!("expected mapping");
        };
        AdapterConfig::new(mapping)
    }

    #[test]
    fn init_sql_quotes_names_and_escapes_scalar_values() {
        let config = config(
            r#"
settings:
  custom"user: "dbt's tests"
  enable_progress_bar: false
  threads: 4
"#,
        );

        assert_eq!(
            settings_sql(&config).unwrap(),
            vec![
                r#"SET "custom""user" = 'dbt''s tests'"#,
                r#"SET "enable_progress_bar" = 'false'"#,
                r#"SET "threads" = '4'"#,
            ]
        );
    }

    #[test]
    fn init_sql_rejects_non_scalar_values_without_echoing_them() {
        let err = settings_sql(&config(
            r#"
settings:
  motherduck_token: [super-secret]
"#,
        ))
        .unwrap_err();

        assert_eq!(
            err.message(),
            "setting values must be strings, booleans, or numbers"
        );
        assert!(!err.message().contains("super-secret"));
    }

    #[test]
    fn execution_errors_do_not_echo_secret_sql_or_driver_errors() {
        let driver = AdapterError::new(AdapterErrorKind::Driver, "SET token = 'secret'");
        let err = init_statement_error(2, None, Some(&driver));
        assert_eq!(
            err.message(),
            "MotherDuck session initialization statement 2 failed"
        );
        assert!(!err.message().contains("token"));

        let driver = AdapterError::new(
            AdapterErrorKind::Driver,
            "Catalog Error: database 'missing' does not exist",
        );
        let err = init_statement_error(1, Some("reporting"), Some(&driver));
        assert_eq!(
            err.message(),
            "MotherDuck session initialization statement 1 failed for catalog 'reporting': Catalog Error: database 'missing' does not exist"
        );
    }

    fn with_catalogs(yaml: &str, f: impl FnOnce(&DbtCatalogsView<'_>)) {
        use dbt_schemas::schemas::dbt_catalogs_deprecated::DbtCatalogs;

        let value: dbt_yaml::Value = dbt_yaml::from_str(yaml).unwrap();
        let dbt_yaml::Value::Mapping(mapping, span) = value else {
            panic!("expected mapping");
        };
        let catalogs = DbtCatalogs::new(mapping, span);
        let view = catalogs.view().unwrap();
        f(&view);
    }

    #[test]
    fn catalog_attach_sql_applies_defaults_and_aliases() {
        with_catalogs(
            r#"
catalogs:
  - name: analytics
    type: motherduck
    table_format: default
    config:
      motherduck: {}
  - name: reporting
    type: motherduck
    table_format: default
    config:
      motherduck:
        database: source_db
        catalog_database: routed_db
  - name: source_only
    type: motherduck
    table_format: default
    config:
      motherduck:
        database: source_db_only
  - name: alias_only
    type: motherduck
    table_format: default
    config:
      motherduck:
        catalog_database: routed_db_only
"#,
            |view| {
                let statements = catalog_attach_sql(view).unwrap();
                assert_eq!(
                    statements
                        .iter()
                        .map(|statement| statement.sql.as_str())
                        .collect::<Vec<_>>(),
                    vec![
                        r#"ATTACH IF NOT EXISTS 'md:analytics' AS "analytics""#,
                        r#"ATTACH IF NOT EXISTS 'md:source_db' AS "routed_db""#,
                        r#"ATTACH IF NOT EXISTS 'md:source_db_only' AS "source_db_only""#,
                        r#"ATTACH IF NOT EXISTS 'md:alias_only' AS "routed_db_only""#,
                    ]
                );
            },
        );
    }

    #[test]
    fn catalog_attach_sql_escapes_database_and_alias() {
        with_catalogs(
            r#"
catalogs:
  - name: reporting
    type: motherduck
    table_format: default
    config:
      motherduck:
        database: "team's data"
        catalog_database: 'route"db'
"#,
            |view| {
                let statements = catalog_attach_sql(view).unwrap();
                assert_eq!(
                    statements
                        .iter()
                        .map(|statement| statement.sql.as_str())
                        .collect::<Vec<_>>(),
                    vec![r#"ATTACH IF NOT EXISTS 'md:team''s data' AS "route""db""#]
                );
            },
        );
    }

    #[test]
    fn catalog_attach_sql_rejects_case_insensitive_alias_collisions() {
        with_catalogs(
            r#"
catalogs:
  - name: first
    type: motherduck
    table_format: default
    config:
      motherduck:
        catalog_database: Reporting
  - name: second
    type: motherduck
    table_format: default
    config:
      motherduck:
        catalog_database: reporting
"#,
            |view| {
                let err = catalog_attach_sql(view).unwrap_err();
                assert_eq!(err.kind(), AdapterErrorKind::Configuration);
                assert!(err.message().contains("collides case-insensitively"));
                assert!(err.message().contains("first"));
                assert!(err.message().contains("second"));
            },
        );
    }

    #[test]
    fn catalog_attach_sql_attaches_ducklake_account_databases() {
        with_catalogs(
            r#"
catalogs:
  - name: hot_dog_lake
    type: ducklake
    table_format: default
    config:
      duckdb:
        metadata_path: dbt_md_lake
  - name: analytics
    type: motherduck
    table_format: default
    config:
      motherduck:
        database: warehouse
"#,
            |view| {
                let statements = catalog_attach_sql(view).unwrap();
                assert_eq!(
                    statements
                        .iter()
                        .map(|statement| statement.sql.as_str())
                        .collect::<Vec<_>>(),
                    vec![
                        r#"ATTACH IF NOT EXISTS 'md:dbt_md_lake' AS "hot_dog_lake""#,
                        r#"ATTACH IF NOT EXISTS 'md:warehouse' AS "warehouse""#,
                    ]
                );
            },
        );
    }

    #[test]
    fn catalog_attach_sql_ignores_absent_and_non_motherduck_catalogs() {
        assert!(
            init_sql_with_catalogs(&config("{}"), None)
                .unwrap()
                .is_empty()
        );
        with_catalogs(
            r#"
catalogs:
  - name: local
    type: local_filesystem
    table_format: default
    config:
      duckdb:
        root_path: /tmp/data
"#,
            |view| assert!(catalog_attach_sql(view).unwrap().is_empty()),
        );
    }

    #[test]
    fn catalog_attachments_precede_session_settings() {
        with_catalogs(
            r#"
catalogs:
  - name: analytics
    type: motherduck
    table_format: default
    config:
      motherduck: {}
"#,
            |view| {
                let statements = init_sql_with_catalogs(
                    &config(
                        r#"
settings:
  enable_progress_bar: false
"#,
                    ),
                    Some(view),
                )
                .unwrap();
                assert_eq!(
                    statements
                        .iter()
                        .map(|statement| statement.sql.as_str())
                        .collect::<Vec<_>>(),
                    vec![
                        r#"ATTACH IF NOT EXISTS 'md:analytics' AS "analytics""#,
                        r#"SET "enable_progress_bar" = 'false'"#,
                    ]
                );
            },
        );
    }
}
