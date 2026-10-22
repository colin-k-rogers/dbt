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

fn catalog_attach_sql(view: &DbtCatalogsView<'_>) -> AdapterResult<Vec<String>> {
    let mut seen_aliases = HashMap::new();
    let mut statements = Vec::new();

    for catalog in view
        .catalogs
        .iter()
        .filter(|catalog| catalog.catalog_type == CatalogType::MotherDuck)
    {
        let config = catalog
            .config_block("motherduck")
            .expect("validated MotherDuck catalogs have a motherduck config block");
        let database = config
            .get("database")
            .and_then(dbt_yaml::Value::as_str)
            .unwrap_or(catalog.name);
        let alias = config
            .get("catalog_database")
            .and_then(dbt_yaml::Value::as_str)
            .unwrap_or(database);
        let collision_key = alias.to_lowercase();
        if let Some(prior) = seen_aliases.insert(collision_key, catalog.name) {
            return Err(config_error(format!(
                "Catalog '{}' MotherDuck attach alias '{alias}' collides case-insensitively with catalog '{prior}'",
                catalog.name
            )));
        }

        let database =
            dbt_adapter_sql::ident::escape_string_literal(database, AdapterType::MotherDuck);
        let alias = dbt_adapter_sql::ident::quote_identifier(alias, AdapterType::MotherDuck);
        statements.push(format!("ATTACH IF NOT EXISTS 'md:{database}' AS {alias}"));
    }

    Ok(statements)
}

fn init_sql_with_catalogs(
    config: &AdapterConfig,
    catalogs: Option<&DbtCatalogsView<'_>>,
) -> AdapterResult<Vec<String>> {
    let mut statements = catalogs.map_or_else(|| Ok(Vec::new()), catalog_attach_sql)?;
    statements.extend(settings_sql(config)?);
    Ok(statements)
}

fn init_sql(config: &AdapterConfig) -> AdapterResult<Vec<String>> {
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

fn init_statement_error(index: usize) -> AdapterError {
    // A driver error may echo the SQL, including a secret setting value.
    // Keep this error deliberately independent of both the SQL and driver message.
    AdapterError::new(
        AdapterErrorKind::Driver,
        format!("MotherDuck session initialization statement {index} failed"),
    )
}

pub fn apply_init_sql(conn: &mut dyn Connection, config: &AdapterConfig) -> AdapterResult<()> {
    for (idx, sql) in init_sql(config)?.iter().enumerate() {
        let mut stmt = conn.new_statement().map_err(adbc_error_to_adapter_error)?;
        stmt.set_sql_query(sql)
            .map_err(|_| init_statement_error(idx + 1))?;
        stmt.execute_update()
            .map_err(|_| init_statement_error(idx + 1))?;
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
        let err = init_statement_error(2);
        assert_eq!(
            err.message(),
            "MotherDuck session initialization statement 2 failed"
        );
        assert!(!err.message().contains("token"));
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
                assert_eq!(
                    catalog_attach_sql(view).unwrap(),
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
                assert_eq!(
                    catalog_attach_sql(view).unwrap(),
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
                    statements,
                    vec![
                        r#"ATTACH IF NOT EXISTS 'md:analytics' AS "analytics""#,
                        r#"SET "enable_progress_bar" = 'false'"#,
                    ]
                );
            },
        );
    }
}
