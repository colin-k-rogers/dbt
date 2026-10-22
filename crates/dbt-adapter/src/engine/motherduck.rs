use dbt_adapter_core::AdapterType;
use dbt_adbc::Connection;
use dbt_auth::AdapterConfig;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};

use crate::errors::adbc_error_to_adapter_error;

fn config_error(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Configuration, message)
}

fn init_sql(config: &AdapterConfig) -> AdapterResult<Vec<String>> {
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
            init_sql(&config).unwrap(),
            vec![
                r#"SET "custom""user" = 'dbt''s tests'"#,
                r#"SET "enable_progress_bar" = 'false'"#,
                r#"SET "threads" = '4'"#,
            ]
        );
    }

    #[test]
    fn init_sql_rejects_non_scalar_values_without_echoing_them() {
        let err = init_sql(&config(
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
}
