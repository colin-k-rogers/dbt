use std::borrow::Cow;

use dbt_adbc::{Backend, database};
use url::{Host, Url};

use crate::{AdapterConfig, Auth, AuthError, AuthWarningPrinter, auth_configure_pipeline};

#[derive(Debug)]
enum MotherDuckAuthIR<'a> {
    Endpoint {
        host: &'a str,
        token: &'a str,
        port: u16,
        user: &'a str,
        database: &'a str,
        sslrootcert: &'a str,
        connect_timeout: Cow<'a, str>,
    },
}

impl MotherDuckAuthIR<'_> {
    fn apply(
        self,
        mut builder: database::Builder,
        _warning_printer: &dyn AuthWarningPrinter,
    ) -> Result<database::Builder, AuthError> {
        match self {
            Self::Endpoint {
                host,
                token,
                port,
                user,
                database,
                sslrootcert,
                connect_timeout,
            } => {
                let mut uri = Url::parse("postgresql://localhost")
                    .expect("the static MotherDuck PostgreSQL URL is valid");
                uri.set_host(Some(host))
                    .map_err(|_| AuthError::config("host is not a valid hostname"))?;
                uri.set_port(Some(port))
                    .map_err(|_| AuthError::config("port must be between 1 and 65535"))?;
                uri.set_path(database);
                uri.query_pairs_mut()
                    .append_pair("sslmode", "verify-full")
                    .append_pair("sslrootcert", sslrootcert)
                    .append_pair("connect_timeout", &connect_timeout)
                    .append_pair("options", "--attach_mode=single");

                builder
                    .with_uri(uri)
                    .with_username(user)
                    .with_password(token);
            }
        }

        Ok(builder)
    }
}

fn non_empty<'a>(config: &'a AdapterConfig, field: &str) -> Result<&'a str, AuthError> {
    let value = config.require_str(field)?;
    if value.trim().is_empty() || value.contains('\0') {
        return Err(AuthError::config(format!(
            "{field} must be a non-empty string without NUL characters"
        )));
    }
    Ok(value)
}

fn parse_auth<'a>(
    config: &'a AdapterConfig,
    _warning_printer: &dyn AuthWarningPrinter,
) -> Result<MotherDuckAuthIR<'a>, AuthError> {
    let host = non_empty(config, "host")?;
    Host::parse(host).map_err(|_| AuthError::config("host is not a valid hostname"))?;

    let database = non_empty(config, "database")?;
    if database.contains('=') || database.contains("://") {
        return Err(AuthError::config(
            "database must be a database name, not a connection string",
        ));
    }

    let port = config
        .get_string("port")
        .unwrap_or(Cow::Borrowed("5432"))
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| AuthError::config("port must be between 1 and 65535"))?;

    let connect_timeout = config
        .get_string("connect_timeout")
        .unwrap_or(Cow::Borrowed("240"));
    if connect_timeout
        .parse::<u64>()
        .ok()
        .filter(|timeout| *timeout > 0)
        .is_none()
    {
        return Err(AuthError::config(
            "connect_timeout must be a positive integer",
        ));
    }

    Ok(MotherDuckAuthIR::Endpoint {
        host,
        token: non_empty(config, "token")?,
        port,
        user: match config.get_str("user") {
            Some(user) if !user.trim().is_empty() && !user.contains('\0') => user,
            Some(_) => {
                return Err(AuthError::config(
                    "user must be a non-empty string without NUL characters",
                ));
            }
            None => "postgres",
        },
        database,
        sslrootcert: match config.get_str("sslrootcert") {
            Some(sslrootcert) if !sslrootcert.trim().is_empty() && !sslrootcert.contains('\0') => {
                sslrootcert
            }
            Some(_) => {
                return Err(AuthError::config(
                    "sslrootcert must be a non-empty string without NUL characters",
                ));
            }
            None => "system",
        },
        connect_timeout,
    })
}

fn apply_connection_args(
    _config: &AdapterConfig,
    builder: database::Builder,
    _warning_printer: &dyn AuthWarningPrinter,
) -> Result<database::Builder, AuthError> {
    Ok(builder)
}

pub struct MotherDuckAuth {
    pub warning_printer: Box<dyn AuthWarningPrinter>,
}

impl MotherDuckAuth {
    pub fn new(warning_printer: Box<dyn AuthWarningPrinter>) -> Self {
        Self { warning_printer }
    }
}

impl Auth for MotherDuckAuth {
    fn backend(&self) -> Backend {
        Backend::MotherDuck
    }

    fn configure(&self, config: &AdapterConfig) -> Result<database::Builder, AuthError> {
        auth_configure_pipeline!(self, config, parse_auth, apply_connection_args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_options::uri_value;
    use dbt_test_primitives::assert_contains;
    use dbt_yaml::Mapping;

    fn config() -> AdapterConfig {
        AdapterConfig::new(Mapping::from_iter([
            ("host".into(), "pg.us-east-1-aws.motherduck.com".into()),
            ("token".into(), "secret/token".into()),
            ("database".into(), "analytics".into()),
            ("schema".into(), "main".into()),
        ]))
    }

    #[test]
    fn builds_secure_endpoint_uri_with_defaults_and_redacts_token() {
        let auth = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter));
        let builder = auth.configure(&config()).expect("configure");
        let debug = format!("{builder:?}");
        let uri = uri_value(&builder);

        assert_eq!(
            crate::auth_for_backend(Backend::MotherDuck).backend(),
            Backend::MotherDuck
        );
        assert_eq!(auth.backend(), Backend::MotherDuck);
        assert_eq!(builder.backend, Backend::MotherDuck);
        assert_contains!(
            &uri,
            "postgresql://pg.us-east-1-aws.motherduck.com:5432/analytics?"
        );
        assert_contains!(&uri, "sslmode=verify-full");
        assert_contains!(&uri, "sslrootcert=system");
        assert_contains!(&uri, "connect_timeout=240");
        assert_contains!(&uri, "options=--attach_mode%3Dsingle");
        assert_contains!(&uri, "user=postgres");
        assert_contains!(&uri, "password=secret%2Ftoken");
        assert!(!debug.contains("secret/token"));
    }

    #[test]
    fn accepts_numeric_and_string_port_and_timeout_values() {
        for (port, timeout) in [
            (dbt_yaml::Value::from(6543), dbt_yaml::Value::from(30)),
            (dbt_yaml::Value::from("6543"), dbt_yaml::Value::from("30")),
        ] {
            let mut mapping = config().repr().clone();
            mapping.insert("port".into(), port);
            mapping.insert("connect_timeout".into(), timeout);

            let builder = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
                .configure(&AdapterConfig::new(mapping))
                .expect("configure");
            let uri = uri_value(&builder);
            assert_contains!(&uri, ":6543/analytics?");
            assert_contains!(&uri, "connect_timeout=30");
        }
    }

    #[test]
    fn rejects_connection_string_as_database_name() {
        let mut mapping = config().repr().clone();
        mapping.insert("database".into(), "postgresql://example.com/db".into());
        let err = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect_err("database URI must be rejected");
        assert_eq!(
            err.msg(),
            "database must be a database name, not a connection string"
        );
    }

    #[test]
    fn rejects_invalid_port_and_timeout_values() {
        for (field, value, expected) in [
            (
                "port",
                dbt_yaml::Value::from(0),
                "port must be between 1 and 65535",
            ),
            (
                "connect_timeout",
                dbt_yaml::Value::from("soon"),
                "connect_timeout must be a positive integer",
            ),
        ] {
            let mut mapping = config().repr().clone();
            mapping.insert(field.into(), value);
            let err = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
                .configure(&AdapterConfig::new(mapping))
                .expect_err("invalid value must be rejected");
            assert_eq!(err.msg(), expected);
        }
    }
}
