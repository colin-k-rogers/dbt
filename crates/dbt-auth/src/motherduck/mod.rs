use std::borrow::Cow;

use dbt_adbc::{Backend, database};
use url::{Host, Url};

use crate::{AdapterConfig, Auth, AuthError, AuthWarningPrinter, auth_configure_pipeline};

#[derive(Debug)]
enum MotherDuckLocation<'a> {
    Host(&'a str),
    Region(&'a str),
}

#[derive(Debug)]
enum MotherDuckAuthIR<'a> {
    Endpoint {
        location: MotherDuckLocation<'a>,
        token: Option<&'a str>,
        port: u16,
        database: &'a str,
        sslrootcert: Option<&'a str>,
        connect_timeout: Cow<'a, str>,
        session_name: Option<&'a str>,
        attach_mode: &'a str,
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
                location,
                token,
                port,
                database,
                sslrootcert,
                connect_timeout,
                session_name,
                attach_mode,
            } => {
                let (mut uri, region) = endpoint_uri(&location, port, database)?;
                {
                    let mut query = uri.query_pairs_mut();
                    query
                        .append_pair("sslmode", "verify-full")
                        .append_pair("connect_timeout", &connect_timeout);
                    if let Some(sslrootcert) = sslrootcert {
                        query.append_pair("sslrootcert", sslrootcert);
                    }
                    // A hostless URL cannot carry a port in its authority.
                    if region.is_some() {
                        query.append_pair("port", &port.to_string());
                    }
                }

                // Token is the password option, not a URI query value.
                // https://github.com/adbc-drivers/motherduck/pull/36
                builder.with_uri(uri);
                if let Some(token) = token {
                    builder.with_password(token);
                }
                builder.with_named_option(dbt_adbc::motherduck::ATTACH_MODE, attach_mode)?;
                if let Some(region) = region {
                    builder.with_named_option(dbt_adbc::motherduck::REGION, region)?;
                }
                if let Some(session_name) = session_name {
                    builder.with_named_option(dbt_adbc::motherduck::SESSION_NAME, session_name)?;
                }
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

fn endpoint_uri<'a>(
    location: &MotherDuckLocation<'a>,
    port: u16,
    database: &str,
) -> Result<(Url, Option<&'a str>), AuthError> {
    let (mut uri, region) = match location {
        MotherDuckLocation::Host(host) => {
            let mut uri = Url::parse("postgresql://localhost")
                .expect("the static MotherDuck PostgreSQL URL is valid");
            uri.set_host(Some(host))
                .map_err(|_| AuthError::config("host is not a valid hostname"))?;
            uri.set_port(Some(port))
                .map_err(|_| AuthError::config("port must be between 1 and 65535"))?;
            (uri, None)
        }
        MotherDuckLocation::Region(region) => (
            Url::parse("postgresql:///")
                .expect("the static MotherDuck PostgreSQL URL is valid"),
            Some(*region),
        ),
    };
    uri.set_path(database);
    Ok((uri, region))
}

fn optional_non_empty<'a>(
    config: &'a AdapterConfig,
    field: &str,
) -> Result<Option<&'a str>, AuthError> {
    match config.get_str(field) {
        Some(value) if !value.trim().is_empty() && !value.contains('\0') => Ok(Some(value)),
        Some(_) => Err(AuthError::config(format!(
            "{field} must be a non-empty string without NUL characters"
        ))),
        None => Ok(None),
    }
}

fn valid_region(region: &str) -> bool {
    !region.is_empty()
        && region
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_session_name(name: &str) -> bool {
    !name.is_empty() && !name.chars().any(|ch| ch.is_whitespace() || ch == '\\')
}

fn parse_auth<'a>(
    config: &'a AdapterConfig,
    _warning_printer: &dyn AuthWarningPrinter,
) -> Result<MotherDuckAuthIR<'a>, AuthError> {
    if config.contains_key("user") || config.contains_key("username") {
        return Err(AuthError::config(
            "MotherDuck profiles do not accept `user` or `username`",
        ));
    }
    let host = optional_non_empty(config, "host")?;
    if let Some(host) = host {
        Host::parse(host).map_err(|_| AuthError::config("host is not a valid hostname"))?;
    }
    let region = match optional_non_empty(config, "region")? {
        Some(region) if valid_region(region) => Some(region),
        Some(_) => {
            return Err(AuthError::config(
                "region must contain only lowercase letters, digits, and dashes",
            ));
        }
        None => None,
    };
    let location = match (host, region) {
        (Some(host), None) => MotherDuckLocation::Host(host),
        (None, Some(region)) => MotherDuckLocation::Region(region),
        (Some(_), Some(_)) => {
            return Err(AuthError::config("set host or region, not both"));
        }
        (None, None) => return Err(AuthError::config("host or region is required")),
    };
    let session_name = match optional_non_empty(config, "session_name")? {
        Some(session_name) if valid_session_name(session_name) => Some(session_name),
        Some(_) => {
            return Err(AuthError::config(
                "session_name cannot contain whitespace or backslashes",
            ));
        }
        None => None,
    };

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

    auth_method(config)?;
    Ok(MotherDuckAuthIR::Endpoint {
        location,
        token: optional_non_empty(config, "token")?,
        session_name,
        port,
        database,
        sslrootcert: optional_non_empty(config, "sslrootcert")?,
        connect_timeout,
        attach_mode: attach_mode(config)?,
    })
}

fn attach_mode(config: &AdapterConfig) -> Result<&str, AuthError> {
    match config.get_str("attach_mode").unwrap_or("single") {
        mode @ ("single" | "workspace") => Ok(mode),
        other => Err(AuthError::config(format!(
            "MotherDuck attach_mode '{other}' is not supported. Use 'single' or 'workspace'."
        ))),
    }
}

fn auth_method(config: &AdapterConfig) -> Result<(), AuthError> {
    match config
        .get_str("method")
        .unwrap_or("token")
        .to_ascii_lowercase()
        .as_str()
    {
        "token" => Ok(()),
        "externalbrowser" => Err(AuthError::config(
            "MotherDuck externalbrowser authentication is not implemented yet.",
        )),
        other => Err(AuthError::config(format!(
            "MotherDuck authentication method '{other}' is not supported. Use 'token' or 'externalbrowser'."
        ))),
    }
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
        assert_eq!(builder.username, None);
        assert_eq!(builder.password.as_deref(), Some("secret/token"));
        assert_contains!(
            &uri,
            "postgresql://pg.us-east-1-aws.motherduck.com:5432/analytics?"
        );
        assert_contains!(&uri, "sslmode=verify-full");
        assert!(!uri.contains("sslrootcert="));
        assert_contains!(&uri, "connect_timeout=240");
        assert!(!uri.contains("user="));
        assert!(!uri.contains("password"));
        assert!(!uri.contains("options="));
        assert!(named_option(
            &builder,
            dbt_adbc::motherduck::ATTACH_MODE,
            "single"
        ));
        assert!(!debug.contains("secret/token"));
    }

    #[test]
    fn region_omits_the_host_and_sends_driver_options() {
        let mut mapping = config().repr().clone();
        mapping.remove("host");
        mapping.insert("region".into(), "us-east-1".into());
        mapping.insert("session_name".into(), "dbt-writer".into());

        let builder = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect("configure");
        let uri = uri_value(&builder);

        assert_contains!(&uri, "postgresql:///analytics?");
        assert_contains!(&uri, "port=5432");
        assert!(!uri.contains("pg.us-east-1-aws.motherduck.com"));
        assert_eq!(builder.username, None);
        assert!(named_option(
            &builder,
            dbt_adbc::motherduck::REGION,
            "us-east-1"
        ));
        assert!(named_option(
            &builder,
            dbt_adbc::motherduck::SESSION_NAME,
            "dbt-writer"
        ));
    }

    #[test]
    fn omitted_token_is_left_for_the_driver() {
        let mut mapping = config().repr().clone();
        mapping.remove("token");
        let builder = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect("configure");
        assert!(builder.password.is_none());
    }

    #[test]
    fn rejects_host_and_region_together_and_invalid_options() {
        for (field, value, expected) in [
            ("region", "us-east-1", "set host or region, not both"),
            (
                "session_name",
                "dbt writer",
                "session_name cannot contain whitespace or backslashes",
            ),
        ] {
            let mut mapping = config().repr().clone();
            mapping.insert(field.into(), value.into());
            let err = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
                .configure(&AdapterConfig::new(mapping))
                .expect_err("invalid connection option");
            assert_eq!(err.msg(), expected);
        }

        let mut mapping = config().repr().clone();
        mapping.remove("host");
        mapping.insert("region".into(), "US-EAST-1".into());
        let err = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect_err("uppercase region");
        assert_eq!(
            err.msg(),
            "region must contain only lowercase letters, digits, and dashes"
        );
    }

    fn named_option(builder: &database::Builder, name: &str, expected: &str) -> bool {
        builder.other.iter().any(|(key, value)| {
            matches!(
                (key, value),
                (
                    adbc_core::options::OptionDatabase::Other(option),
                    adbc_core::options::OptionValue::String(value),
                ) if option == name && value == expected
            )
        })
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

    #[test]
    fn explicit_token_method_sends_the_password_option() {
        let mut mapping = config().repr().clone();
        mapping.insert("method".into(), "token".into());
        let builder = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect("token method");
        assert_eq!(builder.password.as_deref(), Some("secret/token"));
    }

    #[test]
    fn externalbrowser_is_recognized_and_not_implemented() {
        let mut mapping = config().repr().clone();
        mapping.insert("method".into(), "externalbrowser".into());
        let err = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect_err("externalbrowser is a stub");
        assert_eq!(
            err.msg(),
            "MotherDuck externalbrowser authentication is not implemented yet."
        );
    }

    #[test]
    fn unknown_method_is_rejected() {
        let mut mapping = config().repr().clone();
        mapping.insert("method".into(), "oauth".into());
        let err = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect_err("unknown method");
        assert!(err.msg().contains("not supported"), "{}", err.msg());
    }

    #[test]
    fn username_is_rejected() {
        for field in ["user", "username"] {
            let mut mapping = config().repr().clone();
            mapping.insert(field.into(), "analyst".into());
            let err = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
                .configure(&AdapterConfig::new(mapping))
                .expect_err("username must be rejected");
            assert_eq!(
                err.msg(),
                "MotherDuck profiles do not accept `user` or `username`"
            );
        }
    }

    #[test]
    fn sslrootcert_is_sent_only_when_set() {
        let mut mapping = config().repr().clone();
        mapping.insert("sslrootcert".into(), "/certs/root.crt".into());
        let builder = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect("sslrootcert");
        assert_contains!(&uri_value(&builder), "sslrootcert=%2Fcerts%2Froot.crt");
    }

    #[test]
    fn attach_mode_defaults_to_single_and_accepts_workspace() {
        let builder = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&config())
            .expect("default attach mode");
        assert!(named_option(
            &builder,
            dbt_adbc::motherduck::ATTACH_MODE,
            "single"
        ));

        let mut mapping = config().repr().clone();
        mapping.insert("attach_mode".into(), "workspace".into());
        let builder = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect("workspace");
        assert!(named_option(
            &builder,
            dbt_adbc::motherduck::ATTACH_MODE,
            "workspace"
        ));

        let mut mapping = config().repr().clone();
        mapping.insert("attach_mode".into(), "SINGLE".into());
        let err = MotherDuckAuth::new(Box::new(crate::NoopAuthWarningPrinter))
            .configure(&AdapterConfig::new(mapping))
            .expect_err("invalid attach mode");
        assert_eq!(
            err.msg(),
            "MotherDuck attach_mode 'SINGLE' is not supported. Use 'single' or 'workspace'."
        );
    }
}
