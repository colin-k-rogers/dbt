/// Attach one database (`single`) or every database in the account (`workspace`).
pub const ATTACH_MODE: &str = "motherduck.attach_mode";

/// MotherDuck session name, passed through as `--session_name`.
pub const SESSION_NAME: &str = "motherduck.session_name";

/// Region whose endpoint is `pg.<region>-aws.motherduck.com`.
pub const REGION: &str = "motherduck.region";

/// Whole seconds to wait while connecting. `0` waits indefinitely.
pub const CONNECT_TIMEOUT: &str = "motherduck.connect_timeout";

/// PEM file of root certificates. Unset, the driver uses its own trust store.
pub const TLS_ROOT_CERT_PATH: &str = "motherduck.tls_root_cert_path";
