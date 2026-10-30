/// Attach one database (`single`) or every database in the account (`workspace`).
pub const ATTACH_MODE: &str = "adbc.motherduck.attach_mode";

/// MotherDuck session name, passed through as `--session_name`.
pub const SESSION_NAME: &str = "adbc.motherduck.session_name";

/// Region whose Postgres endpoint is `pg.<region>-aws.motherduck.com`.
pub const REGION: &str = "adbc.motherduck.region";
