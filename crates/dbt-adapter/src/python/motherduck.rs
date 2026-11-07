//! Run MotherDuck Python models as Flights.
//!
//! A Flight is one Python file MotherDuck runs in its own container. The
//! submitted source is the model's compiled code (including `dbtObj` and
//! `materialize`) plus a generated `main()` that connects with `duckdb.connect("md:")`.
//! The lifecycle is the `MD_*_FLIGHT` SQL functions on the model's connection.
//! There is no in-process Python runtime on this adapter, so Flight is the only
//! submission method.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use arrow::array::Array;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use arrow_array::RecordBatch;
use dbt_adbc::QueryCtx;
use dbt_adapter_core::AdapterType;
use dbt_common::cancellation::CancellationToken;
use dbt_common::{AdapterError, AdapterErrorKind, AdapterResult};
use dbt_yaml::Span;
use dbt_yaml::Value as YmlValue;
use minijinja::{State, Value};
use regex::Regex;

use crate::AdapterResponse;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::connection::ConnectionGuard;
use crate::macro_exec::execute_macro_with_package;
use crate::query_ctx::query_ctx_from_state;

const TERMINAL_STATUSES: &[&str] = &["SUCCEEDED", "FAILED", "CANCELLED"];
const MAX_SOURCE_BYTES: usize = 200 * 1024;
const MAX_REQUIREMENTS_BYTES: usize = 20 * 1024;
const MAX_FLIGHT_NAME_LENGTH: usize = 200;
const CANCEL_WAIT_SEC: u64 = 60;
const DEFAULT_TIMEOUT_SEC: u64 = 3600;
const DEFAULT_POLL_SEC: u64 = 2;
const DEFAULT_DUCKDB_VERSION: &str = "1.5.5";
const DEFAULT_LOG_URL: &str = "https://app.motherduck.com/flights/{flight_id}/runs/{run_number}";
const LIST_PAGE: i64 = 200;

const FLIGHT_ENTRYPOINT: &str = r#"

# --- dbt-motherduck flight entrypoint (generated) ---
__dbt_settings = {settings}


def __dbt_apply_settings(cursor):
    for statement in __dbt_settings:
        cursor.execute(statement)


def main():
    import duckdb as _duckdb

    con = _duckdb.connect("md:")
    __dbt_apply_settings(con)

    def load_df_function(table_name):
        return con.query(f"select * from {table_name}")

    dbt = dbtObj(load_df_function)
    df = model(dbt, con)
    if isinstance(df, _duckdb.DuckDBPyRelation):
        materialize(df, con)
    else:
        write_cursor = con.cursor()
        __dbt_apply_settings(write_cursor)
        materialize(df, write_cursor)


if __name__ == "__main__":
    main()
"#;

struct FlightOptions {
    access_token_name: Option<String>,
    max_runtime_sec: Option<i64>,
    timeout_sec: u64,
    poll_interval_sec: u64,
    log_lines: i64,
    log_url_template: String,
    requirements: Vec<String>,
    duckdb_version: String,
    instance_type: Option<String>,
}

impl Default for FlightOptions {
    fn default() -> Self {
        Self {
            access_token_name: None,
            max_runtime_sec: None,
            timeout_sec: DEFAULT_TIMEOUT_SEC,
            poll_interval_sec: DEFAULT_POLL_SEC,
            log_lines: 0,
            log_url_template: DEFAULT_LOG_URL.to_string(),
            requirements: Vec::new(),
            duckdb_version: DEFAULT_DUCKDB_VERSION.to_string(),
            instance_type: None,
        }
    }
}

struct FlightSpec {
    source: String,
    requirements: String,
    instance_type: Option<String>,
    access_token_name: Option<String>,
    max_runtime_sec: Option<i64>,
}

enum Expected {
    Text(String),
    Int(i64),
}

pub fn submit_python_job(
    adapter: &AdapterImpl,
    ctx: &QueryCtx,
    conn: &mut ConnectionGuard<'_>,
    state: &State,
    model: &Value,
    compiled_code: &str,
    token: CancellationToken,
) -> AdapterResult<AdapterResponse> {
    let submission = config_string(model, "submission_method").unwrap_or_else(|| "flight".into());
    if !submission.eq_ignore_ascii_case("flight") {
        return Err(config_err(format!(
            "MotherDuck Python models only run as Flights; submission_method is '{submission}'"
        )));
    }

    let options = flight_options(adapter);
    let name = flight_name(state, model)?;
    let spec = FlightSpec {
        source: build_source(
            compiled_code,
            &crate::engine::settings_sql(adapter.engine().get_config())?,
        )?,
        requirements: build_requirements(model, &options)?,
        instance_type: instance_type(model, &options),
        access_token_name: options.access_token_name.clone(),
        max_runtime_sec: options.max_runtime_sec,
    };

    let mut cache = HashMap::new();
    let ctx = query_ctx_from_state(state).unwrap_or_else(|_| ctx.clone());
    let mut query =
        |sql: &str| execute_flight_sql(adapter, &ctx, conn, state, &token, &options, sql);
    let flight_id = upsert_flight(&mut query, &mut cache, &name, &spec, 0)?;
    let run_number = start_run(&mut query, &flight_id)?;
    tracing::debug!("Flight {name} ({flight_id}) started run {run_number}");
    let (status, exit_code) =
        await_run(&mut query, &options, &token, &flight_id, run_number, &name)?;
    if status != "SUCCEEDED" {
        return Err(runtime_err(format!(
            "Python model failed on MotherDuck Flight '{name}' (run {run_number}, status {status}, exit code {exit_code}).\n{}",
            log_pointer(&mut query, &options, &flight_id, run_number)
        )));
    }
    Ok(AdapterResponse::new()
        .with_message(format!(
            "Python model ran on MotherDuck Flight {name} ({flight_id}), run {run_number}"
        ))
        .with_code("OK"))
}

fn execute_flight_sql(
    adapter: &AdapterImpl,
    ctx: &QueryCtx,
    conn: &mut ConnectionGuard<'_>,
    state: &State,
    token: &CancellationToken,
    options: &FlightOptions,
    sql: &str,
) -> AdapterResult<Vec<Vec<Option<String>>>> {
    adapter
        .engine()
        .execute(Some(state), conn.as_mut(), ctx, sql, token.clone())
        .map_err(|err| {
            runtime_err(redact(
                &err.to_string(),
                options.access_token_name.as_deref(),
            ))
        })
        .and_then(|batch| rows_as_strings(&batch))
}

fn flight_name(state: &State, model: &Value) -> AdapterResult<String> {
    let rendered = execute_macro_with_package(
        state,
        &[model.clone()],
        "motherduck__flight_name",
        "dbt_motherduck",
    )?;
    sanitize_flight_name(rendered.as_str().unwrap_or("").trim())
}

fn sanitize_flight_name(name: &str) -> AdapterResult<String> {
    let re = Regex::new(r"[^A-Za-z0-9_.-]+").expect("flight name pattern");
    let name = re.replace_all(name, "_");
    let name = name.trim_matches(|c| c == '_' || c == '-');
    if name.is_empty() {
        return Err(config_err(
            "The flight_name macro returned an empty Flight name.",
        ));
    }
    if name.len() > MAX_FLIGHT_NAME_LENGTH {
        return Err(config_err(format!(
            "Flight name '{name}' is {} characters, over MotherDuck's {MAX_FLIGHT_NAME_LENGTH} character limit. Override the `motherduck__flight_name` macro to give this project's Flights shorter names.",
            name.len()
        )));
    }
    Ok(name.to_string())
}

fn build_source(compiled_code: &str, settings: &[String]) -> AdapterResult<String> {
    let rendered = python_list(settings);
    let entrypoint = FLIGHT_ENTRYPOINT.replace("{settings}", &rendered);
    let source = format!("{}{entrypoint}", compiled_code.trim_start());
    let size = source.len();
    if size > MAX_SOURCE_BYTES {
        return Err(config_err(format!(
            "Python model is too large to run as a MotherDuck Flight: {size} bytes exceeds the {MAX_SOURCE_BYTES} byte limit. Move the bulk of the code into a package listed in the model's `packages` config."
        )));
    }
    Ok(source)
}

fn build_requirements(model: &Value, options: &FlightOptions) -> AdapterResult<String> {
    let mut packages = vec![format!("duckdb=={}", options.duckdb_version)];
    packages.extend(options.requirements.iter().cloned());
    packages.extend(config_string_list(model, "packages"));

    let mut resolved: HashMap<String, String> = HashMap::new();
    let mut order = Vec::new();
    let mut passthrough = Vec::new();
    for package in packages {
        let entry = package.trim();
        if entry.is_empty() {
            continue;
        }
        match distribution_name(entry) {
            Some(name) => {
                if !resolved.contains_key(&name) {
                    order.push(name.clone());
                }
                resolved.insert(name, entry.to_string());
            }
            None => passthrough.push(entry.to_string()),
        }
    }
    let mut lines = passthrough;
    lines.extend(order.into_iter().filter_map(|name| resolved.remove(&name)));
    let requirements = format!("{}\n", lines.join("\n"));
    let size = requirements.len();
    if size > MAX_REQUIREMENTS_BYTES {
        return Err(config_err(format!(
            "Python model requirements are too large for a MotherDuck Flight: {size} bytes exceeds the {MAX_REQUIREMENTS_BYTES} byte limit."
        )));
    }
    Ok(requirements)
}

fn distribution_name(requirement: &str) -> Option<String> {
    let re = Regex::new(r"^([A-Za-z0-9][A-Za-z0-9._-]*)\s*(?:\[|[=<>!~;@]|$)").expect("req");
    let name = re.captures(requirement)?.get(1)?.as_str();
    let collapsed = Regex::new(r"[-_.]+")
        .expect("pep503")
        .replace_all(name, "-");
    Some(collapsed.to_lowercase())
}

fn instance_type(model: &Value, options: &FlightOptions) -> Option<String> {
    config_string(model, "instance_type")
        .or_else(|| options.instance_type.clone())
        .map(|value| value.to_ascii_uppercase())
        .filter(|value| !value.is_empty())
}

impl FlightSpec {
    fn sql_args(&self) -> String {
        self.version_fields()
            .into_iter()
            .map(|(column, expected)| match expected {
                Expected::Text(text) => format!("{column} := '{}'", escape_sql(&text)),
                Expected::Int(number) => format!("{column} := {number}"),
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn version_fields(&self) -> Vec<(&'static str, Expected)> {
        let mut fields = vec![
            ("source_code", Expected::Text(self.source.clone())),
            (
                "requirements_txt",
                Expected::Text(self.requirements.clone()),
            ),
        ];
        if let Some(token) = &self.access_token_name {
            fields.push(("access_token_name", Expected::Text(token.clone())));
        }
        if let Some(seconds) = self.max_runtime_sec {
            fields.push(("max_runtime_sec", Expected::Int(seconds)));
        }
        if let Some(instance) = &self.instance_type {
            fields.push(("instance_type", Expected::Text(instance.clone())));
        }
        fields
    }
}

fn upsert_flight(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    cache: &mut HashMap<String, String>,
    name: &str,
    spec: &FlightSpec,
    attempt: u8,
) -> AdapterResult<String> {
    let Some(flight_id) = find_flight(query, cache, name)? else {
        return create_flight(query, cache, name, spec, attempt);
    };
    if is_current(query, &flight_id, spec)? {
        tracing::debug!("MotherDuck Flight {name} is up to date; reusing it");
        return Ok(flight_id);
    }
    query(&format!(
        "SELECT flight_id FROM MD_UPDATE_FLIGHT(flight_id := '{}', {})",
        escape_sql(&flight_id),
        spec.sql_args()
    ))?;
    tracing::debug!("Updated MotherDuck Flight {name} ({flight_id})");
    Ok(flight_id)
}

fn create_flight(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    cache: &mut HashMap<String, String>,
    name: &str,
    spec: &FlightSpec,
    attempt: u8,
) -> AdapterResult<String> {
    let sql = format!(
        "SELECT flight_id FROM MD_CREATE_FLIGHT(name := '{}', {})",
        escape_sql(name),
        spec.sql_args()
    );
    match query(&sql) {
        Ok(rows) => {
            let flight_id = cell(&rows, 0, 0).ok_or_else(|| {
                runtime_err(format!(
                    "MD_CREATE_FLIGHT returned no flight_id for '{name}'"
                ))
            })?;
            cache.insert(name.to_string(), flight_id.clone());
            tracing::debug!("Created MotherDuck Flight {name} ({flight_id})");
            Ok(flight_id)
        }
        Err(err)
            if attempt == 0
                && err
                    .to_string()
                    .to_ascii_lowercase()
                    .contains("already exists") =>
        {
            cache.remove(name);
            match find_flight(query, cache, name)? {
                Some(_) => upsert_flight(query, cache, name, spec, 1),
                None => Err(runtime_err(format!(
                    "A MotherDuck Flight named '{name}' already exists but is not yours, so this model cannot run on it. Override the `motherduck__flight_name` macro to give this project's Flights a distinct name."
                ))),
            }
        }
        Err(err) => Err(err),
    }
}

fn find_flight(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    cache: &mut HashMap<String, String>,
    name: &str,
) -> AdapterResult<Option<String>> {
    if let Some(id) = cache.get(name) {
        return Ok(Some(id.clone()));
    }
    let mut offset = 0;
    loop {
        let rows = query(&format!(
            "SELECT flight_id, flight_name FROM MD_LIST_FLIGHTS(\"limit\" := {LIST_PAGE}, \"offset\" := {offset}, owner_only := true)"
        ))?;
        if rows.is_empty() {
            return Ok(None);
        }
        for row in &rows {
            if let (Some(id), Some(flight_name)) = (
                row.first().and_then(|c| c.clone()),
                row.get(1).and_then(|c| c.clone()),
            ) {
                cache.insert(flight_name, id);
            }
        }
        if let Some(id) = cache.get(name) {
            return Ok(Some(id.clone()));
        }
        if rows.len() < LIST_PAGE as usize {
            return Ok(None);
        }
        offset += LIST_PAGE;
    }
}

fn is_current(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    flight_id: &str,
    spec: &FlightSpec,
) -> AdapterResult<bool> {
    let current = query(&format!(
        "SELECT current_version FROM MD_GET_FLIGHT(flight_id := '{}')",
        escape_sql(flight_id)
    ))?;
    let Some(version) = cell(&current, 0, 0).and_then(|value| value.parse::<i64>().ok()) else {
        return Ok(false);
    };
    let expected = spec.version_fields();
    let columns = expected
        .iter()
        .map(|(column, _)| *column)
        .collect::<Vec<_>>()
        .join(", ");
    let row = query(&format!(
        "SELECT {columns} FROM MD_GET_FLIGHT_VERSION(flight_id := '{}', version_number := {version})",
        escape_sql(flight_id)
    ))?;
    let Some(actual) = row.first() else {
        return Ok(false);
    };
    if actual.len() != expected.len() {
        return Ok(false);
    }
    Ok(expected
        .iter()
        .zip(actual)
        .all(|(field, value)| values_match(value, &field.1)))
}

fn start_run(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    flight_id: &str,
) -> AdapterResult<i64> {
    let rows = query(&format!(
        "SELECT run_number FROM MD_RUN_FLIGHT(flight_id := '{}')",
        escape_sql(flight_id)
    ))?;
    cell(&rows, 0, 0)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| runtime_err("MD_RUN_FLIGHT returned no run_number"))
}

fn await_run(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    options: &FlightOptions,
    token: &CancellationToken,
    flight_id: &str,
    run_number: i64,
    name: &str,
) -> AdapterResult<(String, String)> {
    let deadline = Instant::now() + Duration::from_secs(options.timeout_sec);
    loop {
        token.check_cancellation().map_err(|_| {
            runtime_err(format!(
                "Cancelled while waiting for MotherDuck Flight '{name}'"
            ))
        })?;
        let (status, exit_code) = run_status(query, flight_id, run_number)?;
        if TERMINAL_STATUSES.contains(&status.as_str()) {
            return Ok((status, exit_code));
        }
        if Instant::now() > deadline {
            let cancelled = cancel_run(query, options, flight_id, run_number);
            let cancel_note = if cancelled {
                "The run was cancelled. "
            } else {
                "The run could not be cancelled and may still be going. "
            };
            return Err(runtime_err(format!(
                "Timed out after {}s waiting for MotherDuck Flight '{name}' run {run_number} (last status: {status}). {cancel_note}Raise `flights.timeout_sec` if the model needs longer.\n{}",
                options.timeout_sec,
                log_pointer(query, options, flight_id, run_number)
            )));
        }
        std::thread::sleep(Duration::from_secs(options.poll_interval_sec.max(1)));
    }
}

fn run_status(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    flight_id: &str,
    run_number: i64,
) -> AdapterResult<(String, String)> {
    let rows = query(&format!(
        "SELECT status, exit_code FROM MD_GET_FLIGHT_RUN(flight_id := '{}', run_number := {run_number})",
        escape_sql(flight_id)
    ))?;
    let status = cell(&rows, 0, 0).unwrap_or_else(|| "UNKNOWN".into());
    let exit_code = cell(&rows, 0, 1).unwrap_or_else(|| "null".into());
    Ok((status, exit_code))
}

fn cancel_run(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    options: &FlightOptions,
    flight_id: &str,
    run_number: i64,
) -> bool {
    if query(&format!(
        "SELECT * FROM MD_CANCEL_FLIGHT_RUN(flight_id := '{}', run_number := {run_number})",
        escape_sql(flight_id)
    ))
    .is_err()
    {
        return false;
    }
    let deadline = Instant::now() + Duration::from_secs(CANCEL_WAIT_SEC);
    while Instant::now() < deadline {
        if let Ok((status, _)) = run_status(query, flight_id, run_number)
            && TERMINAL_STATUSES.contains(&status.as_str())
        {
            return true;
        }
        std::thread::sleep(Duration::from_secs(options.poll_interval_sec.max(1)));
    }
    tracing::warn!(
        "MotherDuck Flight run {run_number} did not stop within {CANCEL_WAIT_SEC}s of being cancelled; it may leave a temp relation behind."
    );
    false
}

fn log_pointer(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    options: &FlightOptions,
    flight_id: &str,
    run_number: i64,
) -> String {
    let location = options
        .log_url_template
        .replace("{flight_id}", flight_id)
        .replace("{run_number}", &run_number.to_string());
    let mut lines = vec![format!("Logs: {location}")];
    if options.log_lines > 0 {
        lines.push(log_tail(query, flight_id, run_number, options.log_lines));
    }
    lines.join("\n")
}

fn log_tail(
    query: &mut dyn FnMut(&str) -> AdapterResult<Vec<Vec<Option<String>>>>,
    flight_id: &str,
    run_number: i64,
    limit: i64,
) -> String {
    match query(&format!(
        "SELECT line FROM MD_GET_FLIGHT_LOGS(flight_id := '{}', run_number := {run_number}, \"limit\" := {limit}, \"order\" := 'desc') ORDER BY line_number",
        escape_sql(flight_id)
    )) {
        Ok(rows) => rows
            .iter()
            .filter_map(|row| row.first().and_then(|cell| cell.clone()))
            .collect::<Vec<_>>()
            .join("\n"),
        Err(err) => format!("(could not read flight logs: {err})"),
    }
}

fn flight_options(adapter: &AdapterImpl) -> FlightOptions {
    let mut options = FlightOptions::default();
    let Some(mapping) = adapter
        .engine()
        .get_config()
        .get("flights")
        .and_then(|value| value.as_mapping())
    else {
        return options;
    };
    options.access_token_name = mapping_string(mapping, "access_token_name");
    options.max_runtime_sec = mapping_i64(mapping, "max_runtime_sec");
    if let Some(timeout) = mapping_i64(mapping, "timeout_sec") {
        options.timeout_sec = timeout.max(0) as u64;
    }
    if let Some(poll) = mapping_i64(mapping, "poll_interval_sec") {
        options.poll_interval_sec = poll.max(0) as u64;
    }
    if let Some(lines) = mapping_i64(mapping, "log_lines") {
        options.log_lines = lines.max(0);
    }
    if let Some(template) = mapping_string(mapping, "log_url_template") {
        options.log_url_template = template;
    }
    if let Some(YmlValue::Sequence(items, _)) = mapping.get(yml_key("requirements")) {
        options.requirements = items
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect();
    }
    if let Some(version) = mapping_string(mapping, "duckdb_version") {
        options.duckdb_version = version;
    }
    options.instance_type = mapping_string(mapping, "instance_type");
    options
}

fn mapping_string(mapping: &dbt_yaml::Mapping, key: &str) -> Option<String> {
    mapping
        .get(yml_key(key))
        .and_then(yaml_scalar)
        .filter(|value| !value.is_empty())
}

fn mapping_i64(mapping: &dbt_yaml::Mapping, key: &str) -> Option<i64> {
    let value = mapping.get(yml_key(key))?;
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn yaml_scalar(value: &YmlValue) -> Option<String> {
    if let Some(text) = value.as_str() {
        return Some(text.to_string());
    }
    if let Some(number) = value.as_i64() {
        return Some(number.to_string());
    }
    if value.as_bool().is_some() {
        return Some(dbt_yaml::to_string(value).ok()?.trim().to_string());
    }
    None
}

fn rows_as_strings(batch: &RecordBatch) -> AdapterResult<Vec<Vec<Option<String>>>> {
    let options = FormatOptions::default();
    let formatters = batch
        .columns()
        .iter()
        .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| runtime_err(err.to_string()))?;
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(formatters.len());
        for (col, formatter) in formatters.iter().enumerate() {
            if batch.column(col).is_null(row) {
                cells.push(None);
            } else {
                cells.push(Some(formatter.value(row).to_string()));
            }
        }
        rows.push(cells);
    }
    Ok(rows)
}

fn cell(rows: &[Vec<Option<String>>], row: usize, col: usize) -> Option<String> {
    rows.get(row)?.get(col)?.clone()
}

fn values_match(actual: &Option<String>, expected: &Expected) -> bool {
    match expected {
        Expected::Text(text) => actual.as_deref() == Some(text.as_str()),
        Expected::Int(number) => {
            actual.as_deref().and_then(|text| text.parse::<i64>().ok()) == Some(*number)
        }
    }
}

fn escape_sql(value: &str) -> String {
    dbt_adapter_sql::ident::escape_string_literal(value, AdapterType::MotherDuck)
}

fn python_list(items: &[String]) -> String {
    let body = items
        .iter()
        .map(|item| serde_json::to_string(item).unwrap_or_else(|_| "\"\"".into()))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{body}]")
}

fn yml_key(key: &str) -> YmlValue {
    YmlValue::String(key.to_string(), Span::default())
}

fn redact(message: &str, token: Option<&str>) -> String {
    match token {
        Some(token) if !token.is_empty() => message.replace(token, "***"),
        _ => message.to_string(),
    }
}

fn attr_string(model: &Value, key: &str) -> Option<String> {
    let value = model.get_attr(key).ok()?;
    let text = value.as_str()?.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

fn config_string(model: &Value, key: &str) -> Option<String> {
    let config = model.get_attr("config").ok()?;
    attr_string(&config, key)
}

fn config_string_list(model: &Value, key: &str) -> Vec<String> {
    let Ok(config) = model.get_attr("config") else {
        return Vec::new();
    };
    let Ok(value) = config.get_attr(key) else {
        return Vec::new();
    };
    value
        .try_iter()
        .ok()
        .map(|items| {
            items
                .filter_map(|item| item.as_str().map(str::trim).map(str::to_string))
                .filter(|item| !item.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn config_err(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::Configuration, message.into())
}

fn runtime_err(message: impl Into<String>) -> AdapterError {
    AdapterError::new(AdapterErrorKind::UnexpectedResult, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> FlightSpec {
        FlightSpec {
            source: "print('a')".into(),
            requirements: "duckdb==1.5.5\n".into(),
            instance_type: Some("STANDARD".into()),
            access_token_name: Some("MD_TOKEN".into()),
            max_runtime_sec: Some(30),
        }
    }

    #[test]
    fn flight_name_rejects_empty_and_overlong_names() {
        assert!(sanitize_flight_name("***").is_err());
        let long = "a".repeat(MAX_FLIGHT_NAME_LENGTH + 1);
        assert!(
            sanitize_flight_name(&long)
                .unwrap_err()
                .to_string()
                .contains("motherduck__flight_name")
        );
        assert_eq!(
            sanitize_flight_name("dbt my.model").unwrap(),
            "dbt_my.model"
        );
    }

    #[test]
    fn requirements_let_the_model_packages_win() {
        let options = FlightOptions {
            requirements: vec!["pandas==2".into(), "duckdb==9".into()],
            ..FlightOptions::default()
        };
        let model = Value::from_serialize(serde_json::json!({
            "config": {"packages": ["DuckDB==1.4.0", "pyarrow"]}
        }));
        let requirements = build_requirements(&model, &options).unwrap();
        assert!(requirements.contains("duckdb==1.4.0") || requirements.contains("DuckDB==1.4.0"));
        assert!(!requirements.contains("duckdb==9"));
        assert!(!requirements.contains("duckdb==1.5.5"));
        assert!(requirements.contains("pandas==2"));
        assert!(requirements.contains("pyarrow"));
    }

    #[test]
    fn source_escapes_into_sql_args_and_rejects_oversized_models() {
        let source = build_source("x = 'quoted'", &["SET threads = '1'".into()]).unwrap();
        assert!(source.contains("SET threads = '1'"));
        assert!(source.contains("def main():"));
        let spec = FlightSpec {
            source: "it's".into(),
            requirements: "ok\n".into(),
            instance_type: None,
            access_token_name: None,
            max_runtime_sec: None,
        };
        assert!(spec.sql_args().contains("source_code := 'it''s'"));
        assert!(build_source(&"x".repeat(MAX_SOURCE_BYTES), &[]).is_err());
    }

    #[test]
    fn upsert_reuses_an_unchanged_flight_and_adopts_a_race() {
        let spec = spec();
        let mut calls = Vec::new();
        let mut query = |sql: &str| {
            calls.push(sql.to_string());
            if sql.contains("MD_LIST_FLIGHTS") {
                Ok(vec![vec![Some("id-1".into()), Some("menu".into())]])
            } else if sql.contains("MD_GET_FLIGHT(") {
                Ok(vec![vec![Some("4".into())]])
            } else if sql.contains("MD_GET_FLIGHT_VERSION") {
                Ok(vec![vec![
                    Some(spec.source.clone()),
                    Some(spec.requirements.clone()),
                    Some("MD_TOKEN".into()),
                    Some("30".into()),
                    Some("STANDARD".into()),
                ]])
            } else if sql.contains("MD_CREATE_FLIGHT") {
                Err(runtime_err("flight already exists"))
            } else if sql.contains("MD_RUN_FLIGHT") {
                Ok(vec![vec![Some("7".into())]])
            } else if sql.contains("MD_GET_FLIGHT_RUN") {
                Ok(vec![vec![Some("SUCCEEDED".into()), Some("0".into())]])
            } else {
                Ok(vec![])
            }
        };
        let mut cache = HashMap::new();
        let id = upsert_flight(&mut query, &mut cache, "menu", &spec, 0).unwrap();
        assert_eq!(id, "id-1");
        assert!(calls.iter().any(|sql| sql.contains("MD_LIST_FLIGHTS")));
        assert!(!calls.iter().any(|sql| sql.contains("MD_UPDATE_FLIGHT")));

        calls.clear();
        cache.clear();
        let mut raced = false;
        let mut query = |sql: &str| {
            calls.push(sql.to_string());
            if sql.contains("MD_LIST_FLIGHTS") {
                if raced {
                    Ok(vec![vec![Some("id-2".into()), Some("menu".into())]])
                } else {
                    Ok(vec![])
                }
            } else if sql.contains("MD_CREATE_FLIGHT") {
                raced = true;
                Err(runtime_err("Flight already exists"))
            } else if sql.contains("MD_GET_FLIGHT(") {
                Ok(vec![vec![Some("1".into())]])
            } else if sql.contains("MD_GET_FLIGHT_VERSION") {
                Ok(vec![vec![
                    Some("old".into()),
                    Some("old".into()),
                    Some("MD_TOKEN".into()),
                    Some("30".into()),
                    Some("STANDARD".into()),
                ]])
            } else if sql.contains("MD_UPDATE_FLIGHT") {
                Ok(vec![vec![Some("id-2".into())]])
            } else {
                Ok(vec![])
            }
        };
        let id = upsert_flight(&mut query, &mut cache, "menu", &spec, 0).unwrap();
        assert_eq!(id, "id-2");
        assert!(calls.iter().any(|sql| sql.contains("MD_UPDATE_FLIGHT")));
    }

    #[test]
    fn errors_redact_the_access_token_name() {
        assert_eq!(
            redact(
                "failed for access_token_name := 'MD_TOKEN'",
                Some("MD_TOKEN")
            ),
            "failed for access_token_name := '***'"
        );
    }
}
