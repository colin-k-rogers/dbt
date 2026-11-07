use std::collections::BTreeMap;

use minijinja::Value;
use uuid::Uuid;

struct Staging {
    database: String,
    schema: String,
    identifier: String,
}

thread_local! {
    static STAGING: std::cell::RefCell<Option<Staging>> = const { std::cell::RefCell::new(None) };
}

pub(crate) fn staging_identifier(identifier: &str, batch_id: &str, suffix: &str) -> String {
    if batch_id.is_empty() {
        format!("{identifier}__{suffix}")
    } else {
        format!("{identifier}__{suffix}__{batch_id}")
    }
}

fn record_staging(database: &str, schema: &str, identifier: &str) {
    STAGING.with(|slot| {
        *slot.borrow_mut() = Some(Staging {
            database: database.to_owned(),
            schema: schema.to_owned(),
            identifier: identifier.to_owned(),
        });
    });
}

pub fn clear_staging() {
    STAGING.with(|slot| *slot.borrow_mut() = None);
}

pub fn take_staging() -> Option<(String, String, String)> {
    STAGING.with(|slot| {
        slot.borrow_mut()
            .take()
            .map(|staging| (staging.database, staging.schema, staging.identifier))
    })
}

pub fn temp_relation_path(
    database: &str,
    identifier: &str,
    batch_id: &str,
    temp_schema: &str,
) -> BTreeMap<String, Value> {
    let schema = if temp_schema.is_empty() {
        "dbt_temp"
    } else {
        temp_schema
    };
    let suffix_owned = Uuid::new_v4().to_string();
    let suffix = suffix_owned.rsplit('-').next().unwrap_or("tmp");
    let staging_identifier = staging_identifier(identifier, batch_id, suffix);
    record_staging(database, schema, &staging_identifier);
    BTreeMap::from([
        ("database".to_owned(), Value::from(database)),
        ("schema".to_owned(), Value::from(schema)),
        ("identifier".to_owned(), Value::from(staging_identifier)),
    ])
}

pub fn include_temp_schema(
    catalog_schemas: &mut BTreeMap<String, BTreeMap<String, String>>,
) {
    for (catalog, schemas) in catalog_schemas.iter_mut() {
        if catalog.is_empty() {
            continue;
        }
        schemas
            .entry("dbt_temp".to_owned())
            .or_insert_with(|| "motherduck.dbt_temp".to_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::{clear_staging, record_staging, staging_identifier, take_staging};

    #[test]
    fn non_microbatch_name_has_no_trailing_separator() {
        assert_eq!(staging_identifier("orders", "", "abc123"), "orders__abc123");
    }

    #[test]
    fn microbatch_keeps_the_batch_after_the_run_suffix() {
        assert_eq!(
            staging_identifier("orders", "20240101T000000", "abc123"),
            "orders__abc123__20240101T000000"
        );
    }

    #[test]
    fn recorded_staging_is_taken_once() {
        clear_staging();
        record_staging("db", "dbt_temp", "orders__abc123");
        assert_eq!(
            take_staging(),
            Some((
                "db".to_owned(),
                "dbt_temp".to_owned(),
                "orders__abc123".to_owned()
            ))
        );
        assert!(take_staging().is_none());
    }
}
