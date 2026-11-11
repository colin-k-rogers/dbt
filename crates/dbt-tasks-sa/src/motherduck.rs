use dbt_adapter::adapter::motherduck::take_staging;
use dbt_jinja_utils::jinja_environment::JinjaEnv;
use minijinja::Value;
use minijinja::value::OverlayMap;

pub(crate) fn drop_staging_after_failure(jinja_env: &JinjaEnv, context: &mut OverlayMap) {
    let Some((database, schema, identifier)) = take_staging() else {
        return;
    };
    context.insert(
        "__motherduck_staging_database".to_string(),
        Value::from(database),
    );
    context.insert(
        "__motherduck_staging_schema".to_string(),
        Value::from(schema),
    );
    context.insert(
        "__motherduck_staging_identifier".to_string(),
        Value::from(identifier),
    );
    if let Err(error) = jinja_env.render_str(
        "{{ motherduck__drop_staging_relation(__motherduck_staging_database, __motherduck_staging_schema, __motherduck_staging_identifier) }}",
        context,
        &[],
    ) {
        tracing::warn!(
            "failed to drop MotherDuck staging relation after materialization error: {error}"
        );
    }
}
