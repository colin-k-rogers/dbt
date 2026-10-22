use dbt_adapter_core::AdapterType;

use crate::macro_test_harness::MacroTestHarness;

#[test]
fn dispatch_inherits_duckdb_macro() {
    let harness = MacroTestHarness::for_adapter(AdapterType::MotherDuck)
        .load_all_macros()
        .build()
        .expect("harness should build");

    let rendered = harness
        .render("{{ adapter.dispatch('any_value', 'dbt')('amount') }}", ())
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "arbitrary(amount)");
}

#[test]
fn motherduck_dispatch_override_wins_over_duckdb() {
    let harness = MacroTestHarness::for_adapter(AdapterType::MotherDuck)
        .load_all_macros()
        .with_macro(
            "dbt_motherduck",
            "motherduck__any_value",
            "{% macro motherduck__any_value(expression) %}motherduck({{ expression }}){% endmacro %}",
        )
        .build()
        .expect("harness should build");

    let rendered = harness
        .render("{{ adapter.dispatch('any_value', 'dbt')('amount') }}", ())
        .expect("render should succeed");

    assert_eq!(rendered.trim(), "motherduck(amount)");
}
