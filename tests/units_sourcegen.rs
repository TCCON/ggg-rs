//! Check that the per-quantity modules under `src/units` match the shared template.
//!
//! The modules `angle.rs`, `pressure.rs`, etc. are generated from
//! `src/units/quantity_template.rs.in` using the substitutions listed in
//! `src/units/quantities.toml`, and checked in so that they remain plain,
//! readable Rust. This test renders the template for each quantity and fails
//! if the checked-in file differs. To regenerate the files after editing the
//! template or the quantities table, run `make sourcegen` (which runs this
//! test with `UPDATE_UNITS=1` set in the environment).
use std::path::PathBuf;

const HEADER: &str = "\
// Generated from quantity_template.rs.in and quantities.toml by tests/units_sourcegen.rs.
// Do not edit directly; edit the template or table, then run `make sourcegen`.
";

/// Values substituted into the template for one quantity.
/// See `src/units/quantities.toml` for the meaning of each field.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct QuantitySpec {
    quantity: String,
    type_name: String,
    unit: String,
    value: String,
    abbr: String,
    field: String,
    units_attr: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct QuantityTable {
    quantity: Vec<QuantitySpec>,
}

fn render(template: &str, spec: &QuantitySpec) -> String {
    let body = template
        .replace("@quantity@", &spec.quantity)
        .replace("@Quantity@", &spec.type_name)
        .replace("@unit@", &spec.unit)
        .replace("@value@", &spec.value)
        .replace("@abbr@", &spec.abbr)
        .replace("@field@", &spec.field)
        .replace("@units_attr@", &spec.units_attr);
    assert!(
        !body.contains('@'),
        "unsubstituted placeholder left in rendered {} module",
        spec.quantity
    );
    format!("{HEADER}{body}")
}

#[test]
fn units_modules_match_template() {
    let units_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/units");
    let template = std::fs::read_to_string(units_dir.join("quantity_template.rs.in"))
        .expect("could not read src/units/quantity_template.rs.in");
    let table = std::fs::read_to_string(units_dir.join("quantities.toml"))
        .expect("could not read src/units/quantities.toml");
    let table: QuantityTable =
        toml::from_str(&table).expect("could not parse src/units/quantities.toml");
    let update = std::env::var_os("UPDATE_UNITS").is_some();

    let mut stale = vec![];
    for spec in &table.quantity {
        let path = units_dir.join(format!("{}.rs", spec.quantity));
        let expected = render(&template, spec);
        let actual = std::fs::read_to_string(&path).unwrap_or_default();
        if actual != expected {
            if update {
                std::fs::write(&path, &expected)
                    .unwrap_or_else(|e| panic!("could not write {}: {e}", path.display()));
            } else {
                stale.push(path.display().to_string());
            }
        }
    }

    assert!(
        stale.is_empty(),
        "These units modules are out of sync with src/units/quantity_template.rs.in:\n  {}\n\
         Edit the template rather than the generated files, then run \
         `make sourcegen` to regenerate them.",
        stale.join("\n  ")
    );
}
