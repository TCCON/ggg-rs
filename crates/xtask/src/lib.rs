//! Source generation for ggg-rs.
//!
//! The modules `src/units/angle.rs`, `src/units/pressure.rs`, etc. are generated
//! from `src/units/quantity_template.rs.in` using the substitutions listed in
//! `src/units/quantities.toml`, and checked in so that they remain plain,
//! readable Rust. This crate deliberately does not depend on ggg-rs, so that
//! the modules can be regenerated even when ggg-rs does not compile.
//!
//! Run `make sourcegen` to regenerate the modules and `make sourcegen-check`
//! to check that they are up to date without building ggg-rs. The ggg-rs test
//! suite also runs [`check`] (in `tests/units_sourcegen.rs`).
use std::path::{Path, PathBuf};

const HEADER: &str = "\
// Generated from quantity_template.rs.in and quantities.toml by xtask.
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

/// The `src/units` directory of the ggg-rs crate this xtask belongs to.
pub fn units_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask must be inside the ggg-rs crate directory")
        .join("../src/units")
}

fn render(template: &str, spec: &QuantitySpec) -> Result<String, String> {
    let body = template
        .replace("@quantity@", &spec.quantity)
        .replace("@Quantity@", &spec.type_name)
        .replace("@unit@", &spec.unit)
        .replace("@value@", &spec.value)
        .replace("@abbr@", &spec.abbr)
        .replace("@field@", &spec.field)
        .replace("@units_attr@", &spec.units_attr);
    if body.contains('@') {
        return Err(format!(
            "unsubstituted placeholder left in rendered {} module",
            spec.quantity
        ));
    }
    Ok(format!("{HEADER}{body}"))
}

/// Render every module in `units_dir`, returning each output path with its expected contents.
fn render_all(units_dir: &Path) -> Result<Vec<(PathBuf, String)>, String> {
    let template_path = units_dir.join("quantity_template.rs.in");
    let template = std::fs::read_to_string(&template_path)
        .map_err(|e| format!("could not read {}: {e}", template_path.display()))?;
    let table_path = units_dir.join("quantities.toml");
    let table = std::fs::read_to_string(&table_path)
        .map_err(|e| format!("could not read {}: {e}", table_path.display()))?;
    let table: QuantityTable = toml::from_str(&table)
        .map_err(|e| format!("could not parse {}: {e}", table_path.display()))?;

    table
        .quantity
        .iter()
        .map(|spec| {
            let path = units_dir.join(format!("{}.rs", spec.quantity));
            Ok((path, render(&template, spec)?))
        })
        .collect()
}

/// Return the generated modules in `units_dir` that differ from the template.
pub fn check(units_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut stale = vec![];
    for (path, expected) in render_all(units_dir)? {
        let actual = std::fs::read_to_string(&path).unwrap_or_default();
        if actual != expected {
            stale.push(path);
        }
    }
    Ok(stale)
}

/// Rewrite the generated modules in `units_dir` that differ from the template,
/// returning the paths that were rewritten.
pub fn update(units_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut written = vec![];
    for (path, expected) in render_all(units_dir)? {
        let actual = std::fs::read_to_string(&path).unwrap_or_default();
        if actual != expected {
            std::fs::write(&path, &expected)
                .map_err(|e| format!("could not write {}: {e}", path.display()))?;
            written.push(path);
        }
    }
    Ok(written)
}
