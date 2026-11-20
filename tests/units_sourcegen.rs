//! Check that the per-quantity modules under `src/units` match the shared template.
//!
//! The generation logic lives in the `xtask` package (see `xtask/src/lib.rs`)
//! so that it can run even when ggg-rs does not compile. Run `make sourcegen`
//! to regenerate the modules if this test fails.

#[test]
fn units_modules_match_template() {
    let stale = xtask::check(&xtask::units_dir()).unwrap_or_else(|e| panic!("{e}"));
    let stale: Vec<String> = stale.iter().map(|p| p.display().to_string()).collect();
    assert!(
        stale.is_empty(),
        "These units modules are out of sync with src/units/quantity_template.rs.in:\n  {}\n\
         Edit the template rather than the generated files, then run \
         `make sourcegen` to regenerate them.",
        stale.join("\n  ")
    );
}
