use ggg_rs_derive::{ReadNetcdf, WriteNetcdf};

/// Read a string attribute off a variable, panicking with a useful message.
fn var_str(file: &netcdf::File, var: &str, attr: &str) -> String {
    file.variable(var)
        .unwrap_or_else(|| panic!("no variable '{var}'"))
        .attribute_value(attr)
        .unwrap_or_else(|| panic!("no attribute '{attr}' on '{var}'"))
        .unwrap()
        .try_into()
        .unwrap()
}

/// Read a string group attribute off the root group.
fn grp_str(file: &netcdf::File, attr: &str) -> String {
    file.attribute(attr)
        .unwrap_or_else(|| panic!("no group attribute '{attr}'"))
        .value()
        .unwrap()
        .try_into()
        .unwrap()
}

fn write<F: FnOnce(&mut netcdf::GroupMut<'_>)>(path: &std::path::Path, f: F) {
    let mut file = netcdf::create(path).unwrap();
    let mut root = file.root_mut().unwrap();
    f(&mut root);
}

// ---------------------------------------------------------------------------
// Field and container level, no context
// ---------------------------------------------------------------------------

/// Writes several attributes at once, including both names it was given, and
/// overwrites `long_name`, which `attr(...)` set first.
fn xco2_attrs(
    data: &Obs,
    values: &[f64],
    field_name: &str,
    nc_name: &str,
    var: &mut netcdf::VariableMut<'_>,
) -> netcdf::Result<()> {
    var.put_attribute("units", "ppm")?;
    var.put_attribute("n_values", values.len() as i32)?;
    var.put_attribute("field_name", field_name)?;
    var.put_attribute("nc_name", nc_name)?;
    var.put_attribute("site", data.site.as_str())?;
    var.put_attribute("long_name", "from attrs")?;
    var.put_attribute("order", "first")?;
    Ok(())
}

/// A second function on the same field: it runs after the first.
fn xco2_more_attrs(
    _data: &Obs,
    _values: &[f64],
    _field_name: &str,
    _nc_name: &str,
    var: &mut netcdf::VariableMut<'_>,
) -> netcdf::Result<()> {
    var.put_attribute("order", "second")?;
    Ok(())
}

/// Container-level: writes group attributes, and reads back a variable a
/// field wrote, which only works if it runs after the fields.
fn global_attrs(data: &Obs, grp: &mut netcdf::GroupMut<'_>) -> netcdf::Result<()> {
    grp.add_attribute("site", data.site.as_str())?;
    let n = grp
        .variable("xco2_ppm")
        .ok_or_else(|| netcdf::Error::Str("xco2_ppm not written yet".to_string()))?
        .len();
    grp.add_attribute("n_obs", n as i32)?;
    Ok(())
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(attrs(func = global_attrs))]
struct Obs {
    #[netcdf(
        rename = "xco2_ppm",
        dimensions("time"),
        attr(name = "long_name", value = "from attr"),
        attrs(func = xco2_attrs),
        attrs(func = xco2_more_attrs)
    )]
    xco2: Vec<f64>,

    #[netcdf(attribute)]
    site: String,
}

fn sample() -> Obs {
    Obs {
        xco2: vec![400.0, 401.0, 402.0],
        site: "pa".to_string(),
    }
}

#[test]
fn test_field_attrs_written() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attrs.nc");
    write(&path, |root| sample().write_netcdf(root).unwrap());

    let file = netcdf::open(&path).unwrap();
    assert_eq!("ppm", var_str(&file, "xco2_ppm", "units"));
    assert_eq!("xco2", var_str(&file, "xco2_ppm", "field_name"));
    assert_eq!("xco2_ppm", var_str(&file, "xco2_ppm", "nc_name"));
    assert_eq!("pa", var_str(&file, "xco2_ppm", "site"));
    let n: i32 = file
        .variable("xco2_ppm")
        .unwrap()
        .attribute_value("n_values")
        .unwrap()
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(3, n);
}

/// The `attrs` function runs after `attr(...)`, so it wins on a shared name.
#[test]
fn test_field_attrs_run_after_attr() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attrs.nc");
    write(&path, |root| sample().write_netcdf(root).unwrap());

    let file = netcdf::open(&path).unwrap();
    assert_eq!("from attrs", var_str(&file, "xco2_ppm", "long_name"));
}

/// Repeated `attrs(...)` all run, in the order written.
#[test]
fn test_field_attrs_run_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attrs.nc");
    write(&path, |root| sample().write_netcdf(root).unwrap());

    let file = netcdf::open(&path).unwrap();
    assert_eq!("second", var_str(&file, "xco2_ppm", "order"));
}

/// The container function wrote group attributes after the fields existed.
#[test]
fn test_container_attrs_written_after_fields() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attrs.nc");
    write(&path, |root| sample().write_netcdf(root).unwrap());

    let file = netcdf::open(&path).unwrap();
    assert_eq!("pa", grp_str(&file, "site"));
    let n: i32 = file
        .attribute("n_obs")
        .unwrap()
        .value()
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(3, n);
}

/// `ReadNetcdf` ignores `attrs` at both levels, so the struct still reads.
#[test]
fn test_attrs_struct_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attrs.nc");
    let data = sample();
    write(&path, |root| data.write_netcdf(root).unwrap());

    let file = netcdf::open(&path).unwrap();
    assert_eq!(data, Obs::read_netcdf(&file.root().unwrap()).unwrap());
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

fn failing_attrs(
    _data: &Failing,
    _values: &[f64],
    _field_name: &str,
    nc_name: &str,
    _var: &mut netcdf::VariableMut<'_>,
) -> netcdf::Result<()> {
    Err(netcdf::Error::Str(format!("attrs failed for '{nc_name}'")))
}

#[derive(WriteNetcdf)]
struct Failing {
    #[netcdf(attrs(func = failing_attrs))]
    values: Vec<f64>,
}

#[test]
fn test_attrs_error_propagates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fail.nc");
    let mut file = netcdf::create(&path).unwrap();
    let mut root = file.root_mut().unwrap();
    let err = Failing { values: vec![1.0] }
        .write_netcdf(&mut root)
        .expect_err("attrs function returned an error");
    assert!(
        err.to_string().contains("attrs failed for 'values'"),
        "unexpected error message: {err}"
    );
}

// ---------------------------------------------------------------------------
// With a context
// ---------------------------------------------------------------------------

struct Provenance {
    version: String,
    units: String,
}

fn ctx_field_attrs(
    _data: &WithCtx,
    _values: &[f64],
    _field_name: &str,
    _nc_name: &str,
    var: &mut netcdf::VariableMut<'_>,
    ctx: &Provenance,
) -> netcdf::Result<()> {
    var.put_attribute("units", ctx.units.as_str())?;
    Ok(())
}

fn ctx_global_attrs(
    _data: &WithCtx,
    grp: &mut netcdf::GroupMut<'_>,
    ctx: &Provenance,
) -> netcdf::Result<()> {
    grp.add_attribute("version", ctx.version.as_str())?;
    Ok(())
}

#[derive(WriteNetcdf)]
#[netcdf(context(ty = "Provenance"), attrs(func = ctx_global_attrs))]
struct WithCtx {
    #[netcdf(attrs(func = ctx_field_attrs))]
    xluft: Vec<f64>,
}

#[test]
fn test_attrs_receive_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ctx.nc");
    let ctx = Provenance {
        version: "GGG2020.1".to_string(),
        units: "1".to_string(),
    };
    write(&path, |root| {
        WithCtx { xluft: vec![0.999] }
            .write_netcdf_context(root, &ctx)
            .unwrap()
    });

    let file = netcdf::open(&path).unwrap();
    assert_eq!("1", var_str(&file, "xluft", "units"));
    assert_eq!("GGG2020.1", grp_str(&file, "version"));
}
