//! The arguments that `map`, `map_units`, and `attr(func = ...)` functions
//! receive: both the Rust field name and the netCDF name, and the context when
//! the struct declares one for that direction.

use ggg_rs_derive::{ReadNetcdf, WriteNetcdf};

const FIELD: &str = "rust_name";
const NC: &str = "ncName";

fn names(field: &str, nc: &str) -> String {
    format!("{field}/{nc}")
}

// ---------------------------------------------------------------------------
// Both names, no context
// ---------------------------------------------------------------------------

/// Writes the names it receives as the data length, so a wrong argument shows
/// up as a wrong value rather than a panic inside generated code.
fn write_map(_data: &Named, values: &[f64], field: &str, nc: &str) -> Vec<f64> {
    assert_eq!((field, nc), (FIELD, NC));
    values.to_vec()
}

fn write_map_units(
    _data: &UnitsNamed,
    values: &[f64],
    field: &str,
    nc: &str,
) -> (Vec<f64>, String) {
    (values.to_vec(), names(field, nc))
}

fn attr_names(_data: &Named, _values: &Vec<f64>, field: &str, nc: &str) -> String {
    names(field, nc)
}

fn read_map(
    values: Vec<f64>,
    field: &str,
    nc: &str,
    var: &netcdf::Variable<'_>,
) -> netcdf::Result<Vec<f64>> {
    // The variable is the one named `nc`, not `field`.
    assert_eq!(var.name(), nc);
    if (field, nc) != (FIELD, NC) {
        return Err(netcdf::Error::Str(format!("wrong names: {field}, {nc}")));
    }
    Ok(values)
}

fn read_map_units(
    values: Vec<f64>,
    field: &str,
    nc: &str,
    _var: &netcdf::Variable<'_>,
    units: Option<&str>,
) -> netcdf::Result<Vec<f64>> {
    if (field, nc) != (FIELD, NC) {
        return Err(netcdf::Error::Str(format!("wrong names: {field}, {nc}")));
    }
    assert_eq!(units, Some("rust_name/ncName"));
    Ok(values)
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
struct Named {
    #[netcdf(
        dimensions("time"),
        rename = "ncName",
        kind = "values",
        map(write = write_map, read = read_map),
        attr(name = "names", func = attr_names)
    )]
    rust_name: Vec<f64>,
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
struct UnitsNamed {
    #[netcdf(
        dimensions("time"),
        rename = "ncName",
        kind = "values",
        map_units(write = write_map_units, read = read_map_units)
    )]
    rust_name: Vec<f64>,
}

fn string_attr(var: &netcdf::Variable<'_>, name: &str) -> String {
    match var.attribute(name).unwrap().value().unwrap() {
        netcdf::AttributeValue::Str(s) => s,
        other => panic!("attribute {name} was not a string: {other:?}"),
    }
}

#[test]
fn test_map_and_attr_func_receive_both_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("named.nc");
    let data = Named {
        rust_name: vec![1.0, 2.0],
    };
    {
        let mut file = netcdf::create(&path).unwrap();
        data.write_netcdf(&mut file.root_mut().unwrap()).unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    let var = file.variable(NC).unwrap();
    assert_eq!(string_attr(&var, "names"), "rust_name/ncName");
    assert_eq!(Named::read_netcdf(&file.root().unwrap()).unwrap(), data);
}

#[test]
fn test_map_units_receives_both_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("units_named.nc");
    let data = UnitsNamed {
        rust_name: vec![1.0, 2.0],
    };
    {
        let mut file = netcdf::create(&path).unwrap();
        data.write_netcdf(&mut file.root_mut().unwrap()).unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    let var = file.variable(NC).unwrap();
    assert_eq!(string_attr(&var, "units"), "rust_name/ncName");
    assert_eq!(
        UnitsNamed::read_netcdf(&file.root().unwrap()).unwrap(),
        data
    );
}

/// A map function generic over the struct type, as a shared helper would be,
/// must still infer through the arity check.
fn generic_double<A>(_data: &A, values: &[f64], _field: &str, _nc: &str) -> Vec<f64> {
    values.iter().map(|v| v * 2.0).collect()
}

fn generic_halve<T: Copy + Into<f64>>(
    values: Vec<T>,
    _field: &str,
    _nc: &str,
    _var: &netcdf::Variable<'_>,
) -> netcdf::Result<Vec<f64>> {
    Ok(values.into_iter().map(|v| v.into() / 2.0).collect())
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
struct Generic {
    #[netcdf(
        dimensions("time"),
        kind = "values",
        map(write = generic_double, read = generic_halve::<f64>)
    )]
    xco2: Vec<f64>,
}

#[test]
fn test_generic_map_functions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("generic.nc");
    let data = Generic {
        xco2: vec![1.0, 2.0],
    };
    {
        let mut file = netcdf::create(&path).unwrap();
        data.write_netcdf(&mut file.root_mut().unwrap()).unwrap();
    }
    let file = netcdf::open(&path).unwrap();
    let stored: Vec<f64> = file.variable("xco2").unwrap().get_values(..).unwrap();
    assert_eq!(stored, vec![2.0, 4.0]);
    assert_eq!(Generic::read_netcdf(&file.root().unwrap()).unwrap(), data);
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// The write-side context: a factor to multiply by and a note to attach.
#[derive(Default)]
struct WCtx {
    factor: f64,
    note: String,
}

/// The read-side context: a factor to divide by.
#[derive(Default)]
struct RCtx {
    factor: f64,
}

fn ctx_write_map(_d: &Ctxd, v: &[f64], _f: &str, _n: &str, ctx: &WCtx) -> Vec<f64> {
    v.iter().map(|x| x * ctx.factor).collect()
}

fn ctx_write_map_units(_d: &Ctxd, v: &[f64], _f: &str, _n: &str, ctx: &WCtx) -> (Vec<f64>, String) {
    (
        v.iter().map(|x| x * ctx.factor).collect(),
        format!("x{}", ctx.factor),
    )
}

fn ctx_attr(_d: &Ctxd, _v: &Vec<f64>, _f: &str, _n: &str, ctx: &WCtx) -> String {
    ctx.note.clone()
}

fn ctx_read_map(
    v: Vec<f64>,
    _f: &str,
    _n: &str,
    _var: &netcdf::Variable<'_>,
    ctx: &RCtx,
) -> netcdf::Result<Vec<f64>> {
    Ok(v.into_iter().map(|x| x / ctx.factor).collect())
}

fn ctx_read_map_units(
    v: Vec<f64>,
    _f: &str,
    _n: &str,
    _var: &netcdf::Variable<'_>,
    _units: Option<&str>,
    ctx: &RCtx,
) -> netcdf::Result<Vec<f64>> {
    Ok(v.into_iter().map(|x| x / ctx.factor).collect())
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(read = "RCtx", write = "WCtx"))]
struct Ctxd {
    #[netcdf(
        dimensions("time"),
        kind = "values",
        map(write = ctx_write_map, read = ctx_read_map),
        attr(name = "note", func = ctx_attr)
    )]
    a: Vec<f64>,

    #[netcdf(
        dimensions("time"),
        kind = "values",
        map_units(write = ctx_write_map_units, read = ctx_read_map_units)
    )]
    b: Vec<f64>,
}

#[test]
fn test_context_reaches_map_map_units_and_attr_func() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ctx.nc");
    let data = Ctxd {
        a: vec![1.0, 2.0],
        b: vec![3.0, 4.0],
    };
    let wctx = WCtx {
        factor: 10.0,
        note: "from the context".to_string(),
    };
    {
        let mut file = netcdf::create(&path).unwrap();
        data.write_netcdf_context(&mut file.root_mut().unwrap(), &wctx)
            .unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    let a = file.variable("a").unwrap();
    let b = file.variable("b").unwrap();
    assert_eq!(a.get_values::<f64, _>(..).unwrap(), vec![10.0, 20.0]);
    assert_eq!(b.get_values::<f64, _>(..).unwrap(), vec![30.0, 40.0]);
    assert_eq!(string_attr(&a, "note"), "from the context");
    assert_eq!(string_attr(&b, "units"), "x10");

    let read = Ctxd::read_netcdf_context(&file.root().unwrap(), &RCtx { factor: 10.0 }).unwrap();
    assert_eq!(read, data);

    // A different read context gives a different result, so it really is used.
    let read = Ctxd::read_netcdf_context(&file.root().unwrap(), &RCtx { factor: 5.0 }).unwrap();
    assert_eq!(read.a, vec![2.0, 4.0]);
}

/// Write context only: read map functions take no context argument.
fn plain_read_map(
    v: Vec<f64>,
    _f: &str,
    _n: &str,
    _var: &netcdf::Variable<'_>,
) -> netcdf::Result<Vec<f64>> {
    Ok(v)
}

fn wonly_write_map(_d: &WriteOnly, v: &[f64], _f: &str, _n: &str, ctx: &WCtx) -> Vec<f64> {
    v.iter().map(|x| x * ctx.factor).collect()
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(write = "WCtx", default))]
struct WriteOnly {
    #[netcdf(
        dimensions("time"),
        kind = "values",
        map(write = wonly_write_map, read = plain_read_map)
    )]
    a: Vec<f64>,
}

#[test]
fn test_write_only_context_and_default() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wonly.nc");
    let data = WriteOnly { a: vec![1.0, 2.0] };
    {
        let mut file = netcdf::create(&path).unwrap();
        // The plain method passes `WCtx::default()`, whose factor is 0.
        data.write_netcdf(&mut file.root_mut().unwrap()).unwrap();
    }
    let file = netcdf::open(&path).unwrap();
    // No context on the read side, so the plain method is the only one.
    let read = WriteOnly::read_netcdf(&file.root().unwrap()).unwrap();
    assert_eq!(read.a, vec![0.0, 0.0]);
}

// ---------------------------------------------------------------------------
// `map` on an `attribute` field
// ---------------------------------------------------------------------------

/// An attribute has no variable, so the read function receives the attribute
/// the value came from in its place.
fn version_from_nc(
    v: String,
    field: &str,
    nc: &str,
    attr: &netcdf::Attribute<'_>,
) -> netcdf::Result<u32> {
    assert_eq!(
        (field, nc, attr.name()),
        ("version", "file_version", "file_version")
    );
    v.strip_prefix('v')
        .and_then(|n| n.parse().ok())
        .ok_or_else(|| netcdf::Error::Str(format!("bad version string '{v}'")))
}

fn version_to_nc(_d: &Versioned, v: &u32, _field: &str, _nc: &str) -> String {
    format!("v{v}")
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
struct Versioned {
    #[netcdf(
        attribute,
        rename = "file_version",
        map(write = version_to_nc, read = version_from_nc)
    )]
    version: u32,
}

#[test]
fn test_map_on_attribute_field_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("versioned.nc");
    let data = Versioned { version: 7 };
    {
        let mut file = netcdf::create(&path).unwrap();
        data.write_netcdf(&mut file.root_mut().unwrap()).unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    assert_eq!(
        file.attribute("file_version").unwrap().value().unwrap(),
        netcdf::AttributeValue::Str("v7".to_string())
    );
    assert_eq!(Versioned::read_netcdf(&file.root().unwrap()).unwrap(), data);
}

/// A missing attribute is reported as such, not as a missing variable.
#[test]
fn test_map_on_missing_attribute_names_the_attribute() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.nc");
    drop(netcdf::create(&path).unwrap());
    let file = netcdf::open(&path).unwrap();
    let msg = Versioned::read_netcdf(&file.root().unwrap())
        .expect_err("attribute is missing")
        .to_string();
    assert!(
        msg.contains("attribute") && msg.contains("file_version"),
        "unexpected error message: {msg}"
    );
}
