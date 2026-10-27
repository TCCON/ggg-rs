use std::collections::HashMap;

use ggg_rs_derive::{ReadNetcdf, WriteNetcdf};

/// Runtime input for the custom functions: the file stores compact site
/// indices, and this table translates them to and from site names.
struct SiteTable {
    names: Vec<String>,
}

impl SiteTable {
    fn new(names: &[&str]) -> Self {
        Self {
            names: names.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn index_of(&self) -> HashMap<&str, i32> {
        self.names
            .iter()
            .enumerate()
            .map(|(i, n)| (n.as_str(), i as i32))
            .collect()
    }
}

fn write_sites(
    _data: &Obs,
    sites: &[String],
    _field: &str,
    nc_name: &str,
    grp: &mut netcdf::GroupMut<'_>,
    ctx: &SiteTable,
) -> netcdf::Result<()> {
    let index = ctx.index_of();
    let idx = sites
        .iter()
        .map(|s| {
            index
                .get(s.as_str())
                .copied()
                .ok_or_else(|| netcdf::Error::Str(format!("unknown site '{s}'")))
        })
        .collect::<netcdf::Result<Vec<i32>>>()?;
    grp.add_dimension("obs", idx.len())?;
    grp.add_variable::<i32>(nc_name, &["obs"])?
        .put_values(&idx, ..)?;
    Ok(())
}

fn read_sites(
    _field: &str,
    nc_name: &str,
    grp: &netcdf::Group<'_>,
    ctx: &SiteTable,
) -> netcdf::Result<Vec<String>> {
    let var = grp
        .variable(nc_name)
        .ok_or_else(|| netcdf::Error::NotFound(format!("variable: '{nc_name}'")))?;
    var.get_values::<i32, _>(..)?
        .into_iter()
        .map(|i| {
            ctx.names
                .get(i as usize)
                .cloned()
                .ok_or_else(|| netcdf::Error::Str(format!("site index {i} not in table")))
        })
        .collect()
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(ty = "SiteTable"))]
struct Obs {
    #[netcdf(rename = "site_index", custom(write = write_sites, read = read_sites))]
    sites: Vec<String>,

    #[netcdf(dimensions("obs"))]
    xco2: Vec<f64>,
}

fn sample() -> Obs {
    Obs {
        sites: vec!["pa".into(), "ci".into(), "pa".into()],
        xco2: vec![400.0, 401.0, 402.0],
    }
}

/// The context reaches both custom functions: the file holds indices, and
/// reading with the same table restores the names.
#[test]
fn test_context_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ctx.nc");
    let table = SiteTable::new(&["ci", "pa"]);
    let data = sample();
    {
        let mut file = netcdf::create(&path).unwrap();
        let mut root = file.root_mut().unwrap();
        data.write_netcdf_context(&mut root, &table).unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    let stored = file
        .variable("site_index")
        .unwrap()
        .get_values::<i32, _>(..)
        .unwrap();
    assert_eq!(vec![1, 0, 1], stored);

    let read = Obs::read_netcdf_context(&file.root().unwrap(), &table).unwrap();
    assert_eq!(data, read);
}

/// Reading with a different table gives different values, so the round trip
/// above really depends on the context passed in at run time.
#[test]
fn test_context_changes_result() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ctx.nc");
    {
        let mut file = netcdf::create(&path).unwrap();
        let mut root = file.root_mut().unwrap();
        sample()
            .write_netcdf_context(&mut root, &SiteTable::new(&["ci", "pa"]))
            .unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    let other = SiteTable::new(&["db", "lh"]);
    let read = Obs::read_netcdf_context(&file.root().unwrap(), &other).unwrap();
    assert_eq!(vec!["lh", "db", "lh"], read.sites);
}

// ---------------------------------------------------------------------------
// `default`: the plain methods also exist and use `Default::default()`
// ---------------------------------------------------------------------------

/// A scale factor that defaults to 1.
struct Scale(f64);

impl Default for Scale {
    fn default() -> Self {
        Self(1.0)
    }
}

fn write_scaled(
    _data: &Scaled,
    v: &[f64],
    _field: &str,
    nc_name: &str,
    grp: &mut netcdf::GroupMut<'_>,
    ctx: &Scale,
) -> netcdf::Result<()> {
    let scaled: Vec<f64> = v.iter().map(|x| x * ctx.0).collect();
    grp.add_dimension("n", scaled.len())?;
    grp.add_variable::<f64>(nc_name, &["n"])?
        .put_values(&scaled, ..)?;
    Ok(())
}

fn read_scaled(
    _field: &str,
    nc_name: &str,
    grp: &netcdf::Group<'_>,
    ctx: &Scale,
) -> netcdf::Result<Vec<f64>> {
    let var = grp
        .variable(nc_name)
        .ok_or_else(|| netcdf::Error::NotFound(format!("variable: '{nc_name}'")))?;
    Ok(var
        .get_values::<f64, _>(..)?
        .into_iter()
        .map(|x| x / ctx.0)
        .collect())
}

#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(ty = "Scale", default))]
struct Scaled {
    #[netcdf(custom(write = write_scaled, read = read_scaled))]
    values: Vec<f64>,
}

#[test]
fn test_default_context_plain_methods() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scaled.nc");
    let data = Scaled {
        values: vec![1.0, 2.0],
    };
    {
        let mut file = netcdf::create(&path).unwrap();
        let mut root = file.root_mut().unwrap();
        data.write_netcdf(&mut root).unwrap();
    }

    let file = netcdf::open(&path).unwrap();
    // The default scale of 1 left the values as they were.
    let stored = file
        .variable("values")
        .unwrap()
        .get_values::<f64, _>(..)
        .unwrap();
    assert_eq!(vec![1.0, 2.0], stored);
    // The plain read uses the same default, and the `_context` read with an
    // explicit context sees the stored numbers differently.
    let root = file.root().unwrap();
    assert_eq!(data, Scaled::read_netcdf(&root).unwrap());
    let halved = Scaled::read_netcdf_context(&root, &Scale(2.0)).unwrap();
    assert_eq!(vec![0.5, 1.0], halved.values);
}

// ---------------------------------------------------------------------------
// A borrowing context
// ---------------------------------------------------------------------------

/// Borrows its data rather than owning it, written with `'_` in the annotation.
struct Prefix<'a>(&'a str);

fn read_prefixed(
    _field: &str,
    nc_name: &str,
    grp: &netcdf::Group<'_>,
    ctx: &Prefix<'_>,
) -> netcdf::Result<String> {
    let s: String = grp
        .attribute_value(nc_name)
        .ok_or_else(|| netcdf::Error::NotFound(format!("attribute: '{nc_name}'")))??
        .try_into()?;
    Ok(format!("{}{s}", ctx.0))
}

#[derive(ReadNetcdf)]
#[netcdf(context(ty = "Prefix<'_>"))]
struct Labelled {
    #[netcdf(custom(read = read_prefixed))]
    label: String,
}

#[test]
fn test_borrowed_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("label.nc");
    {
        let mut file = netcdf::create(&path).unwrap();
        file.add_attribute("label", "xco2").unwrap();
    }
    let prefix = String::from("tccon_");
    let file = netcdf::open(&path).unwrap();
    let read = Labelled::read_netcdf_context(&file.root().unwrap(), &Prefix(&prefix)).unwrap();
    assert_eq!("tccon_xco2", read.label);
}
