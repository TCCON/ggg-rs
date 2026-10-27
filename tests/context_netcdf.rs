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

// ---------------------------------------------------------------------------
// Per-direction contexts
// ---------------------------------------------------------------------------

/// Write-side context: multiplies the values before storing them.
#[derive(Default)]
struct WScale(f64);

/// Read-side context: adds to the stored values.
#[derive(Default)]
struct ROffset(f64);

fn put_plain(nc_name: &str, grp: &mut netcdf::GroupMut<'_>, v: &[f64]) -> netcdf::Result<()> {
    grp.add_dimension(&format!("{nc_name}_n"), v.len())?;
    grp.add_variable::<f64>(nc_name, &[&format!("{nc_name}_n")])?
        .put_values(v, ..)?;
    Ok(())
}

fn get_plain(nc_name: &str, grp: &netcdf::Group<'_>) -> netcdf::Result<Vec<f64>> {
    grp.variable(nc_name)
        .ok_or_else(|| netcdf::Error::NotFound(format!("variable: '{nc_name}'")))?
        .get_values::<f64, _>(..)
}

macro_rules! write_fns {
    ($s:ty, $plain:ident, $scaled:ident) => {
        #[allow(dead_code)]
        fn $plain(
            _d: &$s,
            v: &[f64],
            _f: &str,
            n: &str,
            g: &mut netcdf::GroupMut<'_>,
        ) -> netcdf::Result<()> {
            put_plain(n, g, v)
        }
        #[allow(dead_code)]
        fn $scaled(
            _d: &$s,
            v: &[f64],
            _f: &str,
            n: &str,
            g: &mut netcdf::GroupMut<'_>,
            ctx: &WScale,
        ) -> netcdf::Result<()> {
            let scaled: Vec<f64> = v.iter().map(|x| x * ctx.0).collect();
            put_plain(n, g, &scaled)
        }
    };
}

fn read_plain(_f: &str, n: &str, g: &netcdf::Group<'_>) -> netcdf::Result<Vec<f64>> {
    get_plain(n, g)
}

fn read_offset(
    _f: &str,
    n: &str,
    g: &netcdf::Group<'_>,
    ctx: &ROffset,
) -> netcdf::Result<Vec<f64>> {
    Ok(get_plain(n, g)?.into_iter().map(|x| x + ctx.0).collect())
}

fn create(path: &std::path::Path) -> netcdf::FileMut {
    netcdf::create(path).unwrap()
}

fn stored(path: &std::path::Path) -> Vec<f64> {
    get_plain("values", &netcdf::open(path).unwrap().root().unwrap()).unwrap()
}

/// Different types per direction: each derive's functions get their own.
#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(read = "ROffset", write = "WScale"))]
struct BothDirs {
    #[netcdf(custom(write = both_write, read = read_offset))]
    values: Vec<f64>,
}
write_fns!(BothDirs, _both_plain, both_write);

#[test]
fn test_separate_read_and_write_contexts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("both.nc");
    BothDirs {
        values: vec![1.0, 2.0],
    }
    .write_netcdf_context(&mut create(&path).root_mut().unwrap(), &WScale(2.0))
    .unwrap();
    assert_eq!(vec![2.0, 4.0], stored(&path));

    let file = netcdf::open(&path).unwrap();
    let read = BothDirs::read_netcdf_context(&file.root().unwrap(), &ROffset(1.0)).unwrap();
    assert_eq!(vec![3.0, 5.0], read.values);
}

/// Read-only context: writing uses the plain method and context-free
/// functions; reading takes the context.
#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(read = "ROffset"))]
struct ReadOnlyCtx {
    #[netcdf(custom(write = ro_write, read = read_offset))]
    values: Vec<f64>,
}
write_fns!(ReadOnlyCtx, ro_write, _ro_scaled);

#[test]
fn test_read_only_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ro.nc");
    ReadOnlyCtx {
        values: vec![1.0, 2.0],
    }
    .write_netcdf(&mut create(&path).root_mut().unwrap())
    .unwrap();
    assert_eq!(vec![1.0, 2.0], stored(&path));

    let file = netcdf::open(&path).unwrap();
    let read = ReadOnlyCtx::read_netcdf_context(&file.root().unwrap(), &ROffset(10.0)).unwrap();
    assert_eq!(vec![11.0, 12.0], read.values);
}

/// Write-only context: the mirror image.
#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(write = "WScale"))]
struct WriteOnlyCtx {
    #[netcdf(custom(write = wo_write, read = read_plain))]
    values: Vec<f64>,
}
write_fns!(WriteOnlyCtx, _wo_plain, wo_write);

#[test]
fn test_write_only_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wo.nc");
    WriteOnlyCtx {
        values: vec![1.0, 2.0],
    }
    .write_netcdf_context(&mut create(&path).root_mut().unwrap(), &WScale(3.0))
    .unwrap();

    let file = netcdf::open(&path).unwrap();
    let read = WriteOnlyCtx::read_netcdf(&file.root().unwrap()).unwrap();
    assert_eq!(vec![3.0, 6.0], read.values);
}

/// `default` inside one direction only: the plain `read_netcdf` exists (and
/// uses `ROffset::default()`), while writing still needs a context.
#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(read(ty = "ROffset", default), write = "WScale"))]
struct ReadDefault {
    #[netcdf(custom(write = rd_write, read = read_offset))]
    values: Vec<f64>,
}
write_fns!(ReadDefault, _rd_plain, rd_write);

#[test]
fn test_per_direction_default() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rd.nc");
    ReadDefault {
        values: vec![1.0, 2.0],
    }
    .write_netcdf_context(&mut create(&path).root_mut().unwrap(), &WScale(2.0))
    .unwrap();

    let file = netcdf::open(&path).unwrap();
    let root = file.root().unwrap();
    assert_eq!(
        vec![2.0, 4.0],
        ReadDefault::read_netcdf(&root).unwrap().values
    );
    assert_eq!(
        vec![3.0, 5.0],
        ReadDefault::read_netcdf_context(&root, &ROffset(1.0))
            .unwrap()
            .values
    );
}

/// A top-level `default` applies to every declared direction.
#[derive(Debug, PartialEq, ReadNetcdf, WriteNetcdf)]
#[netcdf(context(read = "ROffset", write = "WScale", default))]
struct AllDefault {
    #[netcdf(custom(write = ad_write, read = read_offset))]
    values: Vec<f64>,
}
write_fns!(AllDefault, _ad_plain, ad_write);

#[test]
fn test_top_level_default_applies_to_both() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ad.nc");
    // `WScale::default()` is 0, so the plain write stores zeros.
    AllDefault {
        values: vec![1.0, 2.0],
    }
    .write_netcdf(&mut create(&path).root_mut().unwrap())
    .unwrap();
    assert_eq!(vec![0.0, 0.0], stored(&path));

    let file = netcdf::open(&path).unwrap();
    assert_eq!(
        vec![0.0, 0.0],
        AllDefault::read_netcdf(&file.root().unwrap())
            .unwrap()
            .values
    );
}
