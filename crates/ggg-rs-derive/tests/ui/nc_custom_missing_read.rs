// A `custom(...)` that only covers writing is an error for `ReadNetcdf`, which
// would otherwise have no way to read the field.
use ggg_rs_derive::ReadNetcdf;

fn to_nc(
    _d: &Bad,
    _v: &Vec<f64>,
    _f: &str,
    _n: &str,
    _g: &mut netcdf::GroupMut<'_>,
) -> netcdf::Result<()> {
    Ok(())
}

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(custom(write = to_nc))]
    values: Vec<f64>,
}

fn main() {}
