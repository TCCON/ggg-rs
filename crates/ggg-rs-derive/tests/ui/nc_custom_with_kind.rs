// `kind` has no meaning for a field written by a `custom` function.
use ggg_rs_derive::WriteNetcdf;

fn to_nc(
    _d: &Bad,
    _v: &Vec<f64>,
    _f: &str,
    _n: &str,
    _g: &mut netcdf::GroupMut<'_>,
) -> netcdf::Result<()> {
    Ok(())
}

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(custom(write = to_nc), kind = "values")]
    values: Vec<f64>,
}

fn main() {}
