// A `read = ...` map half would never run on a `skip(read)` field.
use ggg_rs_derive::ReadNetcdf;

fn from_nc(
    v: Vec<f64>,
    _f: &str,
    _n: &str,
    _var: &netcdf::Variable<'_>,
) -> netcdf::Result<Vec<f64>> {
    Ok(v)
}

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(skip(read), kind = "values", map(read = from_nc))]
    values: Vec<f64>,
}

fn main() {}
