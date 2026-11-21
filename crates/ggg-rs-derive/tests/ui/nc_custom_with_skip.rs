// `skip` and `custom` contradict each other.
use ggg_rs_derive::ReadNetcdf;

fn from_nc(_f: &str, _n: &str, _g: &netcdf::Group<'_>) -> netcdf::Result<Vec<f64>> {
    Ok(Vec::new())
}

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(skip, custom(read = from_nc))]
    values: Vec<f64>,
}

fn main() {}
