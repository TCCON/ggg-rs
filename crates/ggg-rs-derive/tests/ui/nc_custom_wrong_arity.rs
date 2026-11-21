// A `custom` read function with neither expected argument count.
use ggg_rs_derive::ReadNetcdf;

fn from_nc(_n: &str) -> netcdf::Result<f64> {
    Ok(0.0)
}

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(custom(read = from_nc))]
    value: f64,
}

fn main() {}
