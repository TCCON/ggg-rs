// `attr` only affects writing, which `skip(write)` turns off.
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(skip(write), attr(name = "units", value = "K"))]
    values: Vec<f64>,
}

fn main() {}
