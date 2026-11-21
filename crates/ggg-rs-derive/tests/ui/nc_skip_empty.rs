// An empty `skip()` names no direction.
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(skip())]
    values: Vec<f64>,
}

fn main() {}
