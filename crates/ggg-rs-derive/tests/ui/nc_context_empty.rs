// `context()` must name a type for at least one direction.
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
#[netcdf(context(default))]
struct Bad {
    value: f64,
}

fn main() {}
