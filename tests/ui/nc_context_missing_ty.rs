// `context(...)` must name its type.
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
#[netcdf(context(default))]
struct Bad {
    value: f64,
}

fn main() {}
