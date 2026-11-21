// `skip(...)` only accepts `read` and `write`.
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(skip(both))]
    values: Vec<f64>,
}

fn main() {}
