// `attrs` needs a function; the bare word is rejected.
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(attrs)]
    values: Vec<f64>,
}

fn main() {}
