// `unpack` only affects reading, which `skip(read)` turns off.
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(skip(read), unpack = false)]
    values: Vec<f64>,
}

fn main() {}
