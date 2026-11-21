// The direction goes in a list, `skip(read)`, not `skip = "read"`.
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(skip = "read")]
    values: Vec<f64>,
}

fn main() {}
