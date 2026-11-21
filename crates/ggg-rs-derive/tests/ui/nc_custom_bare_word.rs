// `custom` needs a direction; the bare word is rejected.
use ggg_rs_derive::WriteNetcdf;

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(custom)]
    values: Vec<f64>,
}

fn main() {}
