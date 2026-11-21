// Integer fields cannot hold NaN.
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
#[netcdf(on_fill(int = "nan"))]
struct Bad {
    #[netcdf(dimensions("time"))]
    quality: Vec<i32>,
}

fn main() {}
