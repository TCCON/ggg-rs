// Integer fields cannot hold NaN, at the field level either.
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(dimensions("time"), on_fill = "nan")]
    quality: Vec<i32>,
}

fn main() {}
