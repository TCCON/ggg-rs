// A container-level `on_fill = "nan"` would also apply to integer fields, which
// cannot hold NaN, so it is refused in favour of `on_fill(float = "nan")`.
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
#[netcdf(on_fill = "nan")]
struct Bad {
    #[netcdf(dimensions("time"))]
    xco2: Vec<f64>,
}

fn main() {}
