// A field has one element type, so the per-class form is container-only.
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(dimensions("time"), on_fill(float = "nan", int = "error"))]
    xco2: Vec<f64>,
}

fn main() {}
