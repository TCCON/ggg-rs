// `map(write = ...)` functions receive both the Rust field name and the
// netCDF name; the old three-argument form is rejected with the new signature.
use ggg_rs_derive::WriteNetcdf;

fn scale(_d: &Bad, v: &Vec<f64>, _n: &str) -> Vec<f64> {
    v.clone()
}

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(dimensions("time"), map(write = scale), kind = "values")]
    xco2: Vec<f64>,
}

fn main() {}
