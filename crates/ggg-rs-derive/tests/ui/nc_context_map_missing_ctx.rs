// With a write context, `map(write = ...)` functions take `&Ctx` as their
// final argument, just like `custom` functions.
use ggg_rs_derive::WriteNetcdf;

struct Scale(f64);

fn scale(_d: &Bad, v: &Vec<f64>, _f: &str, _n: &str) -> Vec<f64> {
    v.clone()
}

#[derive(WriteNetcdf)]
#[netcdf(context(write = "Scale"))]
struct Bad {
    #[netcdf(dimensions("time"), map(write = scale), kind = "values")]
    xco2: Vec<f64>,
}

fn main() {}
