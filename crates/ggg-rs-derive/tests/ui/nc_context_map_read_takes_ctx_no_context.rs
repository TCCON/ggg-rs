// Without a read context, `map(read = ...)` functions take no context
// argument, even though the struct has a write context.
use ggg_rs_derive::ReadNetcdf;

struct Scale(f64);

fn from_nc(
    v: Vec<f64>,
    _f: &str,
    _n: &str,
    _var: &netcdf::Variable<'_>,
    _ctx: &Scale,
) -> netcdf::Result<Vec<f64>> {
    Ok(v)
}

#[derive(ReadNetcdf)]
#[netcdf(context(write = "Scale"))]
struct Bad {
    #[netcdf(dimensions("time"), map(read = from_nc), kind = "values")]
    xco2: Vec<f64>,
}

fn main() {}
