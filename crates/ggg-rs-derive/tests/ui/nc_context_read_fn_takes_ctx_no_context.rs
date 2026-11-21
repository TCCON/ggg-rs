// A `custom` read function takes a context, but the struct has no `context`.
use ggg_rs_derive::ReadNetcdf;

struct Ctx;

fn from_nc(_f: &str, _n: &str, _g: &netcdf::Group<'_>, _c: &Ctx) -> netcdf::Result<f64> {
    Ok(0.0)
}

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(custom(read = from_nc))]
    value: f64,
}

fn main() {}
