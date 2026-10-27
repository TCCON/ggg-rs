// With a context, every `custom` function takes it as its final argument.
use ggg_rs_derive::ReadNetcdf;

struct Ctx;

fn from_nc(_f: &str, _n: &str, _g: &netcdf::Group<'_>) -> netcdf::Result<f64> {
    Ok(0.0)
}

#[derive(ReadNetcdf)]
#[netcdf(context(ty = "Ctx"))]
struct Bad {
    #[netcdf(custom(read = from_nc))]
    value: f64,
}

fn main() {}
