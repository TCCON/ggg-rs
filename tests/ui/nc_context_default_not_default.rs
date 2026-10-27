// `context(default)` needs the context type to implement `Default`; the error
// should point at the annotated type, not at the derive.
use ggg_rs_derive::ReadNetcdf;

struct NoDefault;

fn from_nc(_f: &str, _n: &str, _g: &netcdf::Group<'_>, _c: &NoDefault) -> netcdf::Result<f64> {
    Ok(0.0)
}

#[derive(ReadNetcdf)]
#[netcdf(context(ty = "NoDefault", default))]
struct Bad {
    #[netcdf(custom(read = from_nc))]
    value: f64,
}

fn main() {}
