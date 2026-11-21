// On an `attribute` field there is no variable, so `map(read = ...)` receives
// the `netcdf::Attribute` instead. A variable-style function is rejected.
use ggg_rs_derive::ReadNetcdf;

fn parse(
    v: String,
    _f: &str,
    _n: &str,
    _var: &netcdf::Variable<'_>,
) -> netcdf::Result<i32> {
    v.parse().map_err(|_| netcdf::Error::Str("bad".into()))
}

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(attribute, map(read = parse))]
    version: i32,
}

fn main() {}
