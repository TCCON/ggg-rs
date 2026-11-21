// `ReadNetcdf` ignores `attrs`, but still reports a malformed one, so the
// mistake is not hidden until `WriteNetcdf` is added.
use ggg_rs_derive::ReadNetcdf;

#[derive(ReadNetcdf)]
#[netcdf(attrs = global_attrs)]
struct Bad {
    values: Vec<f64>,
}

fn main() {}
