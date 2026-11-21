// A field-level `unpack` on a `custom` read field would be silently ignored.
use ggg_rs_derive::ReadNetcdf;

fn from_nc(_f: &str, _n: &str, _g: &netcdf::Group<'_>) -> netcdf::Result<Vec<f64>> {
    Ok(Vec::new())
}

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(custom(read = from_nc), unpack)]
    values: Vec<f64>,
}

fn main() {}
