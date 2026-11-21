// `custom` replaces the derived write entirely, so a `map` would be ignored.
use ggg_rs_derive::WriteNetcdf;

fn to_nc(
    _d: &Bad,
    _v: &Vec<f64>,
    _f: &str,
    _n: &str,
    _g: &mut netcdf::GroupMut<'_>,
) -> netcdf::Result<()> {
    Ok(())
}

fn scale(_d: &Bad, v: &Vec<f64>, _f: &str, _n: &str) -> Vec<f64> {
    v.clone()
}

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(custom(write = to_nc), map(write = scale), kind = "values")]
    values: Vec<f64>,
}

fn main() {}
