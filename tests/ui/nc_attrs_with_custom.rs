// A `custom` write owns the variable, so it sets any attributes itself.
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

fn more(
    _d: &Bad,
    _v: &Vec<f64>,
    _f: &str,
    _n: &str,
    _var: &mut netcdf::VariableMut<'_>,
) -> netcdf::Result<()> {
    Ok(())
}

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(custom(write = to_nc), attrs(func = more))]
    values: Vec<f64>,
}

fn main() {}
