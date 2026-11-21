// `skip` and `attrs` contradict each other.
use ggg_rs_derive::WriteNetcdf;

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
    #[netcdf(skip, attrs(func = more))]
    values: Vec<f64>,
}

fn main() {}
