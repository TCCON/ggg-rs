// With a write context, a field `attrs` function must take it.
use ggg_rs_derive::WriteNetcdf;

struct Ctx;

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
#[netcdf(context(ty = "Ctx"))]
struct Bad {
    #[netcdf(attrs(func = more))]
    values: Vec<f64>,
}

fn main() {}
