// A `custom` write function takes a context, but only reading declares one.
use ggg_rs_derive::WriteNetcdf;

struct Ctx;

fn to_nc(
    _d: &Bad,
    _v: &f64,
    _f: &str,
    _n: &str,
    _g: &mut netcdf::GroupMut<'_>,
    _c: &Ctx,
) -> netcdf::Result<()> {
    Ok(())
}

#[derive(WriteNetcdf)]
#[netcdf(context(read = "Ctx"))]
struct Bad {
    #[netcdf(custom(write = to_nc))]
    value: f64,
}

fn main() {}
