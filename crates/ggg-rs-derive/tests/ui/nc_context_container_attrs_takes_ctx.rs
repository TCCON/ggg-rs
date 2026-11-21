// A container `attrs` function takes a context, but the struct has none.
use ggg_rs_derive::WriteNetcdf;

struct Ctx;

fn global(_d: &Bad, _g: &mut netcdf::GroupMut<'_>, _c: &Ctx) -> netcdf::Result<()> {
    Ok(())
}

#[derive(WriteNetcdf)]
#[netcdf(attrs(func = global))]
struct Bad {
    value: f64,
}

fn main() {}
