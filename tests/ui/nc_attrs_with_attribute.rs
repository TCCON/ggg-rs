// A group-attribute field has no variable for `attrs` to write onto.
use ggg_rs_derive::WriteNetcdf;

fn more(
    _d: &Bad,
    _v: &String,
    _f: &str,
    _n: &str,
    _var: &mut netcdf::VariableMut<'_>,
) -> netcdf::Result<()> {
    Ok(())
}

#[derive(WriteNetcdf)]
struct Bad {
    #[netcdf(attribute, attrs(func = more))]
    site: String,
}

fn main() {}
