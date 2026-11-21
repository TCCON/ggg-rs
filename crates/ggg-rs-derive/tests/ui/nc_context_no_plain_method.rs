// Without `default`, a struct with a context has only the `_context` methods.
use ggg_rs_derive::ReadNetcdf;

struct Ctx;

#[derive(ReadNetcdf)]
#[netcdf(context(ty = "Ctx"))]
struct Data {
    value: f64,
}

fn main() {
    let file = netcdf::open("x.nc").unwrap();
    let _ = Data::read_netcdf(&file.root().unwrap());
}
