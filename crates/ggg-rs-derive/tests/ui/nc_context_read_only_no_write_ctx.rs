// With a read-only context, `WriteNetcdf` generates only the plain method.
use ggg_rs_derive::WriteNetcdf;

struct Ctx;

#[derive(WriteNetcdf)]
#[netcdf(context(read = "Ctx"))]
struct Data {
    value: f64,
}

fn main() {
    let mut file = netcdf::create("x.nc").unwrap();
    let data = Data { value: 1.0 };
    let _ = data.write_netcdf_context(&mut file.root_mut().unwrap(), &Ctx);
}
