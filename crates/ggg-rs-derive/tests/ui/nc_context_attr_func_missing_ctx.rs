// With a write context, `attr(func = ...)` functions take `&Ctx` as their
// final argument.
use ggg_rs_derive::WriteNetcdf;

struct Note(String);

fn note(_d: &Bad, _v: &Vec<f64>, _f: &str, _n: &str) -> String {
    String::new()
}

#[derive(WriteNetcdf)]
#[netcdf(context(write = "Note"))]
struct Bad {
    #[netcdf(dimensions("time"), attr(name = "note", func = note))]
    xco2: Vec<f64>,
}

fn main() {}
