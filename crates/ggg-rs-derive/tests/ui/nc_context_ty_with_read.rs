// `ty = ...` already covers both directions, so `read`/`write` contradict it.
use ggg_rs_derive::ReadNetcdf;

struct A;
struct B;

#[derive(ReadNetcdf)]
#[netcdf(context(ty = "A", read = "B"))]
struct Bad {
    value: f64,
}

fn main() {}
