// A type alias is opaque, so an explicit fill check on one is refused rather
// than silently skipped.
use ggg_rs_derive::ReadNetcdf;

type Flags = Vec<i32>;

#[derive(ReadNetcdf)]
struct Bad {
    #[netcdf(dimensions("time"), kind = "values", on_fill = "error")]
    flags: Flags,
}

fn main() {}
