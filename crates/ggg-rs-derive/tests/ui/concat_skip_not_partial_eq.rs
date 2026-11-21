use ggg_rs_derive::Concat;

#[derive(Clone)]
struct NoEq;

#[derive(Concat)]
struct Bad {
    values: Vec<u8>,
    #[concat(skip)]
    meta: NoEq,
}

fn main() {}
